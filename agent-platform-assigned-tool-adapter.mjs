import { createHash, randomUUID } from 'node:crypto';
import { createToolRegistration, resolveToolAssignments } from './agent-platform-tool-assignment.mjs';
import { valueSchemaIssues } from './value-schema.mjs';

const MAX_STICKY_LIFECYCLE_FAILURES = 1_024;

/** Transport-free v1 assigned-call adapter. No process, provider, native tools or credentials. */
export function createAssignedToolAdapter({ write } = {}) {
  if (typeof write !== 'function') throw new TypeError('Assigned adapter requires an owned reply writer.');
  const adapter = new AssignedToolAdapter();
  adapter.write = write;
  return adapter;
}

// Internal base shared with the local transport; the distribution exports only the factory.
export class AssignedToolAdapter {
  constructor() {
    this.bridgeSessionId = randomUUID();
    this.dynamicTools = new Map();
    this.threadGrants = new Map();
    this.assignmentAttempts = new WeakMap();
    this.activeAssignmentAttempts = new Set();
    this.assignedThreadIds = new Set();
    this.issuedAssignedGrants = new WeakSet();
    this.assignedRetirements = new Map();
    this.assignedCalls = new Set();
    this.lifecycleSubscribers = new Set();
    this.lifecycleByThread = new Map();
    this.failedLifecycleThreads = new Set();
    this.lifecycleFailureGlobal = false;
    this.lastError = null;
  }

  async ensureStarted() { /* The host owns provider startup; this adapter owns calls only. */ }

  validateAssignedExecutionPolicy(workspaceAccess, networkAccess) {
    if (workspaceAccess !== 'read-only' || networkAccess !== false) {
      throw new TypeError('Assigned adapter requires read-only workspace and disabled network.');
    }
  }

  subscribeLifecycle(listener) {
    if (typeof listener !== 'function') throw new TypeError('Lifecycle subscriber must be a function.');
    this.lifecycleSubscribers.add(listener);
    return () => this.lifecycleSubscribers.delete(listener);
  }

  emitLifecycle(message) {
    const sessionId = this.bridgeSessionId;
    const tasks = [...this.lifecycleSubscribers].map(listener => {
      try { return Promise.resolve(listener(message)); }
      catch (error) { return Promise.reject(error); }
    });
    const settled = Promise.allSettled(tasks);
    for (const threadId of lifecycleThreadIds(message)) {
      const previous = this.lifecycleByThread.get(threadId) ?? Promise.resolve(false);
      const current = previous.then(async priorFailure => {
        const outcomes = await settled;
        if (sessionId !== this.bridgeSessionId) return true;
        if (outcomes.some(outcome => outcome.status === 'rejected')) {
          this.lastError = 'Assigned lifecycle failed.';
          quarantineLifecycleThread(this, threadId);
          return true;
        }
        return priorFailure || this.lifecycleFailureGlobal || this.failedLifecycleThreads.has(threadId);
      });
      this.lifecycleByThread.set(threadId, current);
      void current.then(() => {
        if (this.lifecycleByThread.get(threadId) === current) this.lifecycleByThread.delete(threadId);
      });
    }
  }

  registerDynamicTool(spec, handler) {
    return this.#registerToolBinding(spec, handler);
  }

  // Opt-in trusted host adapter. Both library and external handlers use this
  // seam; drain must await owned resources even when the handler rejects early.
  registerAssignedTool({ registration, handler, drain, maxInputBytes, maxResultBytes }) {
    const descriptor = createToolRegistration(registration);
    if (typeof drain !== "function") throw new TypeError("Assigned tools require an owned drain callback.");
    positiveInteger(maxInputBytes, "Assigned tool input bound");
    positiveInteger(maxResultBytes, "Assigned tool result bound");
    return this.#registerToolBinding({ name: descriptor.toolId, inputSchema: descriptor.inputSchema }, handler,
      { assignmentRegistration: descriptor, drain, maxInputBytes, maxResultBytes });
  }

  #registerToolBinding(spec, handler, metadata = {}) {
    if (!spec?.name || typeof handler !== "function") throw new TypeError("Dynamic tools require a name and handler.");
    if (this.dynamicTools.has(spec.name)) throw new TypeError(`Dynamic tool already registered: ${spec.name}.`);
    const registration = Object.freeze({
      spec: deepFreeze(JSON.parse(JSON.stringify({ type: "function", deferLoading: false, ...spec }))),
      handler,
      ...metadata,
    });
    this.dynamicTools.set(spec.name, registration);
    return () => {
      if (this.dynamicTools.get(spec.name) === registration) {
        this.dynamicTools.delete(spec.name);
        if (registration.assignmentRegistration) {
          for (const state of this.activeAssignmentAttempts) {
            if ([...state.registrations.values()].some(tools => [...tools.values()].includes(registration))) {
              void this.cancelToolAssignmentAttempt(state.token).catch(() => {});
            }
          }
        }
      }
    };
  }

  createToolAssignmentAttempt({ declarations, attemptId, parentParts, deadline, authorize, signal,
    workspaceAccess, networkAccess }) {
    if ([...this.activeAssignmentAttempts].some(state => state.closed)) {
      throw new Error("Assigned work has not drained successfully.");
    }
    const assignment = resolveToolAssignments(declarations);
    applicationText(attemptId, "Assigned attempt id");
    if (typeof authorize !== "function") throw new TypeError("Assigned attempts require fresh host authorization.");
    this.validateAssignedExecutionPolicy(workspaceAccess, networkAccess);
    if (!Number.isSafeInteger(deadline?.deadlineAtMs) || deadline.deadlineAtMs <= Date.now()) {
      throw new TypeError("Assigned attempts require a future resolved execution deadline.");
    }
    signal?.throwIfAborted();
    const parents = assignedPartParents(parentParts, assignment.parts);
    const registrations = new Map(assignment.parts.map(part => [part.partId, new Map(part.tools.map(tool => {
      const registration = this.dynamicTools.get(tool.toolId);
      if (registration?.assignmentRegistration?.bindingId !== tool.bindingId) {
        throw new Error("Assigned implementation binding is unavailable.");
      }
      return [tool.toolId, registration];
    }))]));
    const token = deepFreeze({ schemaVersion: 1, assignment, attemptId, generation: randomUUID(),
      sessionId: this.bridgeSessionId, deadlineAtMs: deadline.deadlineAtMs,
      workspaceAccess, networkAccess });
    const state = { token, parents, registrations, authorize, controller: new AbortController(),
      closed: false, drainFailed: false, timer: null };
    this.assignmentAttempts.set(token, state);
    this.activeAssignmentAttempts.add(state);
    const cancel = () => { void this.cancelToolAssignmentAttempt(token).catch(() => {}); };
    state.removeAbort = () => signal?.removeEventListener("abort", cancel);
    signal?.addEventListener("abort", cancel, { once: true });
    const schedule = () => {
      const remaining = token.deadlineAtMs - Date.now();
      if (remaining <= 0) cancel();
      else state.timer = setTimeout(schedule, Math.min(remaining, 2_147_483_647));
    };
    schedule();
    if (signal?.aborted) cancel();
    return token;
  }

  assignedAttempt(token) {
    const state = this.assignmentAttempts.get(token);
    if (!state || state.closed || this.lifecycleFailureGlobal || token.sessionId !== this.bridgeSessionId
      || token.deadlineAtMs <= Date.now() || state.controller.signal.aborted) {
      throw new Error("Assigned attempt is unavailable.");
    }
    return state;
  }

  async grantAssignedThread({ attempt, partId, threadId, parentThreadId = null }) {
    const state = this.assignedAttempt(attempt);
    applicationText(threadId, "Assigned thread id");
    if (!state.parents.has(partId) || this.failedLifecycleThreads.has(threadId)
      || this.threadGrants.has(threadId) || this.assignedThreadIds.has(threadId)) {
      throw new Error("Assigned thread requires an unused identity and declared part.");
    }
    const parentPart = state.parents.get(partId);
    const parent = parentThreadId === null ? null : this.currentAssignedGrant(parentThreadId);
    if ((parentPart === null && parentThreadId !== null)
      || (parentPart !== null && (!parent || parent.attempt !== attempt || parent.partId !== parentPart))) {
      throw new Error("Assigned child requires its exact host-declared parent.");
    }
    const grant = { schemaVersion: 1, attempt, partId, threadId, parentThreadId,
      parentGrant: parent, sessionId: this.bridgeSessionId, generation: randomUUID(),
      capabilityId: `assigned-tools:${attempt.assignment.assignmentHash}:${partId}`,
      registrations: state.registrations.get(partId), controller: new AbortController(),
      executionCeiling: { workspaceAccess: attempt.workspaceAccess, networkAccess: attempt.networkAccess } };
    const call = { grant, done: null };
    call.done = Promise.resolve().then(async () => {
      let authorized;
      try { authorized = await state.authorize(this.assignedContext(grant, "grant")); }
      catch { throw new Error("Assigned thread authorization failed."); }
      if (authorized !== true) {
        throw new Error("Assigned thread authorization denied.");
      }
      this.assignedAttempt(attempt);
      grant.controller.signal.throwIfAborted();
      if (parent && this.currentAssignedGrant(parentThreadId) !== parent) throw new Error("Assigned parent changed.");
      if (this.failedLifecycleThreads.has(threadId) || this.threadGrants.has(threadId) || this.assignedThreadIds.has(threadId)) {
        throw new Error("Assigned thread identity changed.");
      }
      for (const [name, registration] of grant.registrations) {
        if (this.dynamicTools.get(name) !== registration) throw new Error("Assigned implementation binding changed.");
      }
      Object.freeze(grant);
      this.issuedAssignedGrants.add(grant);
      this.threadGrants.set(threadId, grant);
      this.assignedThreadIds.add(threadId);
      return deepFreeze({ schemaVersion: 1, threadId, partId, generation: grant.generation,
        attemptGeneration: attempt.generation, providerEffectiveCatalog: null,
        providerEffectiveWorkspaceAccess: null, providerEffectiveNetworkAccess: null });
    });
    this.assignedCalls.add(call);
    try { return await call.done; }
    finally { this.assignedCalls.delete(call); }
  }

  currentAssignedGrant(threadId) {
    const grant = this.threadGrants.get(threadId);
    if (!grant?.attempt || !this.issuedAssignedGrants.has(grant) || grant.threadId !== threadId || grant.sessionId !== this.bridgeSessionId
      || grant.controller.signal.aborted || this.lifecycleFailureGlobal || this.failedLifecycleThreads.has(threadId)) {
      throw new Error("Assigned thread grant is unavailable.");
    }
    this.assignedAttempt(grant.attempt);
    if (grant.parentGrant && this.currentAssignedGrant(grant.parentThreadId) !== grant.parentGrant) {
      throw new Error("Assigned ancestor changed.");
    }
    for (const [name, registration] of grant.registrations) {
      if (this.dynamicTools.get(name) !== registration) throw new Error("Assigned implementation binding changed.");
    }
    return grant;
  }

  assignedContext(grant, phase, tool = null, callId = null) {
    const { attempt } = grant;
    return Object.freeze({ schemaVersion: 1, phase, threadId: grant.threadId, partId: grant.partId,
      sessionId: grant.sessionId, grantGeneration: grant.generation,
      attemptId: attempt.attemptId, attemptGeneration: attempt.generation,
      definitionId: attempt.assignment.definitionId, definitionHash: attempt.assignment.definitionHash,
      assignmentHash: attempt.assignment.assignmentHash, deadlineAtMs: attempt.deadlineAtMs,
      toolId: tool?.assignmentRegistration.toolId ?? null,
      bindingId: tool?.assignmentRegistration.bindingId ?? null, callId, signal: grant.controller.signal });
  }

  async #assignedLifecycle(threadId) {
    while (true) {
      const pending = this.lifecycleByThread.get(threadId);
      if (await pending) throw new Error("Assigned lifecycle failed.");
      if (!this.lifecycleByThread.has(threadId) || this.lifecycleByThread.get(threadId) === pending) return;
    }
  }

  cancelToolAssignmentAttempt(token) {
    const state = this.assignmentAttempts.get(token);
    if (!state) return Promise.reject(new Error("Unknown assigned attempt."));
    if (state.closing) return state.closing;
    state.closed = true;
    clearTimeout(state.timer);
    state.removeAbort();
    state.closing = Promise.resolve().then(async () => {
      await Promise.allSettled([...this.assignedCalls].filter(call => call.grant?.attempt === token).map(call => call.done));
      if (state.drainFailed) throw new Error("Assigned tool drain failed.");
      this.activeAssignmentAttempts.delete(state);
    });
    for (const [id, grant] of this.threadGrants) {
      if (grant.attempt === token) {
        void this.revokeThreadGrant(id, { sessionId: grant.sessionId, generation: grant.generation }).catch(() => {});
      }
    }
    for (const call of this.assignedCalls) {
      if (call.grant?.attempt === token) call.grant.controller.abort(new Error("Assigned attempt stopped."));
    }
    state.controller.abort(new Error("Assigned attempt stopped."));
    return state.closing;
  }

  async drainToolAssignments() {
    await Promise.all([...this.activeAssignmentAttempts].filter(state => state.closed).map(state => state.closing));
    await Promise.all([...this.assignedCalls].map(call => call.done));
  }

  /** Own host work under an exact grant, including cancellation and resource drain. */
  runAssignedOperation({ attempt, partId, threadId, operation, drain }) {
    if (typeof operation !== 'function' || typeof drain !== 'function') {
      throw new TypeError('Assigned operations require owned work and drain callbacks.');
    }
    const state = this.assignedAttempt(attempt);
    const grant = this.currentAssignedGrant(threadId);
    if (grant.attempt !== attempt || grant.partId !== partId) throw new Error('Assigned operation lineage mismatch.');
    const current = () => {
      if (this.currentAssignedGrant(threadId) !== grant) throw new Error('Assigned operation grant changed.');
    };
    const call = { grant, done: null };
    call.done = Promise.resolve().then(async () => {
      await this.#assignedLifecycle(threadId);
      current();
      if (await state.authorize(this.assignedContext(grant, 'operation')) !== true) {
        throw new Error('Assigned operation authorization denied.');
      }
      current();
      let result;
      try { result = await operation(this.assignedContext(grant, 'operation')); }
      finally {
        try { await drain(); }
        catch {
          state.drainFailed = true;
          void this.cancelToolAssignmentAttempt(attempt).catch(() => {});
          throw new Error('Assigned operation drain failed.');
        }
      }
      await this.#assignedLifecycle(threadId);
      current();
      return result;
    }).finally(() => this.assignedCalls.delete(call));
    this.assignedCalls.add(call);
    return call.done;
  }

  /** Host-dispatched parts use the same owned, fresh answer authorization boundary. */
  async authorizeAssignedAnswer({ attempt, partId, threadId }) {
    const state = this.assignedAttempt(attempt);
    await this.#assignedLifecycle(threadId);
    const grant = this.currentAssignedGrant(threadId);
    if (grant.attempt !== attempt || grant.partId !== partId) throw new Error("Assigned answer lineage mismatch.");
    const authorization = { grant, done: null };
    authorization.done = Promise.resolve().then(() => state.authorize(this.assignedContext(grant, "answer")));
    this.assignedCalls.add(authorization);
    let authorized;
    try { authorized = await authorization.done; }
    catch { throw new Error("Assigned answer authorization failed."); }
    finally { this.assignedCalls.delete(authorization); }
    if (authorized !== true) throw new Error("Assigned answer authorization denied.");
    await this.#assignedLifecycle(threadId);
    if (this.currentAssignedGrant(threadId) !== grant) throw new Error("Assigned answer grant changed.");
  }

  revokeThreadGrant(threadId, identity) {
    const grant = this.threadGrants.get(threadId);
    let history = this.assignedRetirements.get(threadId);
    if (identity !== undefined) {
      if (!identity || Object.keys(identity).sort().join(",") !== "generation,sessionId"
        || typeof identity.generation !== "string" || typeof identity.sessionId !== "string") {
        throw new TypeError("Assigned retirement requires exact session and generation identity.");
      }
      const previous = history?.get(JSON.stringify([identity.sessionId, identity.generation]));
      if (previous) return previous;
      if (!grant?.attempt || grant.sessionId !== identity.sessionId || grant.generation !== identity.generation) {
        throw new Error("Assigned retirement identity is unavailable.");
      }
    } else if (!grant && history) {
      return [...history.values()].at(-1);
    }
    const ownedCalls = [];
    const ownedRetirements = [];
    let retirement;
    if (grant?.attempt) {
      const key = JSON.stringify([grant.sessionId, grant.generation]);
      if (history?.has(key)) return history.get(key);
      retirement = Promise.resolve().then(async () => {
        // Call cancellation/authorization denial is not a failed resource
        // retirement. Await both kinds; aggregate failures only from retirements.
        const [outcomes] = await Promise.all([
          Promise.allSettled(ownedRetirements), Promise.allSettled(ownedCalls),
        ]);
        if (outcomes.some(outcome => outcome.status === "rejected")
          || this.assignmentAttempts.get(grant.attempt)?.drainFailed) throw new Error("Assigned tool drain failed.");
      });
      // Publish ownership before recursion/removal/abort, which can reenter
      // revocation. Retain the actual promise (including rejection), not a flag.
      if (!history) this.assignedRetirements.set(threadId, history = new Map());
      history.set(key, retirement);
      void retirement.catch(() => {});
    }
    for (const [id, child] of this.threadGrants) {
      if (child.parentThreadId === threadId && (!grant?.attempt || !child.attempt || child.parentGrant === grant)) {
        const drain = this.revokeThreadGrant(id, child.attempt
          ? { sessionId: child.sessionId, generation: child.generation } : undefined);
        if (drain) ownedRetirements.push(drain);
      }
    }
    if (this.threadGrants.get(threadId) === grant) this.threadGrants.delete(threadId);
    if (grant?.attempt) {
      grant.controller.abort(new Error("Assigned grant revoked."));
      for (const call of this.assignedCalls) {
        for (let ancestor = call.grant; ancestor; ancestor = ancestor.parentGrant) {
          if (ancestor === grant) {
            call.grant.controller.abort(new Error("Assigned ancestor revoked."));
            ownedCalls.push(call.done);
            break;
          }
        }
      }
      const state = this.assignmentAttempts.get(grant.attempt);
      if (grant.parentThreadId === null && !state.closed) ownedRetirements.push(this.cancelToolAssignmentAttempt(grant.attempt));
      return retirement;
    }
  }

  executeAssignedToolRequest(message) {
    const grant = this.threadGrants.get(message.params?.threadId);
    const priorLifecycle = this.lifecycleByThread.get(message.params?.threadId);
    const call = { grant, done: null };
    call.done = Promise.resolve().then(async () => {
      if (!grant?.attempt) return;
      const state = this.assignmentAttempts.get(grant.attempt);
      const registration = grant.registrations.get(message.params?.tool);
      const context = this.assignedContext(grant, "invoke", registration, String(message.id));
      const startedAt = Date.now();
      let failed = false;
      let invoked = false;
      let result;
      let audit;
      const current = () => {
        if (this.currentAssignedGrant(grant.threadId) !== grant) throw new Error("Assigned grant changed.");
      };
      try {
        if (await priorLifecycle) throw new Error("Assigned lifecycle failed.");
        current();
        if (!registration?.assignmentRegistration) throw new Error("Assigned tool is unavailable.");
        const encoded = JSON.stringify(message.params?.arguments);
        if (typeof encoded !== "string" || Buffer.byteLength(encoded) > registration.maxInputBytes) throw new Error("Assigned input rejected.");
        const argumentsValue = JSON.parse(encoded);
        if (valueSchemaIssues(argumentsValue, registration.assignmentRegistration.inputSchema).length) throw new Error("Assigned input rejected.");
        audit = { threadId: grant.threadId, callId: String(message.id), tool: registration.spec.name,
          grantGeneration: grant.generation, attemptGeneration: grant.attempt.generation,
          argumentsSha256: createHash("sha256").update(encoded).digest("hex"), argumentsByteLength: Buffer.byteLength(encoded) };
        if (await state.authorize(context) !== true) throw new Error("Assigned invocation denied.");
        current();
        this.emitLifecycle({ method: "tool/call/started", params: { ...audit, status: "running" } });
        await this.#assignedLifecycle(grant.threadId);
        current();
        invoked = true;
        result = await registration.handler({ arguments: argumentsValue, context });
      } catch {
        failed = true;
      } finally {
        if (invoked) {
          try { await registration.drain(context); }
          catch {
            state.drainFailed = true;
            this.lastError = "Assigned tool drain failed.";
            void this.cancelToolAssignmentAttempt(grant.attempt).catch(() => {});
          }
        }
      }
      try {
        await this.#assignedLifecycle(grant.threadId);
        current();
        let text = "Assigned tool call failed.";
        if (!failed) {
          try {
            const encoded = JSON.stringify(result);
            if (typeof encoded !== "string" || Buffer.byteLength(encoded) > registration.maxResultBytes
              || valueSchemaIssues(JSON.parse(encoded), registration.assignmentRegistration.resultSchema).length) {
              throw new Error("Assigned result rejected.");
            }
            text = typeof result === "string" ? result : encoded;
          } catch { failed = true; }
        }
        current();
        if (await state.authorize(this.assignedContext(grant, "disclose", registration, String(message.id))) !== true) {
          failed = true;
        }
        await this.#assignedLifecycle(grant.threadId);
        current();
        this.write({ id: message.id, result: { success: !failed,
          contentItems: [{ type: "inputText", text: failed ? "Assigned tool call failed." : text }] } });
        this.emitLifecycle({ method: failed ? "tool/call/failed" : "tool/call/completed", params: {
          ...(audit ?? { threadId: grant.threadId, callId: String(message.id) }),
          status: failed ? "failed" : "completed", durationMs: Date.now() - startedAt,
          ...(failed ? { code: "ASSIGNED_TOOL_CALL_FAILED" } : { resultByteLength: Buffer.byteLength(text) }),
        } });
      } catch {
        // Revoked/expired/replaced work discloses neither success nor raw error.
      }
    }).finally(() => this.assignedCalls.delete(call));
    this.assignedCalls.add(call);
    return call.done;
  }

}

function quarantineLifecycleThread(bridge, threadId) {
  if (bridge.lifecycleFailureGlobal) return;
  bridge.failedLifecycleThreads.add(threadId);
  if (bridge.threadGrants.get(threadId)?.attempt) void bridge.revokeThreadGrant(threadId)?.catch(() => {});
  if (bridge.failedLifecycleThreads.size > MAX_STICKY_LIFECYCLE_FAILURES) {
    bridge.lifecycleFailureGlobal = true;
    bridge.failedLifecycleThreads.clear();
    for (const state of bridge.activeAssignmentAttempts) void bridge.cancelToolAssignmentAttempt(state.token).catch(() => {});
  }
}
function assignedPartParents(input, parts) {
  if (!Array.isArray(input) || input.length !== parts.length) throw new TypeError("Assigned parts require explicit host parent declarations.");
  const ids = new Set(parts.map(part => part.partId));
  const parents = new Map();
  for (const entry of input) {
    if (!entry || Object.keys(entry).sort().join(",") !== "parentPartId,partId" || !ids.has(entry.partId)
      || parents.has(entry.partId) || (entry.parentPartId !== null && !ids.has(entry.parentPartId))) {
      throw new TypeError("Invalid assigned part parent declaration.");
    }
    parents.set(entry.partId, entry.parentPartId);
  }
  if ([...parents.values()].filter(parent => parent === null).length !== 1) throw new TypeError("Assigned parts require one declared root.");
  for (const id of ids) {
    const visited = new Set();
    for (let part = id; part !== null; part = parents.get(part)) {
      if (visited.has(part)) throw new TypeError("Assigned part ancestry is cyclic.");
      visited.add(part);
    }
  }
  return parents;
}
function lifecycleThreadIds(message) {
  const ids = new Set();
  const parent = message.params?.threadId;
  if (typeof parent === "string" && parent) ids.add(parent);
  const agent = message.params?.item?.agentThreadId;
  if (typeof agent === "string" && agent) ids.add(agent);
  const receivers = message.params?.item?.receiverThreadIds;
  if (Array.isArray(receivers)) {
    for (const id of receivers) {
      if (typeof id === "string" && id) ids.add(id);
    }
  }
  return ids;
}
function deepFreeze(value) {
  if (value && typeof value === "object" && !Object.isFrozen(value)) {
    for (const child of Object.values(value)) deepFreeze(child);
    Object.freeze(value);
  }
  return value;
}
function applicationText(value, label) {
  if (typeof value !== 'string' || !value.trim() || value !== value.trim() || value.length > 200) {
    throw new TypeError(`${label} must be a non-empty, unpadded string of at most 200 characters.`);
  }
  return value;
}
function positiveInteger(value, label) {
  if (!Number.isSafeInteger(value) || value < 1) throw new TypeError(`${label} must be a positive safe integer.`);
  return value;
}
