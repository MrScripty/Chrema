import { createHash, randomUUID } from "node:crypto";
import { normalizeModelSettings, normalizeUsage, versionIdentity, normalizeVersion, normalizePriceVersion, normalizeAttemptPrice, normalizeMoney } from "./agent-platform-journal-contract.mjs";
import { assertSafeAuditPrompt } from "../shared/agent-audit-safety.mjs";
import { resolvePostgresRelations } from "../shared/postgres-relations.mjs";
import { claimLostError, normalizeExpectedClaim, assertMemoryExpectedClaim, assertPostgresExpectedClaim } from "./agent-platform-claim-bound-journal.mjs";

const postgresRelationKeys = [
  "versions", "prices", "runs", "assignments", "assignmentTargets", "attempts",
  "events", "stagedCommands", "validations", "admissions",
];
const postgresRunFamilyRelationKeys = ["families", "members"];
const cleanJournalRelationNames = Object.freeze({
  versions: "agent_workflow_versions",
  prices: "agent_model_price_versions",
  runs: "agent_workflow_runs",
  assignments: "agent_workflow_assignments",
  assignmentTargets: "agent_workflow_assignment_targets",
  attempts: "agent_workflow_attempts",
  events: "agent_workflow_events",
  stagedCommands: "agent_workflow_staged_commands",
  validations: "agent_workflow_validations",
  admissions: "agent_workflow_admissions",
});

const runStatuses = new Set([
  "queued", "running", "validating", "ready-for-admission", "admitted",
  "completed", "failed", "cancelled", "blocked",
]);
const openAssignmentStatuses = new Set(["dispatching", "running", "unknown"]);
const terminalAssignmentStatuses = new Set(["completed", "failed", "interrupted"]);
const terminalAttemptOutcomes = new Set(["completed", "failed", "interrupted"]);

// These two immutable parent events are a versioned domain record. Ordinary
// event APIs cannot write their reserved identities. No live claim is retained
// by a blocked run; only createRevisionRunAndAppendEvent may consume the record.
const revisionGrantIdentity = "workflow.revision-handoff.v1";
const revisionConsumptionIdentity = "workflow.revision-handoff-consumed.v1";
const revisionHandoffWrite = Symbol("checked revision handoff write");
const cancellationRequestIdentity = 'workflow.cancellation-requested.v1';
const cancellationConfirmationIdentity = 'workflow.cancellation-confirmed.v1';
const cancellationWrite = Symbol('authorized run cancellation write');

export const agentWorkflowRunStatuses = Object.freeze([...runStatuses]);

/**
 * Capture the host's completion policy. The journal awaits assertCompletion
 * before completing a run; the generic database schema checks
 * only row shape and cannot enforce this policy for direct SQL writers.
 */
export function createAgentWorkflowJournalFactory(dependencies) {
  if (!dependencies || typeof dependencies !== "object" || Array.isArray(dependencies)) {
    throw new TypeError("Agent workflow journal dependencies are required.");
  }
  const { assertCompletion, projectMetrics, observeJournalEvent, observeAttemptOutput,
    assignmentMarker, postgresRelations, postgresRunFamilyRelations, authorizeRunSummary } = dependencies;
  for (const [name, dependency] of Object.entries({
    assertCompletion, projectMetrics, observeJournalEvent, observeAttemptOutput, assignmentMarker,
  })) {
    if (typeof dependency !== "function") throw new TypeError(`Agent workflow journal ${name} dependency must be callable.`);
  }
  if (authorizeRunSummary !== undefined && typeof authorizeRunSummary !== "function") {
    throw new TypeError("Agent workflow journal authorizeRunSummary dependency must be callable.");
  }
  const resolvedPostgresRelations = postgresRelations === undefined ? null
    : resolvePostgresRelations(postgresRelations, postgresRelationKeys, "Agent workflow journal");
  const resolvedRunFamilyRelations = postgresRunFamilyRelations === undefined ? null
    : resolvePostgresRelations(postgresRunFamilyRelations, postgresRunFamilyRelationKeys, "Agent workflow run family");

  return function createAgentWorkflowJournal({
    database,
    createId = randomUUID,
    now = () => new Date().toISOString(),
    lockResumeSource,
  } = {}) {
    if (lockResumeSource !== undefined && typeof lockResumeSource !== "function") {
      throw new TypeError("Agent workflow resume source lock must be callable.");
    }
    return database
      ? createPostgresJournal({ database, createId, now, relations: resolvedPostgresRelations,
        familyRelations: resolvedRunFamilyRelations, lockResumeSource })
      : createMemoryJournal({ createId, now });
  };

  // Opt-in metadata seam. Owner equality narrows selection; only the host can
  // authorize the requesting actor and any additional application eligibility.
  async function authorizedRunSummary(input, { signal } = {}, read) {
    if (typeof authorizeRunSummary !== "function") throw new TypeError("Run summary reads require explicit host authorization.");
    assertRunSummaryFields(input, ["runId", "owner"]);
    const ownerKind = Object.getOwnPropertyDescriptor(input.owner ?? {}, "kind")?.value;
    const ownerFields = ownerKind === "authenticated" ? ["kind", "principalId", "accountId"]
      : ownerKind === "public" ? ["kind", "assistantSessionId"] : ["kind", "principalId"];
    assertRunSummaryFields(input.owner, ownerFields);
    const owner = normalizeOwner(input.owner);
    for (const value of Object.values(owner)) {
      if (value !== null && (typeof value !== "string" || Buffer.byteLength(value) > 512)) {
        throw new TypeError("Run summary owner exceeds its identity bound.");
      }
    }
    const runId = boundedText(input.runId, "Run summary id", 512);
    if (Buffer.byteLength(runId) > 512) throw new TypeError("Run summary id exceeds its identity bound.");
    const request = Object.freeze({ runId, owner: Object.freeze(owner) });
    async function authorize(phase) {
      signal?.throwIfAborted();
      if (await authorizeRunSummary(Object.freeze({ ...request, phase, signal })) !== true) {
        const error = new Error("Run summary access is unavailable.");
        error.code = "AGENT_WORKFLOW_RUN_SUMMARY_DENIED";
        throw error;
      }
      signal?.throwIfAborted();
    }
    await authorize("read");
    const summary = await read(request, signal);
    await authorize("disclose");
    return summary === null ? null : projectRunSummary(summary);
  }

function createMemoryJournal({ createId, now }) {
  const versions = new Map();
  const prices = new Map();
  const runs = new Map();
  const runFamilies = new Map();
  const runFamilyMembers = new Map();
  let queue = Promise.resolve();

  function transact(operation) {
    const task = queue.then(operation, operation);
    queue = task.catch(() => undefined);
    return task;
  }

  function createCombinedRunEvent(input, revisionRequest = null) {
    const { run: runInput, event: eventInput, parentRunId } = requireCombinedRunEvent(input);
    const version = versions.get(requireText(runInput.workflowVersionId, "Workflow version id"));
    if (!version) throw new TypeError(`Unknown workflow version: ${runInput.workflowVersionId}.`);
    const runId = resolveRunId(runInput, createId);
    const owner = normalizeOwner(runInput.owner);
    const normalizedInput = cloneJson(runInput.input ?? {});
    const authorization = cloneJson(runInput.authorization ?? {});
    const queuePriority = workflowQueuePriority(runInput.queuePriority);
    const runFamilyBudget = normalizeRunFamilyBudget(runInput.runFamilyBudget);
    if (runFamilyBudget) throw new TypeError("Only a root workflow run can declare a run family budget.");
    const parent = requireRun(runs, parentRunId);
    assertResumeSource(parent, { owner, workflowType: version.workflowType });
    let grant;
    let consumed = false;
    if (revisionRequest) {
      grant = assertRevisionGrant(parent, parent.events.find(event => event.sourceKey === revisionGrantIdentity),
        revisionRequest.expected, revisionRequest.outcome);
      consumed = assertRevisionConsumption(parent.events.find(event => event.sourceKey === revisionConsumptionIdentity),
        grant, revisionRequest.consumption);
    } else {
      assertMemoryExpectedClaim(parent, eventInput.expectedClaim, now);
    }
    const identityHash = hashValue({ version: version.id, owner, input: normalizedInput,
      authorization: identityAuthorization(runInput, authorization), queuePriority, resumedFromRunId: parentRunId });
    const existingRun = runs.get(runId);
    if (existingRun && existingRun.identityHash !== identityHash) {
      throw conflict(`Workflow run ${runId} already exists with different input.`);
    }
    const sourceKey = boundedText(eventInput.sourceKey, "Workflow event source key", 500);
    const payload = cloneJson(eventInput.payload ?? {});
    const payloadHash = hashValue(payload);
    const existingEvent = parent.events.find(event => event.sourceKey === sourceKey);
    if (existingEvent && (existingEvent.payloadHash !== payloadHash
      || eventIdentityHash(existingEvent) !== eventInputIdentityHash(eventInput, payloadHash))) {
      throw conflict(`Workflow event ${sourceKey} already has a different payload.`);
    }
    if (consumed && (!existingRun || !existingEvent)) throw conflict("Consumed revision is missing its child or event.");
    const timestamp = now();
    const consumptionEvent = revisionRequest && !consumed
      ? domainEvent(parentRunId, revisionConsumptionIdentity, revisionRequest.consumption, timestamp) : null;
    const event = existingEvent ?? {
      eventId: `event-${hashValue({ runId: parentRunId, sourceKey }).slice(0, 24)}`,
      runId: parentRunId, assignmentId: null, attemptId: null,
      sourceKey, eventType: requireText(eventInput.eventType, "Workflow event type"),
      threadId: optionalText(eventInput.threadId), turnId: optionalText(eventInput.turnId),
      occurredAt: eventInput.occurredAt == null ? null : isoTimestamp(eventInput.occurredAt, "Workflow event occurrence time"),
      receivedAt: timestamp, payload, payloadHash,
    };
    const familyRootId = runFamilies.has(parentRunId) ? parentRunId : runFamilyMembers.get(parentRunId);
    const family = familyRootId ? runFamilies.get(familyRootId) : null;
    if (!existingRun && family && family.memberCount >= family.maxDescendantRuns) {
      throw conflict(`Workflow run family ${familyRootId} has exhausted its descendant budget.`);
    }
    const child = existingRun ?? {
      runId, workflowVersionId: version.id, workflowType: version.workflowType,
      owner, input: normalizedInput, inputHash: hashValue(normalizedInput), authorization,
      identityHash, status: "queued", queuePriority, recoveryCount: 0,
      resumedFromRunId: parentRunId, claim: null, createdAt: timestamp, updatedAt: timestamp,
      startedAt: null, completedAt: null, failure: null,
      assignments: [], events: [], stagedCommands: [], validations: [], admission: null,
    };
    if (!existingRun) {
      runs.set(runId, child);
      if (family) {
        runFamilyMembers.set(runId, familyRootId);
        family.memberCount += 1;
      }
    }
    if (!existingEvent) {
      parent.events.push(event);
      parent.updatedAt = timestamp;
    }
    if (consumptionEvent) parent.events.push(consumptionEvent);
    return { run: publicRunSummary(child), event: clone(event) };
  }

  return {
    assertExpectedClaim(input) {
      return transact(() => {
        const run = requireRun(runs, input?.runId);
        assertMemoryExpectedClaim(run, input?.expectedClaim, now);
      });
    },

    registerVersion(input) {
      return transact(async () => {
        const version = normalizeVersion(input, now());
        const existing = versions.get(version.id);
        if (existing) {
          if (existing.definitionHash !== version.definitionHash) {
            throw conflict(`Workflow version ${version.id} already has a different definition.`);
          }
          return clone(existing);
        }
        versions.set(version.id, version);
        return clone(version);
      });
    },

    inspectVersion(input) {
      return transact(() => {
        const version = versions.get(requireText(input?.workflowVersionId, 'Workflow version id'));
        if (!version) throw new TypeError('Unknown pinned workflow version.');
        return clone(version);
      });
    },

    registerPriceVersion(input) {
      return transact(async () => {
        const price = normalizePriceVersion(input, now());
        const expectedClaim = normalizeExpectedClaim(input?.expectedClaim);
        if (expectedClaim) {
          const run = runs.get(expectedClaim.runId);
          if (!run) throw claimLostError();
          assertMemoryExpectedClaim(run, expectedClaim, now);
        }
        const existing = prices.get(price.id);
        if (existing) {
          if (existing.priceHash !== price.priceHash) {
            throw conflict(`Model price version ${price.id} already has different rates.`);
          }
          return clone(existing);
        }
        prices.set(price.id, price);
        return clone(price);
      });
    },

    createRunAndAppendEvent(input) {
      return transact(() => createCombinedRunEvent(input));
    },

    createRevisionRunAndAppendEvent(input) {
      return transact(() => {
        const request = normalizeRevisionRequest(input);
        return createCombinedRunEvent(request, request);
      });
    },

    blockRunForRevision(input) {
      return transact(() => {
        const grant = revisionGrant(input);
        const run = requireRun(runs, input.runId);
        const prior = run.events.find(event => event.sourceKey === revisionGrantIdentity);
        if (prior) {
          const stored = assertRevisionGrant(run, prior, grant.claim, grant.outcome);
          if (stableJson(stored) !== stableJson(grant)) throw conflict("Revision handoff already records a different blocked decision.");
          return clone(run);
        }
        assertMemoryExpectedClaim(run, grant.claim, now);
        assertRunTransition(run, "blocked", input);
        const timestamp = now();
        const event = domainEvent(run.runId, revisionGrantIdentity, grant, timestamp);
        run.status = "blocked";
        run.failure = clone(grant.failure);
        run.finalSummary = clone(grant.finalSummary);
        run.completedAt = timestamp;
        run.updatedAt = timestamp;
        run.claim = null;
        run.events.push(event);
        return clone(run);
      });
    },

    createRun(input) {
      return transact(async () => {
        const version = versions.get(requireText(input?.workflowVersionId, "Workflow version id"));
        if (!version) throw new TypeError(`Unknown workflow version: ${input.workflowVersionId}.`);
        const runId = resolveRunId(input, createId);
        const timestamp = now();
        const owner = normalizeOwner(input.owner);
        const normalizedInput = cloneJson(input.input ?? {});
        const authorization = cloneJson(input.authorization ?? {});
        const queuePriority = workflowQueuePriority(input.queuePriority);
        const resumedFromRunId = optionalText(input.resumedFromRunId);
        const runFamilyBudget = normalizeRunFamilyBudget(input?.runFamilyBudget);
        if (resumedFromRunId && runFamilyBudget) throw new TypeError("Only a root workflow run can declare a run family budget.");
        if (resumedFromRunId) assertResumeSource(runs.get(resumedFromRunId), { owner, workflowType: version.workflowType });
        const identity = {
          version: version.id, owner, input: normalizedInput, authorization: identityAuthorization(input, authorization), queuePriority, resumedFromRunId,
        };
        if (runFamilyBudget) identity.runFamilyBudget = runFamilyBudget;
        const identityHash = hashValue(identity);
        const existing = runs.get(runId);
        if (existing) {
          if (existing.identityHash !== identityHash) {
            throw conflict(`Workflow run ${runId} already exists with different input.`);
          }
          return publicRunSummary(existing);
        }
        const familyRootId = resumedFromRunId
          ? (runFamilies.has(resumedFromRunId) ? resumedFromRunId : runFamilyMembers.get(resumedFromRunId))
          : null;
        const family = familyRootId ? runFamilies.get(familyRootId) : null;
        if (family && family.memberCount >= family.maxDescendantRuns) {
          throw conflict(`Workflow run family ${familyRootId} has exhausted its descendant budget.`);
        }
        const run = {
          runId,
          workflowVersionId: version.id,
          workflowType: version.workflowType,
          owner,
          input: normalizedInput,
          inputHash: hashValue(normalizedInput),
          authorization,
          identityHash,
          status: "queued",
          queuePriority,
          recoveryCount: 0,
          resumedFromRunId,
          claim: null,
          createdAt: timestamp,
          updatedAt: timestamp,
          startedAt: null,
          completedAt: null,
          failure: null,
          assignments: [],
          events: [],
          stagedCommands: [],
          validations: [],
          admission: null,
        };
        runs.set(runId, run);
        if (runFamilyBudget) runFamilies.set(runId, {
          maxDescendantRuns: runFamilyBudget.maxDescendantRuns,
          version: runFamilyBudget.version,
          maxTotalAttempts: runFamilyBudget.version >= 2 ? runFamilyBudget.maxTotalAttempts : null,
          maxTotalTokens: runFamilyBudget.version === 3 ? runFamilyBudget.maxTotalTokens : null,
          memberCount: 0,
        });
        if (family) {
          runFamilyMembers.set(runId, familyRootId);
          family.memberCount += 1;
        }
        return publicRunSummary(run);
      });
    },

    findRunByIdempotencyKey(input) {
      return transact(async () => {
        const owner = normalizeOwner(input?.owner);
        const idempotencyKey = requireText(input?.idempotencyKey, "Workflow idempotency key");
        const runId = resolveRunId({ owner, idempotencyKey }, createId);
        const run = runs.get(runId);
        if (!run) return null;
        if (stableJson(run.owner) !== stableJson(owner)) return null;
        return withMetrics(clone(run));
      });
    },

    cancelQueuedRun(input, options) {
      const {runId, owner, authorize} = queuedCancellationInput(input, options);
      return transact(async () => {
        const run = runs.get(runId);
        if (!run || stableJson(run.owner) !== stableJson(owner)) throw notFoundRun();
        await authorizeQueuedCancellation(run, authorize);
        if (!assertQueuedCancellation(run)) return clone(run);
        run.status = 'cancelled';
        run.failure = {code: 'WORKFLOW_QUEUED_CANCELLED'};
        run.updatedAt = run.completedAt = now();
        return clone(run);
      });
    },

    requestRunCancellation(input, options) {
      const {runId, owner, authorize} = runningCancellationInput(input, options);
      return transact(async () => {
        const run = runs.get(runId);
        if (!run || stableJson(run.owner) !== stableJson(owner)) throw notFoundRun();
        await authorizeRunningCancellation(run, authorize, versions.get(run.workflowVersionId));
        if (assertCancellationRequest(run)) {
          const timestamp = now();
          run.events.push(domainEvent(runId, cancellationRequestIdentity,
            cancellationIntent(run, timestamp), timestamp));
          run.updatedAt = timestamp;
        }
        return {run: clone(run), cancellation: cancellationState(run)};
      });
    },

    inspectRunCancellation(input) {
      const expected = cancellationClaimInput(input);
      return transact(() => {
        const run = requireRun(runs, input.runId);
        assertMemoryExpectedClaim(run, expected, now);
        return cancellationState(run);
      });
    },

    confirmRunCancellation(input) {
      const expected = cancellationConfirmationInput(input);
      return transact(() => {
        const run = requireRun(runs, input.runId);
        if (assertCancellationConfirmation(run, expected)) return clone(run);
        assertMemoryExpectedClaim(run, expected, now);
        const timestamp = now();
        run.events.push(domainEvent(run.runId, cancellationConfirmationIdentity,
          cancellationConfirmation(run, timestamp), timestamp));
        run.status = 'cancelled'; run.failure = {code: 'WORKFLOW_RUNNING_CANCELLED'};
        run.updatedAt = run.completedAt = timestamp; run.claim = null; run.finalSummary = null;
        return clone(run);
      });
    },

    claimRun(input) {
      return transact(async () => {
        const workerId = requireText(input?.workerId, "Worker id");
        const workflowType = optionalText(input?.workflowType);
        const leaseSeconds = boundedInteger(input?.leaseSeconds ?? 120, "Run lease seconds", 30, 3600);
        const timestamp = now();
        const selected = [...runs.values()]
          .filter((run) => !run.events.some(event => event.sourceKey === cancellationRequestIdentity)
            && (!workflowType || run.workflowType === workflowType)
            && (run.status === "queued"
              || (["running", "ready-for-admission"].includes(run.status) && isExpired(run.claim, timestamp))))
          .sort((left, right) => right.queuePriority - left.queuePriority
            || left.createdAt.localeCompare(right.createdAt) || left.runId.localeCompare(right.runId))[0];
        if (!selected) return null;
        if (["running", "ready-for-admission"].includes(selected.status)) selected.recoveryCount += 1;
        if (selected.status === "queued") selected.status = "running";
        selected.startedAt ??= timestamp;
        selected.updatedAt = timestamp;
        selected.claim = {
          workerId,
          claimedAt: timestamp,
          heartbeatAt: timestamp,
          expiresAt: addSeconds(timestamp, leaseSeconds),
        };
        return clone(selected);
      });
    },

    heartbeatRun(input) {
      return transact(async () => {
        const run = requireRun(runs, input?.runId);
        const workerId = requireText(input?.workerId, "Worker id");
        const claimedAt = input?.claimedAt == null ? null : isoTimestamp(input.claimedAt, "Workflow claim epoch");
        const leaseSeconds = boundedInteger(input?.leaseSeconds ?? 120, "Run lease seconds", 30, 3600);
        const timestamp = now();
        if (!["running", "ready-for-admission"].includes(run.status)
          || run.claim?.workerId !== workerId
          || (claimedAt && run.claim?.claimedAt !== claimedAt)
          || isExpired(run.claim, timestamp)) {
          if (claimedAt) throw claimLostError();
          throw conflict(`Workflow run ${run.runId} is not leased by ${workerId}.`);
        }
        run.claim.heartbeatAt = timestamp;
        run.claim.expiresAt = addSeconds(timestamp, leaseSeconds);
        run.updatedAt = timestamp;
        return clone(run.claim);
      });
    },

    createAssignment(input) {
      return transact(async () => {
        const run = requireRun(runs, input?.runId);
        assertMemoryExpectedClaim(run, input?.expectedClaim, now);
        const assignmentId = input.assignmentId == null ? createId() : requireText(input.assignmentId, "Assignment id");
        const existing = findAssignment(runs, assignmentId, { required: false });
        const normalized = normalizeAssignment(input, { assignmentId, timestamp: now() });
        assertRunAcceptsAssignment(run, normalized);
        const identityHash = hashValue({
          runId: run.runId,
          parentAssignmentId: normalized.parentAssignmentId,
          stepKey: normalized.stepKey,
          assignedTo: normalized.assignedTo,
          execution: normalized.execution,
          targets: normalized.targets,
        });
        if (existing) {
          if (existing.assignment.identityHash !== identityHash) {
            throw conflict(`Assignment ${assignmentId} already exists with different input.`);
          }
          return clone(existing.assignment);
        }
        if (normalized.parentAssignmentId) {
          const parent = findAssignment(runs, normalized.parentAssignmentId);
          if (parent.run.runId !== run.runId) throw new TypeError("Parent assignment must belong to the same workflow run.");
        }
        const requestedTargets = new Set(normalized.targets.map((target) => `${target.type}:${target.id}`));
        for (const candidateRun of runs.values()) {
          for (const assignment of candidateRun.assignments) {
            if (!openAssignmentStatuses.has(assignment.status)) continue;
            const overlap = assignment.targets.find((target) => requestedTargets.has(`${target.type}:${target.id}`));
            if (overlap) throw conflict(`Target ${overlap.type}:${overlap.id} is already assigned by ${assignment.assignmentId}.`);
          }
        }
        normalized.identityHash = identityHash;
        run.assignments.push(normalized);
        run.updatedAt = normalized.createdAt;
        return clone(normalized);
      });
    },

    createPreparedAssignment(input) {
      return transact(async () => {
        const run = requireRun(runs, input?.runId);
        assertMemoryExpectedClaim(run, input?.expectedClaim, now);
        const assignmentId = requireText(input?.assignmentId, "Assignment id");
        const prepared = normalizePreparedAssignment(input);
        const existing = findAssignment(runs, assignmentId, { required: false });
        if (existing) {
          if (existing.run.runId !== run.runId) throw conflict(`Assignment ${assignmentId} belongs to another run.`);
          return memoryPreparedResult(run, existing.assignment, prepared);
        }
        const assignment = normalizeAssignment(input, { assignmentId, timestamp: now() });
        assertRunAcceptsAssignment(run, assignment);
        assertPreparedExecutionRef({ run, assignment, executionRef: prepared.executionRef });
        const requestedTargets = new Set(assignment.targets.map((target) => `${target.type}:${target.id}`));
        for (const candidateRun of runs.values()) {
          for (const candidate of candidateRun.assignments) {
            if (!openAssignmentStatuses.has(candidate.status)) continue;
            const overlap = candidate.targets.find((target) => requestedTargets.has(`${target.type}:${target.id}`));
            if (overlap) throw conflict(`Target ${overlap.type}:${overlap.id} is already assigned by ${candidate.assignmentId}.`);
          }
        }
        assignment.identityHash = assignmentIdentityHash(run.runId, assignment);
        const timestamp = assignment.createdAt;
        const event = preparedAssignmentEvent({ runId: run.runId, assignmentId, prepared, timestamp });
        run.assignments.push(assignment);
        run.events.push(event);
        run.updatedAt = timestamp;
        return preparedResult(assignment, event.payload, true);
      });
    },

    startAttempt(input) {
      return transact(async () => {
        const { run, assignment } = findAssignment(runs, input?.assignmentId);
        assertMemoryExpectedClaim(run, input?.expectedClaim, now);
        if (!openAssignmentStatuses.has(assignment.status)) {
          throw conflict(`Assignment ${assignment.assignmentId} is terminal.`);
        }
        if (assignment.attempts.some((attempt) => attempt.outcome === null)) {
          throw conflict(`Assignment ${assignment.assignmentId} already has an active attempt.`);
        }
        const attemptId = input.attemptId == null ? createId() : requireText(input.attemptId, "Attempt id");
        const prior = findAttempt(runs, attemptId, { required: false });
        const timestamp = now();
        const prompt = assertSafeAuditPrompt(input?.prompt, { label: "Attempt prompt" });
        const requested = normalizeModelSettings(input?.requested, { requireModel: true });
        const capabilityProfile = cloneJson(input?.capabilityProfile ?? { allowedTools: [] });
        const attempt = {
          attemptId,
          assignmentId: assignment.assignmentId,
          ordinal: assignment.attempts.length + 1,
          prompt,
          promptHash: hashValue(prompt),
          requested,
          observed: null,
          capabilityProfile,
          capabilityHash: hashValue(capabilityProfile),
          threadId: optionalText(input?.threadId),
          turnId: optionalText(input?.turnId),
          startedAt: timestamp,
          completedAt: null,
          outcome: null,
          telemetryStatus: "pending",
          usage: null,
          price: null,
          apiEquivalentCost: null,
          billedCost: null,
          result: null,
          resultHash: null,
          error: null,
          finishHash: null,
        };
        if (prior) throw conflict(`Attempt ${attemptId} already exists.`);
        const familyRootId = runFamilies.has(run.runId) ? run.runId : runFamilyMembers.get(run.runId);
        const family = familyRootId ? runFamilies.get(familyRootId) : null;
        const resourceReservation = normalizeResourceReservation(input?.resourceReservation, family?.version === 3);
        if (family?.maxTotalAttempts !== null && family?.maxTotalAttempts !== undefined) {
          let totalAttempts = 0;
          for (const candidate of runs.values()) {
            if (candidate.runId === familyRootId || runFamilyMembers.get(candidate.runId) === familyRootId) {
              for (const memberAssignment of candidate.assignments) totalAttempts += memberAssignment.attempts.length;
            }
          }
          if (totalAttempts >= family.maxTotalAttempts) {
            throw conflict(`Workflow run family ${familyRootId} total attempt budget is exhausted.`);
          }
        }
        if (family?.version === 3) {
          const budget = memoryFamilyBudget(runs, runFamilyMembers, familyRootId, family);
          if (BigInt(budget.overrunTokens) > 0n || BigInt(budget.measuredTokens) + BigInt(budget.heldTokens)
            + BigInt(resourceReservation.totalTokens) > BigInt(family.maxTotalTokens)) {
            throw conflict(`Workflow run family ${familyRootId} total token budget is exhausted.`);
          }
        }
        if (resourceReservation) {
          attempt.resourceReservation = resourceReservation;
          attempt.resourceUsageBasis = null;
        }
        assignment.attempts.push(attempt);
        assignment.status = "running";
        assignment.startedAt ??= timestamp;
        assignment.updatedAt = timestamp;
        run.updatedAt = timestamp;
        return clone(attempt);
      });
    },

    bindAttempt(input) {
      return transact(async () => {
        const { run, assignment, attempt } = findAttempt(runs, input?.attemptId);
        assertMemoryExpectedClaim(run, input?.expectedClaim, now);
        const threadId = optionalText(input?.threadId);
        const turnId = optionalText(input?.turnId);
        if (!threadId && !turnId) throw new TypeError("Attempt binding requires a thread id or turn id.");
        if (attempt.threadId && threadId && attempt.threadId !== threadId) throw conflict(`Attempt ${attempt.attemptId} is already bound to another thread.`);
        if (attempt.turnId && turnId && attempt.turnId !== turnId) throw conflict(`Attempt ${attempt.attemptId} is already bound to another turn.`);
        if (assignment.agentThreadId && threadId && assignment.agentThreadId !== threadId) {
          throw conflict(`Assignment ${assignment.assignmentId} is already bound to another thread.`);
        }
        const timestamp = now();
        attempt.threadId ??= threadId;
        attempt.turnId ??= turnId;
        assignment.agentThreadId ??= threadId;
        assignment.updatedAt = timestamp;
        run.updatedAt = timestamp;
        return clone(attempt);
      });
    },

    observeAssignment(input) {
      return transact(async () => {
        const { run, assignment } = findAssignment(runs, input?.assignmentId);
        assertMemoryExpectedClaim(run, input?.expectedClaim, now);
        const observationId = boundedText(input?.observationId, "Assignment observation id", 500);
        const status = enumValue(input?.status, "Assignment observation status", new Set([...openAssignmentStatuses, ...terminalAssignmentStatuses]));
        const threadId = requireText(input?.threadId, "Observed agent thread id");
        const sourceKey = `assignment-observation:${observationId}`;
        const payload = { observationId, status, threadId, message: optionalText(input?.message) };
        const payloadHash = hashValue(payload);
        const existing = run.events.find((event) => event.sourceKey === sourceKey);
        if (existing) {
          if (existing.payloadHash !== payloadHash) throw conflict(`Workflow event ${sourceKey} already has a different payload.`);
          return clone(assignment);
        }
        if (terminalAssignmentStatuses.has(assignment.status) && openAssignmentStatuses.has(status)) {
          throw conflict(`Terminal assignment ${assignment.assignmentId} cannot return to ${status}.`);
        }
        if (assignment.agentThreadId && assignment.agentThreadId !== threadId) {
          throw conflict(`Assignment ${assignment.assignmentId} is already bound to another thread.`);
        }
        if (terminalAssignmentStatuses.has(status) && assignment.attempts.some((attempt) => attempt.outcome === null)) {
          throw conflict(`Assignment ${assignment.assignmentId} still has an active attempt.`);
        }
        const timestamp = now();
        assignment.agentThreadId = threadId;
        assignment.status = status;
        assignment.startedAt ??= status === "running" ? timestamp : null;
        assignment.completedAt = status === "completed" ? (assignment.completedAt ?? timestamp) : assignment.completedAt;
        if (terminalAssignmentStatuses.has(status)) assignment.finishedAt ??= timestamp;
        assignment.failure = status === "failed" ? (payload.message ?? "Agent execution failed.") : assignment.failure;
        assignment.updatedAt = timestamp;
        run.updatedAt = timestamp;
        run.events.push({
          eventId: `event-${hashValue({ runId: run.runId, sourceKey }).slice(0, 24)}`,
          runId: run.runId,
          assignmentId: assignment.assignmentId,
          attemptId: null,
          sourceKey,
          eventType: "workflow.assignment-observed",
          threadId,
          turnId: null,
          occurredAt: input?.occurredAt == null ? null : isoTimestamp(input.occurredAt, "Assignment observation time"),
          receivedAt: timestamp,
          payload,
          payloadHash,
        });
        return clone(assignment);
      });
    },

    appendEvent(input) {
      return transact(async () => {
        assertOrdinaryEvent(input);
        const run = requireRun(runs, input?.runId);
        assertMemoryExpectedClaim(run, input?.expectedClaim, now);
        const sourceKey = boundedText(input?.sourceKey, "Workflow event source key", 500);
        const payload = cloneJson(input?.payload ?? {});
        const payloadHash = hashValue(payload);
        const existing = run.events.find((event) => event.sourceKey === sourceKey);
        if (existing) {
          if (existing.payloadHash !== payloadHash || eventIdentityHash(existing) !== eventInputIdentityHash(input, payloadHash)) {
            throw conflict(`Workflow event ${sourceKey} already has a different payload.`);
          }
          return clone(existing);
        }
        const assignmentId = optionalText(input?.assignmentId);
        const attemptId = optionalText(input?.attemptId);
        if (assignmentId) {
          const found = findAssignment(runs, assignmentId);
          if (found.run.runId !== run.runId) throw new TypeError("Workflow event assignment belongs to another run.");
        }
        if (attemptId) {
          const found = findAttempt(runs, attemptId);
          if (found.run.runId !== run.runId || (assignmentId && found.assignment.assignmentId !== assignmentId)) {
            throw new TypeError("Workflow event attempt does not match its run and assignment.");
          }
        }
        const timestamp = now();
        const event = {
          eventId: `event-${hashValue({ runId: run.runId, sourceKey }).slice(0, 24)}`,
          runId: run.runId,
          assignmentId,
          attemptId,
          sourceKey,
          eventType: requireText(input?.eventType, "Workflow event type"),
          threadId: optionalText(input?.threadId),
          turnId: optionalText(input?.turnId),
          occurredAt: input?.occurredAt == null ? null : isoTimestamp(input.occurredAt, "Workflow event occurrence time"),
          receivedAt: timestamp,
          payload,
          payloadHash,
        };
        run.events.push(event);
        run.updatedAt = timestamp;
        return clone(event);
      });
    },

    finishAttempt(input) {
      return transact(async () => {
        const { run, assignment, attempt } = findAttempt(runs, input?.attemptId);
        assertMemoryExpectedClaim(run, input?.expectedClaim, now);
        const familyRootId = runFamilies.has(run.runId) ? run.runId : runFamilyMembers.get(run.runId);
        const isTokenBudgeted = runFamilies.get(familyRootId)?.version === 3;
        const outcome = enumValue(input?.outcome, "Attempt outcome", terminalAttemptOutcomes);
        const result = cloneJson(input?.result ?? {});
        const finish = {
          outcome,
          observed: normalizeModelSettings(input?.observed, { requireModel: false }),
          usage: input?.usage == null ? null
            : isTokenBudgeted && input?.resourceUsageBasis === "unknown"
              ? normalizeUnknownUsage(input.usage) : normalizeUsage(input.usage),
          telemetryStatus: enumValue(input?.telemetryStatus, "Attempt telemetry status", new Set(["complete", "incomplete", "inconsistent"])),
          result,
          error: optionalText(input?.error),
          price: input?.price == null ? null : normalizeAttemptPrice(input.price),
          apiEquivalentCost: input?.apiEquivalentCost == null ? null : normalizeMoney(input.apiEquivalentCost, "API-equivalent cost"),
          billedCost: input?.billedCost == null ? null : normalizeMoney(input.billedCost, "Billed cost"),
        };
        const resourceUsageBasis = normalizeResourceUsageBasis(input?.resourceUsageBasis, isTokenBudgeted, finish, input?.usage);
        if (isTokenBudgeted) finish.resourceUsageBasis = resourceUsageBasis;
        const finishHash = hashValue(finish);
        if (attempt.outcome !== null) {
          if (attempt.finishHash !== finishHash) throw conflict(`Attempt ${attempt.attemptId} is already complete with a different result.`);
          return clone(attempt);
        }
        const timestamp = now();
        Object.assign(attempt, finish, {
          resultHash: hashValue(result),
          completedAt: timestamp,
          finishHash,
        });
        assignment.updatedAt = timestamp;
        run.updatedAt = timestamp;
        return clone(attempt);
      });
    },

    finishAssignment(input) {
      return transact(async () => {
        const { run, assignment } = findAssignment(runs, input?.assignmentId);
        assertMemoryExpectedClaim(run, input?.expectedClaim, now);
        const status = enumValue(input?.status, "Assignment status", terminalAssignmentStatuses);
        if (assignment.attempts.some((attempt) => attempt.outcome === null)) {
          throw conflict(`Assignment ${assignment.assignmentId} still has an active attempt.`);
        }
        const finishHash = hashValue({ status, failure: optionalText(input?.failure) });
        if (terminalAssignmentStatuses.has(assignment.status)) {
          if (assignment.finishHash !== finishHash) {
            throw conflict(`Assignment ${assignment.assignmentId} is already complete with a different result.`);
          }
          return clone(assignment);
        }
        const timestamp = now();
        assignment.status = status;
        assignment.failure = optionalText(input?.failure);
        assignment.completedAt = status === "completed" ? timestamp : null;
        assignment.finishedAt = timestamp;
        assignment.updatedAt = timestamp;
        assignment.finishHash = finishHash;
        run.updatedAt = timestamp;
        return clone(assignment);
      });
    },

    transitionRun(input) {
      return transact(async () => {
        const run = requireRun(runs, input?.runId);
        assertMemoryExpectedClaim(run, input?.expectedClaim, now);
        const status = enumValue(input?.status, "Workflow run status", runStatuses);
        if (run.status === status) return clone(run);
        assertCancellationTransition(run.events, status);
        assertRunTransition(run, status, input);
        const version = versions.get(run.workflowVersionId);
        if (status === "completed") {
          await assertCompletion(run, version, input?.finalSummary);
        }
        const timestamp = now();
        run.status = status;
        run.updatedAt = timestamp;
        if (status === "ready-for-admission") {
          run.validatedManifestHash = requireHash(input.manifestHash, "Validated manifest hash");
          run.lastValidationPassedAt = timestamp;
        }
        if (status === "admitted") {
          run.admittedManifestHash = requireHash(input.manifestHash, "Admitted manifest hash");
          run.admission = cloneJson(input.receipt);
          run.finalSummary = cloneJson(input.finalSummary ?? input.receipt);
          run.completedAt = timestamp;
          run.claim = null;
        }
        if (status === "completed") {
          run.finalSummary = cloneJson(input.finalSummary);
          run.completedAt = timestamp;
          run.claim = null;
        }
        if (["failed", "cancelled", "blocked"].includes(status)) {
          run.failure = input.failure == null ? null : cloneJson(input.failure);
          run.finalSummary = input.finalSummary == null ? null : cloneJson(input.finalSummary);
          run.completedAt = timestamp;
          run.claim = null;
        }
        return clone(run);
      });
    },

    getRunSummary(input, options) {
      return authorizedRunSummary(input, options, request => transact(() => {
        const run = runs.get(request.runId);
        return run && stableJson(run.owner) === stableJson(request.owner) ? projectRunSummary(run) : null;
      }));
    },
    inspectRun(input) {
      return transact(async () => {
        const run = requireRun(runs, input?.runId);
        if (input?.owner) assertOwner(run.owner, normalizeOwner(input.owner));
        return withMetrics(clone(run));
      });
    },
    inspectAssignment(input) {
      return transact(async () => {
        const runId = requireText(input?.runId, "Workflow run id");
        const assignmentId = requireText(input?.assignmentId, "Assignment id");
        const run = requireRun(runs, runId);
        if (input?.owner) assertOwner(run.owner, normalizeOwner(input.owner));
        const assignment = run.assignments.find((candidate) => candidate.assignmentId === assignmentId);
        if (!assignment) throw new TypeError(`Unknown workflow assignment: ${assignmentId}.`);
        return clone(assignment);
      });
    },
    inspectRunFamilyBudget(input) {
      return transact(async () => {
        const run = requireRun(runs, input?.runId);
        if (input?.owner) assertOwner(run.owner, normalizeOwner(input.owner));
        const rootRunId = runFamilies.has(run.runId) ? run.runId : runFamilyMembers.get(run.runId);
        if (!rootRunId) return null;
        return { ...memoryFamilyBudget(runs, runFamilyMembers, rootRunId, runFamilies.get(rootRunId)), runId: run.runId };
      });
    },
  };
}

function createPostgresJournal({ database, createId, now, relations, familyRelations, lockResumeSource }) {
  if (typeof database.transaction !== "function" || typeof database.query !== "function") {
    throw new TypeError("PostgreSQL AgentWorkflowJournal requires a transaction-capable database.");
  }
  relations ??= resolvePostgresRelations(undefined, postgresRelationKeys, "Agent workflow journal");
  const runSchema = relations.runs.slice(0, relations.runs.indexOf("."));
  const hasCleanJournalRelations = Object.entries(cleanJournalRelationNames).every(
    ([key, name]) => relations[key] === `${runSchema}."${name}"`,
  );
  if (familyRelations && (!hasCleanJournalRelations
    || familyRelations.families !== `${runSchema}."agent_workflow_run_families"`
    || familyRelations.members !== `${runSchema}."agent_workflow_run_family_members"`)) {
    throw new TypeError("PostgreSQL run family relations must bind the clean journal schema and exact family tables.");
  }
  async function createRunInTransaction(client, input, parentLock = "FOR SHARE", authorizeParent) {
    const workflowVersionId = requireText(input?.workflowVersionId, "Workflow version id");
    const runId = resolveRunId(input, createId);
    const owner = normalizeOwner(input?.owner);
    const payload = cloneJson(input?.input ?? {});
    const authorization = cloneJson(input?.authorization ?? {});
    const queuePriority = workflowQueuePriority(input?.queuePriority);
    const resumedFromRunId = optionalText(input?.resumedFromRunId);
    const runFamilyBudget = normalizeRunFamilyBudget(input?.runFamilyBudget);
    if (resumedFromRunId && runFamilyBudget) throw new TypeError("Only a root workflow run can declare a run family budget.");
    if (runFamilyBudget && !familyRelations) {
      throw new TypeError("Budgeted PostgreSQL workflow runs require run family relations.");
    }
    const timestamp = now();
    const versionResult = await client.query(
      `SELECT id,workflow_type FROM ${relations.versions} WHERE id=$1`,
      [workflowVersionId],
    );
    const version = versionResult.rows[0];
    if (!version) throw new TypeError(`Unknown workflow version: ${workflowVersionId}.`);
    let familyRootId = null;
    if (resumedFromRunId && familyRelations) {
      const lockedFamily = await client.query(
        `SELECT ${runSchema}.lock_agent_workflow_run_family($1) AS root_run_id`,
        [resumedFromRunId],
      );
      familyRootId = lockedFamily.rows[0]?.root_run_id ?? null;
    }
    if (resumedFromRunId) {
      let source;
      if (lockResumeSource) {
        source = await lockResumeSource({ client, runId: resumedFromRunId, owner, lockMode: parentLock });
        if (source != null) {
          if (typeof source !== "object" || Array.isArray(source)) {
            throw new TypeError("Resume source lock returned an invalid run.");
          }
          source = {
            owner: normalizeOwner(source.owner),
            status: requireText(source.status, "Resume source status"),
            workflowType: requireText(source.workflowType, "Resume source workflow type"),
          };
        }
      } else {
        const locked = await client.query(
          `SELECT * FROM ${relations.runs} WHERE id=$1 ${parentLock}`,
          [resumedFromRunId],
        );
        source = locked.rows[0] ? {
          owner: mapOwnerRow(locked.rows[0]), status: locked.rows[0].status,
          workflowType: locked.rows[0].workflow_type,
        } : null;
      }
      assertResumeSource(source, { owner, workflowType: version.workflow_type });
      if (familyRootId) {
        const checkedFamily = await client.query(`
          SELECT root_run_id FROM ${familyRelations.families} WHERE root_run_id=$1
          UNION ALL
          SELECT root_run_id FROM ${familyRelations.members} WHERE run_id=$1
        `, [resumedFromRunId]);
        if (checkedFamily.rows.length !== 1 || checkedFamily.rows[0].root_run_id !== familyRootId) {
          throw conflict("Workflow run family ancestry changed during creation.");
        }
      }
    }
    if (authorizeParent) await authorizeParent();
    const identity = {
      version: workflowVersionId, owner, input: payload, authorization: identityAuthorization(input, authorization), queuePriority, resumedFromRunId,
    };
    if (runFamilyBudget) identity.runFamilyBudget = runFamilyBudget;
    const identityHash = hashValue(identity);
    const values = postgresOwnerValues(owner);
    const inserted = await client.query(`
      INSERT INTO ${relations.runs} (
        id,workflow_version_id,workflow_type,owner_kind,owner_principal_id,
        owner_account_id,owner_assistant_session_id,authorization_snapshot,
        input_payload,input_sha256,identity_sha256,status,queue_priority,
        resumed_from_run_id,created_at,updated_at${runFamilyBudget ? ",run_family_budget_version,run_family_max_descendant_runs" : ""}${runFamilyBudget?.version >= 2 ? ",run_family_max_total_attempts" : ""}${runFamilyBudget?.version === 3 ? ",run_family_max_total_tokens" : ""}
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb,$10,$11,'queued',$12,$13,$14,$14${runFamilyBudget ? ",$15,$16" : ""}${runFamilyBudget?.version >= 2 ? ",$17" : ""}${runFamilyBudget?.version === 3 ? ",$18" : ""})
      ON CONFLICT (id) DO NOTHING
      ${runFamilyBudget || familyRootId ? "RETURNING id" : ""}
    `, [
      runId, workflowVersionId, version.workflow_type, owner.kind, values.principalId,
      values.accountId, values.assistantSessionId, JSON.stringify(authorization),
      JSON.stringify(payload), hashValue(payload), identityHash, queuePriority,
      resumedFromRunId, timestamp, ...(runFamilyBudget ? [runFamilyBudget.version, runFamilyBudget.maxDescendantRuns] : []),
      ...(runFamilyBudget?.version >= 2 ? [runFamilyBudget.maxTotalAttempts] : []),
      ...(runFamilyBudget?.version === 3 ? [runFamilyBudget.maxTotalTokens] : []),
    ]);
    if (inserted.rows[0] && runFamilyBudget) {
      await client.query(`
        INSERT INTO ${familyRelations.families} (root_run_id,budget_version,max_descendant_runs${runFamilyBudget.version >= 2 ? ",max_total_attempts" : ""}${runFamilyBudget.version === 3 ? ",max_total_tokens" : ""})
        VALUES ($1,$2,$3${runFamilyBudget.version >= 2 ? ",$4" : ""}${runFamilyBudget.version === 3 ? ",$5" : ""})
      `, [runId, runFamilyBudget.version, runFamilyBudget.maxDescendantRuns,
        ...(runFamilyBudget.version >= 2 ? [runFamilyBudget.maxTotalAttempts] : []),
        ...(runFamilyBudget.version === 3 ? [runFamilyBudget.maxTotalTokens] : [])]);
    }
    if (inserted.rows[0] && familyRootId) {
      await client.query(`
        INSERT INTO ${familyRelations.members} (run_id,root_run_id,parent_run_id)
        VALUES ($1,$2,$3)
      `, [runId, familyRootId, resumedFromRunId]);
    }
    const result = await client.query(`
      SELECT id,workflow_version_id,workflow_type,status,created_at,updated_at,identity_sha256
      FROM ${relations.runs} WHERE id=$1
    `, [runId]);
    const row = result.rows[0];
    if (!row) throw new Error(`Workflow run ${runId} could not be persisted.`);
    if (row.identity_sha256 !== identityHash) {
      throw conflict(`Workflow run ${runId} already exists with different input.`);
    }
    if (familyRelations) {
      const storedRun = await client.query(`
        SELECT resumed_from_run_id,run_family_budget_version,run_family_max_descendant_runs,run_family_max_total_attempts,run_family_max_total_tokens
        FROM ${relations.runs} WHERE id=$1
      `, [runId]);
      const recorded = storedRun.rows[0];
      if (!recorded) throw conflict(`Workflow run ${runId} lost its family identity.`);
      if (runFamilyBudget) {
        const storedFamily = await client.query(`
          SELECT root_run_id,budget_version,max_descendant_runs,max_total_attempts,max_total_tokens
          FROM ${familyRelations.families} WHERE root_run_id=$1
        `, [runId]);
        const declaration = storedFamily.rows[0];
        if (recorded.resumed_from_run_id !== null
          || Number(recorded.run_family_budget_version) !== runFamilyBudget.version
          || String(recorded.run_family_max_descendant_runs) !== String(runFamilyBudget.maxDescendantRuns)
          || (recorded.run_family_max_total_attempts === null ? null : String(recorded.run_family_max_total_attempts))
            !== (runFamilyBudget.version >= 2 ? String(runFamilyBudget.maxTotalAttempts) : null)
          || (recorded.run_family_max_total_tokens === null ? null : String(recorded.run_family_max_total_tokens))
            !== (runFamilyBudget.version === 3 ? String(runFamilyBudget.maxTotalTokens) : null)
          || declaration?.root_run_id !== runId
          || Number(declaration.budget_version) !== runFamilyBudget.version
          || String(declaration.max_descendant_runs) !== String(runFamilyBudget.maxDescendantRuns)
          || (declaration.max_total_attempts === null ? null : String(declaration.max_total_attempts))
            !== (runFamilyBudget.version >= 2 ? String(runFamilyBudget.maxTotalAttempts) : null)
          || (declaration.max_total_tokens === null ? null : String(declaration.max_total_tokens))
            !== (runFamilyBudget.version === 3 ? String(runFamilyBudget.maxTotalTokens) : null)) {
          throw conflict(`Workflow run ${runId} has a different family budget declaration.`);
        }
      } else {
        const storedFamily = await client.query(`
          SELECT root_run_id FROM ${familyRelations.families} WHERE root_run_id=$1
        `, [runId]);
        const storedMember = await client.query(`
          SELECT run_id,root_run_id,parent_run_id FROM ${familyRelations.members} WHERE run_id=$1
        `, [runId]);
        const member = storedMember.rows[0];
        if (recorded.resumed_from_run_id !== resumedFromRunId
          || recorded.run_family_budget_version !== null
          || recorded.run_family_max_descendant_runs !== null
          || recorded.run_family_max_total_attempts !== null
          || recorded.run_family_max_total_tokens !== null
          || storedFamily.rows.length !== 0
          || (familyRootId
            ? (member?.run_id !== runId || member.root_run_id !== familyRootId
              || member.parent_run_id !== resumedFromRunId)
            : storedMember.rows.length !== 0)) {
          throw conflict(`Workflow run ${runId} has different family membership.`);
        }
      }
    }
    return {
      runId: row.id,
      workflowVersionId: row.workflow_version_id,
      workflowType: row.workflow_type,
      status: row.status,
      createdAt: asIso(row.created_at),
      updatedAt: asIso(row.updated_at),
    };
  }

  async function appendEventInTransaction(client, input, authority) {
    if (authority !== revisionHandoffWrite && authority !== cancellationWrite) assertOrdinaryEvent(input);
    const runId = requireText(input?.runId, "Workflow run id");
    const sourceKey = boundedText(input?.sourceKey, "Workflow event source key", 500);
    const assignmentId = optionalText(input?.assignmentId);
    const attemptId = optionalText(input?.attemptId);
    if (attemptId && !assignmentId) throw new TypeError("Workflow event attempt requires an assignment.");
    const payload = cloneJson(input?.payload ?? {});
    const payloadHash = hashValue(payload);
    const receivedAt = now();
    if (assignmentId) {
      const assignment = await client.query(
        `SELECT id,run_id FROM ${relations.assignments} WHERE id=$1 FOR UPDATE`,
        [assignmentId],
      );
      if (!assignment.rows[0] || assignment.rows[0].run_id !== runId) {
        throw conflict("Workflow event assignment does not belong to its run.");
      }
    }
    let attemptOutcome = null;
    if (attemptId) {
      const attempt = await client.query(
        `SELECT id,run_id,assignment_id,outcome FROM ${relations.attempts} WHERE id=$1 FOR UPDATE`,
        [attemptId],
      );
      if (!attempt.rows[0] || attempt.rows[0].run_id !== runId
        || attempt.rows[0].assignment_id !== assignmentId) {
        throw conflict("Workflow event attempt does not belong to its assignment.");
      }
      attemptOutcome = attempt.rows[0].outcome;
    }
    const run = await client.query(
      `SELECT id FROM ${relations.runs} WHERE id=$1 FOR UPDATE`,
      [runId],
    );
    if (!run.rows[0]) throw notFoundRun();
    await assertPostgresExpectedClaim(client, relations, runId, input?.expectedClaim);
    const result = await client.query(`
      INSERT INTO ${relations.events} (
        run_id,assignment_id,attempt_id,source_key,event_type,thread_id,turn_id,
        occurred_at,received_at,payload,payload_sha256
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11)
      ON CONFLICT (run_id,source_key) DO NOTHING
      RETURNING *
    `, [
      runId, assignmentId, attemptId, sourceKey,
      requireText(input?.eventType, "Workflow event type"), optionalText(input?.threadId),
      optionalText(input?.turnId), input?.occurredAt == null ? null : isoTimestamp(input.occurredAt, "Workflow event occurrence time"),
      receivedAt, JSON.stringify(payload), payloadHash,
    ]);
    const inserted = result.rows.length > 0;
    let row = result.rows[0];
    if (!row) {
      const existing = await client.query(
        `SELECT * FROM ${relations.events} WHERE run_id=$1 AND source_key=$2`,
        [runId, sourceKey],
      );
      row = existing.rows[0];
      if (!row || row.payload_sha256 !== payloadHash
        || eventIdentityHash(mapEventRow(row)) !== eventInputIdentityHash(input, payloadHash)) {
        throw conflict(`Workflow event ${sourceKey} already has a different payload.`);
      }
    }
    await observeJournalEvent(client, row, receivedAt, { inserted, attemptOutcome });
    await client.query(`UPDATE ${relations.runs} SET updated_at=$2 WHERE id=$1`, [runId, receivedAt]);
    return mapEventRow(row);
  }

  async function readHandoffEvent(client, runId, identity) {
    const result = await client.query(`SELECT * FROM ${relations.events} WHERE run_id=$1 AND source_key=$2`, [runId, identity]);
    return result.rows[0] ? mapEventRow(result.rows[0]) : null;
  }

  async function blockRunForRevisionInTransaction(client, input) {
    const grant = revisionGrant(input);
    const runId = grant.claim.runId;
    const selected = await client.query(`SELECT * FROM ${relations.runs} WHERE id=$1 FOR UPDATE`, [runId]);
    const row = selected.rows[0];
    if (!row) throw claimLostError();
    const prior = await readHandoffEvent(client, runId, revisionGrantIdentity);
    if (prior) {
      const stored = assertRevisionGrant({ runId, status: row.status, claim: mapClaim(row),
        failure: row.failure, finalSummary: row.final_summary }, prior, grant.claim, grant.outcome);
      if (stableJson(stored) !== stableJson(grant)) throw conflict("Revision handoff already records a different blocked decision.");
      return readPostgresRun(client, runId, relations);
    }
    await assertPostgresExpectedClaim(client, relations, runId, grant.claim);
    assertRunTransition({ runId, status: row.status }, "blocked", input);
    const timestamp = now();
    await appendEventInTransaction(client, { ...domainEvent(runId, revisionGrantIdentity, grant, timestamp),
      expectedClaim: grant.claim }, revisionHandoffWrite);
    await client.query(`
      UPDATE ${relations.runs}
      SET status='blocked',failure=$2::jsonb,final_summary=$3::jsonb,
          completed_at=$4,updated_at=$4,claimed_by=NULL,claimed_at=NULL,
          heartbeat_at=NULL,lease_expires_at=NULL
      WHERE id=$1
    `, [runId, JSON.stringify(grant.failure), JSON.stringify(grant.finalSummary), timestamp]);
    return readPostgresRun(client, runId, relations);
  }

  async function createRevisionRunAndAppendEvent(input) {
    const request = normalizeRevisionRequest(input);
    return database.transaction(async client => {
      let previousEvent = null;
      // The existing creation owner still acquires family, domain resume-source,
      // then parent locks. Authorization is checked only inside that boundary.
      const created = await createRunInTransaction(client, request.run, "FOR UPDATE", async () => {
        const selected = await client.query(`SELECT * FROM ${relations.runs} WHERE id=$1 FOR UPDATE`, [request.parentRunId]);
        const row = selected.rows[0];
        if (!row) throw claimLostError();
        const grant = assertRevisionGrant({ runId: request.parentRunId, status: row.status, claim: mapClaim(row),
          failure: row.failure, finalSummary: row.final_summary },
        await readHandoffEvent(client, request.parentRunId, revisionGrantIdentity), request.expected, request.outcome);
        const consumed = assertRevisionConsumption(
          await readHandoffEvent(client, request.parentRunId, revisionConsumptionIdentity), grant, request.consumption);
        if (consumed) {
          previousEvent = await readHandoffEvent(client, request.parentRunId, request.event.sourceKey);
          const child = await client.query(`SELECT id FROM ${relations.runs} WHERE id=$1`, [request.run.runId]);
          if (!child.rows[0] || !previousEvent
            || previousEvent.payloadHash !== hashValue(request.event.payload)
            || eventIdentityHash(previousEvent) !== eventInputIdentityHash(request.event, previousEvent.payloadHash)) {
            throw conflict("Consumed revision is missing or conflicts with its child or event.");
          }
        }
      });
      if (previousEvent) return { run: created, event: previousEvent };
      const event = await appendEventInTransaction(client, request.event, revisionHandoffWrite);
      await appendEventInTransaction(client,
        domainEvent(request.parentRunId, revisionConsumptionIdentity, request.consumption, now()), revisionHandoffWrite);
      return { run: created, event };
    });
  }

  return {
    async assertExpectedClaim(input) {
      return database.transaction(client => assertPostgresExpectedClaim(client, relations, input?.runId, input?.expectedClaim));
    },

    blockRunForRevisionInTransaction,
    blockRunForRevision(input) {
      return database.transaction(client => blockRunForRevisionInTransaction(client, input));
    },
    createRevisionRunAndAppendEvent,

    async registerVersion(input) {
      const version = normalizeVersion(input, now());
      return database.transaction(async (client) => {
        await client.query(`
          INSERT INTO ${relations.versions} (
            id,workflow_type,predecessor_id,definition,definition_sha256,
            code_revision,change_reason,improvement_hypothesis,reviewed_by,created_at
          ) VALUES ($1,$2,$3,$4::jsonb,$5,$6,$7,$8,$9,$10)
          ON CONFLICT (id) DO NOTHING
        `, [
          version.id, version.workflowType, version.predecessorId, JSON.stringify(version.definition),
          version.definitionHash, version.codeRevision, version.changeReason,
          version.improvementHypothesis, version.reviewedBy, version.createdAt,
        ]);
        const result = await client.query(`
          SELECT id,workflow_type,predecessor_id,definition,definition_sha256,
                 code_revision,change_reason,improvement_hypothesis,reviewed_by,created_at
          FROM ${relations.versions} WHERE id=$1
        `, [version.id]);
        const stored = mapVersionRow(result.rows[0]);
        if (!stored || stableJson(versionIdentity(stored)) !== stableJson(versionIdentity(version))) {
          throw conflict(`Workflow version ${version.id} already has a different definition.`);
        }
        return stored;
      });
    },

    async inspectVersion(input) {
      const id = requireText(input?.workflowVersionId, 'Workflow version id');
      const result = await database.query(`
        SELECT id,workflow_type,predecessor_id,definition,definition_sha256,
               code_revision,change_reason,improvement_hypothesis,reviewed_by,created_at
        FROM ${relations.versions} WHERE id=$1
      `, [id]);
      if (!result.rows[0]) throw new TypeError('Unknown pinned workflow version.');
      return mapVersionRow(result.rows[0]);
    },

    async registerPriceVersion(input) {
      const price = normalizePriceVersion(input, now());
      return database.transaction(async (client) => {
        const expectedClaim = normalizeExpectedClaim(input?.expectedClaim);
        if (expectedClaim) {
          await assertPostgresExpectedClaim(client, relations, expectedClaim.runId, expectedClaim);
        }
        await client.query(`
          INSERT INTO ${relations.prices} (
            id,provider,model,service_tier,currency,input_per_million,
            cached_input_per_million,cache_write_input_per_million,
            output_per_million,reasoning_output_per_million,effective_from,
            effective_until,source_reference,reviewed_by,price_sha256,created_at
          ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
          ON CONFLICT (id) DO NOTHING
        `, [
          price.id, price.provider, price.model, price.serviceTier, price.currency,
          price.inputPerMillion, price.cachedInputPerMillion, price.cacheWriteInputPerMillion,
          price.outputPerMillion, price.reasoningOutputPerMillion, price.effectiveFrom,
          price.effectiveUntil, price.sourceReference, price.reviewedBy, price.priceHash,
          price.createdAt,
        ]);
        const result = await client.query(`SELECT * FROM ${relations.prices} WHERE id=$1`, [price.id]);
        const stored = mapPriceRow(result.rows[0]);
        if (!stored || stored.priceHash !== price.priceHash) {
          throw conflict(`Model price version ${price.id} already has different rates.`);
        }
        return stored;
      });
    },

    async createRunAndAppendEvent(input) {
      const { run, event } = requireCombinedRunEvent(input);
      return database.transaction(async client => {
        const created = await createRunInTransaction(client, run, "FOR UPDATE");
        const appended = await appendEventInTransaction(client, event);
        return { run: created, event: appended };
      });
    },

    async createRun(input) {
      return database.transaction(client => createRunInTransaction(client, input));
    },

    async findRunByIdempotencyKey(input) {
      const owner = normalizeOwner(input?.owner);
      const idempotencyKey = requireText(input?.idempotencyKey, "Workflow idempotency key");
      const runId = resolveRunId({ owner, idempotencyKey }, createId);
      const run = await readPostgresRun(database, runId, relations);
      if (!run) return null;
      if (stableJson(run.owner) !== stableJson(owner)) return null;
      return withMetrics(run);
    },

    async cancelQueuedRun(input, options) {
      const {runId, owner, authorize} = queuedCancellationInput(input, options);
      const values = postgresOwnerValues(owner);
      return database.transaction(async client => {
        const selected = await client.query(`
          SELECT id FROM ${relations.runs}
          WHERE id=$1 AND owner_kind=$2
            AND owner_principal_id IS NOT DISTINCT FROM $3
            AND owner_account_id IS NOT DISTINCT FROM $4
            AND owner_assistant_session_id IS NOT DISTINCT FROM $5
          FOR UPDATE
        `, [runId, owner.kind, values.principalId, values.accountId, values.assistantSessionId]);
        if (!selected.rows.length) throw notFoundRun();
        const run = await readPostgresRun(client, runId, relations);
        await authorizeQueuedCancellation(run, authorize);
        if (!assertQueuedCancellation(run)) return run;
        const timestamp = now();
        await client.query(`UPDATE ${relations.runs}
          SET status='cancelled',failure=$2::jsonb,updated_at=$3,completed_at=$3
          WHERE id=$1`, [runId, JSON.stringify({code: 'WORKFLOW_QUEUED_CANCELLED'}), timestamp]);
        return readPostgresRun(client, runId, relations);
      });
    },

    async requestRunCancellation(input, options) {
      const {runId, owner, authorize} = runningCancellationInput(input, options);
      const values = postgresOwnerValues(owner);
      return database.transaction(async client => {
        const selected = await client.query(`SELECT id FROM ${relations.runs}
          WHERE id=$1 AND owner_kind=$2 AND owner_principal_id IS NOT DISTINCT FROM $3
            AND owner_account_id IS NOT DISTINCT FROM $4 AND owner_assistant_session_id IS NOT DISTINCT FROM $5
          FOR UPDATE`, [runId, owner.kind, values.principalId, values.accountId, values.assistantSessionId]);
        if (!selected.rows.length) throw notFoundRun();
        let run = await readPostgresRun(client, runId, relations);
        const version = await client.query(`SELECT * FROM ${relations.versions} WHERE id=$1`, [run.workflowVersionId]);
        await authorizeRunningCancellation(run, authorize, mapVersionRow(version.rows[0]));
        if (assertCancellationRequest(run)) {
          const timestamp = now();
          await appendEventInTransaction(client, domainEvent(runId, cancellationRequestIdentity,
            cancellationIntent(run, timestamp), timestamp), cancellationWrite);
          run = await readPostgresRun(client, runId, relations);
        }
        return {run, cancellation: cancellationState(run)};
      });
    },

    async inspectRunCancellation(input) {
      const expected = cancellationClaimInput(input);
      return database.transaction(async client => {
        await assertPostgresExpectedClaim(client, relations, input.runId, expected);
        const result = await client.query(`SELECT status FROM ${relations.runs} WHERE id=$1`, [input.runId]);
        const events = await readCancellationEvents(client, input.runId, relations);
        return cancellationState({runId: input.runId, status: result.rows[0].status, events});
      });
    },

    async confirmRunCancellation(input) {
      const expected = cancellationConfirmationInput(input);
      return database.transaction(async client => {
        const selected = await client.query(`SELECT id FROM ${relations.runs} WHERE id=$1 FOR UPDATE`, [input.runId]);
        if (!selected.rows.length) throw notFoundRun();
        const run = await readPostgresRun(client, input.runId, relations);
        if (assertCancellationConfirmation(run, expected)) return run;
        await assertPostgresExpectedClaim(client, relations, input.runId, expected);
        const timestamp = now();
        await appendEventInTransaction(client, domainEvent(run.runId, cancellationConfirmationIdentity,
          cancellationConfirmation(run, timestamp), timestamp), cancellationWrite);
        await client.query(`UPDATE ${relations.runs} SET status='cancelled',failure=$2::jsonb,final_summary=NULL,
          updated_at=$3,completed_at=$3,claimed_by=NULL,claimed_at=NULL,heartbeat_at=NULL,lease_expires_at=NULL WHERE id=$1`,
        [run.runId, JSON.stringify({code: 'WORKFLOW_RUNNING_CANCELLED'}), timestamp]);
        return readPostgresRun(client, input.runId, relations);
      });
    },

    async claimRun(input) {
      const workerId = requireText(input?.workerId, "Worker id");
      const workflowType = optionalText(input?.workflowType);
      const leaseSeconds = boundedInteger(input?.leaseSeconds ?? 120, "Run lease seconds", 30, 3600);
      return database.transaction(async (client) => {
        const selected = await client.query(`
          SELECT id,status
          FROM ${relations.runs}
          WHERE (status='queued' OR (status IN ('running','ready-for-admission') AND lease_expires_at <= clock_timestamp()))
            AND NOT EXISTS (SELECT 1 FROM ${relations.events} e WHERE e.run_id=${relations.runs}.id AND e.source_key=$2)
            AND ($1::text IS NULL OR workflow_type=$1)
          ORDER BY queue_priority DESC,created_at,id
          FOR UPDATE SKIP LOCKED
          LIMIT 1
        `, [workflowType, cancellationRequestIdentity]);
        if (!selected.rows[0]) return null;
        // Claim epochs round to milliseconds because the public journal maps
        // PostgreSQL timestamps through JavaScript Date before exact replay.
        const timestamp = asIso((await client.query(
          "SELECT date_trunc('milliseconds', clock_timestamp()) AS claimed_at",
        )).rows[0].claimed_at);
        await client.query(`
          UPDATE ${relations.runs}
          SET status=CASE WHEN status='queued' THEN 'running' ELSE status END,
              claimed_by=$2,claimed_at=$1,heartbeat_at=$1,
              lease_expires_at=$3,started_at=coalesce(started_at,$1),updated_at=$1,
              recovery_count=recovery_count + CASE WHEN status IN ('running','ready-for-admission') THEN 1 ELSE 0 END
          WHERE id=$4
        `, [timestamp, workerId, addSeconds(timestamp, leaseSeconds), selected.rows[0].id]);
        return readPostgresRun(client, selected.rows[0].id, relations);
      });
    },

    async heartbeatRun(input) {
      const runId = requireText(input?.runId, "Workflow run id");
      const workerId = requireText(input?.workerId, "Worker id");
      const claimedAt = input?.claimedAt == null ? null : isoTimestamp(input.claimedAt, "Workflow claim epoch");
      const leaseSeconds = boundedInteger(input?.leaseSeconds ?? 120, "Run lease seconds", 30, 3600);
      return database.transaction(async (client) => {
        const locked = await client.query(`
          SELECT status,claimed_by,claimed_at,lease_expires_at
          FROM ${relations.runs} WHERE id=$1 FOR UPDATE
        `, [runId]);
        const clock = await client.query(`
          WITH checked_clock AS MATERIALIZED (SELECT clock_timestamp() AS checked_at)
          SELECT date_trunc('milliseconds',checked_clock.checked_at) AS issued_at,
                 ${relations.runs}.lease_expires_at > checked_clock.checked_at AS lease_active
          FROM checked_clock CROSS JOIN ${relations.runs} WHERE ${relations.runs}.id=$1
        `, [runId]);
        const row = locked.rows[0];
        const issuedAt = clock.rows[0]?.issued_at == null ? null : asIso(clock.rows[0].issued_at);
        if (!row || !issuedAt || !["running", "ready-for-admission"].includes(row.status)
          || row.claimed_by !== workerId
          || (claimedAt && asIso(row.claimed_at) !== claimedAt)
          || clock.rows[0]?.lease_active !== true) {
          if (claimedAt) throw claimLostError();
          throw conflict(`Workflow run ${runId} is not leased by ${workerId}.`);
        }
        const updated = await client.query(`
          UPDATE ${relations.runs}
          SET heartbeat_at=$1,lease_expires_at=$2,updated_at=$1
          WHERE id=$3 AND lease_expires_at > clock_timestamp()
          RETURNING claimed_by,claimed_at,heartbeat_at,lease_expires_at
        `, [issuedAt, addSeconds(issuedAt, leaseSeconds), runId]);
        if (!updated.rows[0]) {
          if (claimedAt) throw claimLostError();
          throw conflict(`Workflow run ${runId} is not leased by ${workerId}.`);
        }
        return mapClaim(updated.rows[0]);
      });
    },

    async createAssignment(input) {
      const runId = requireText(input?.runId, "Workflow run id");
      const assignmentId = input?.assignmentId == null ? createId() : requireText(input.assignmentId, "Assignment id");
      const timestamp = now();
      const assignment = normalizeAssignment(input, { assignmentId, timestamp });
      const identityHash = hashValue({
        runId, parentAssignmentId: assignment.parentAssignmentId, stepKey: assignment.stepKey,
        assignedTo: assignment.assignedTo, execution: assignment.execution, targets: assignment.targets,
      });
      return database.transaction(async (client) => {
        if (assignment.parentAssignmentId) {
          const parent = await client.query(
            `SELECT run_id FROM ${relations.assignments} WHERE id=$1 FOR UPDATE`,
            [assignment.parentAssignmentId],
          );
          if (!parent.rows[0] || parent.rows[0].run_id !== runId) {
            throw new TypeError("Parent assignment must belong to the same workflow run.");
          }
        }
        const runResult = await client.query(
          `SELECT status,workflow_type FROM ${relations.runs} WHERE id=$1 FOR UPDATE`,
          [runId],
        );
        if (!runResult.rows[0]) throw notFoundRun();
        await assertPostgresExpectedClaim(client, relations, runId, input?.expectedClaim);
        assertRunAcceptsAssignment({
          runId,
          status: runResult.rows[0].status,
          workflowType: runResult.rows[0].workflow_type,
        }, assignment);
        const existing = await client.query(
          `SELECT identity_sha256 FROM ${relations.assignments} WHERE id=$1`,
          [assignmentId],
        );
        if (existing.rows[0]) {
          if (existing.rows[0].identity_sha256 !== identityHash) {
            throw conflict(`Assignment ${assignmentId} already exists with different input.`);
          }
          return readPostgresAssignment(client, assignmentId, relations);
        }
        for (const target of [...assignment.targets].sort((left, right) => {
          const leftKey = `${left.type}:${left.id}`;
          const rightKey = `${right.type}:${right.id}`;
          return leftKey < rightKey ? -1 : leftKey > rightKey ? 1 : 0;
        })) {
          await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))", [`${target.type}:${target.id}`]);
        }
        const conflicts = await client.query(`
          SELECT t.target_type,t.target_id,t.assignment_id
          FROM ${relations.assignmentTargets} t
          JOIN ${relations.assignments} a ON a.id=t.assignment_id
          WHERE a.status IN ('dispatching','running','unknown')
            AND (t.target_type,t.target_id) IN (
              SELECT value->>'type',value->>'id' FROM jsonb_array_elements($1::jsonb) value
            )
          LIMIT 1
        `, [JSON.stringify(assignment.targets)]);
        if (conflicts.rows[0]) {
          const row = conflicts.rows[0];
          throw conflict(`Target ${row.target_type}:${row.target_id} is already assigned by ${row.assignment_id}.`);
        }
        const ordinal = await client.query(`
          SELECT coalesce(max(assignment_ordinal),0)+1 AS ordinal
          FROM ${relations.assignments} WHERE run_id=$1
        `, [runId]);
        await client.query(`
          INSERT INTO ${relations.assignments} (
            id,run_id,parent_assignment_id,assignment_ordinal,marker,step_key,
            assigned_to,execution,status,agent_thread_id,identity_sha256,
            created_at,started_at,updated_at
          ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$12)
        `, [
          assignmentId, runId, assignment.parentAssignmentId, Number(ordinal.rows[0].ordinal),
          assignment.marker, assignment.stepKey, assignment.assignedTo, assignment.execution,
          assignment.status, assignment.agentThreadId, identityHash, timestamp, assignment.startedAt,
        ]);
        for (const target of assignment.targets) {
          await client.query(`
            INSERT INTO ${relations.assignmentTargets} (
              run_id,assignment_id,target_ordinal,target_type,target_id,expected_version
            ) VALUES ($1,$2,$3,$4,$5,$6)
          `, [runId, assignmentId, target.ordinal, target.type, target.id, target.expectedVersion]);
        }
        await client.query(`UPDATE ${relations.runs} SET updated_at=$2 WHERE id=$1`, [runId, timestamp]);
        return readPostgresAssignment(client, assignmentId, relations);
      });
    },

    async createPreparedAssignment(input) {
      const runId = requireText(input?.runId, "Workflow run id");
      const assignmentId = requireText(input?.assignmentId, "Assignment id");
      const prepared = normalizePreparedAssignment(input);
      const timestamp = now();
      const assignment = normalizeAssignment(input, { assignmentId, timestamp });
      const identityHash = assignmentIdentityHash(runId, assignment);
      const event = preparedAssignmentEvent({ runId, assignmentId, prepared, timestamp });
      return database.transaction(async (client) => {
        // Every writer of this lineage takes assignment, then attempt, then run.
        const parentResult = await client.query(
          `SELECT id,run_id,status FROM ${relations.assignments} WHERE id=$1 FOR UPDATE`,
          [prepared.executionRef.assignmentId],
        );
        const parent = parentResult.rows[0];
        if (!parent || parent.run_id !== runId || assignment.parentAssignmentId !== parent.id) {
          throw conflict("Prepared assignment caller does not belong to its parent run.");
        }
        const attemptResult = await client.query(
          `SELECT id,assignment_id,run_id,thread_id,outcome FROM ${relations.attempts} WHERE id=$1 FOR UPDATE`,
          [prepared.executionRef.attemptId],
        );
        const attempt = attemptResult.rows[0];
        if (!attempt || attempt.assignment_id !== parent.id || attempt.run_id !== runId) {
          throw conflict("Prepared assignment caller attempt does not belong to its parent assignment.");
        }
        const runResult = await client.query(
          `SELECT status,workflow_type FROM ${relations.runs} WHERE id=$1 FOR UPDATE`,
          [runId],
        );
        if (!runResult.rows[0]) throw notFoundRun();
        await assertPostgresExpectedClaim(client, relations, runId, input?.expectedClaim);
        const existing = await client.query(
          `SELECT identity_sha256 FROM ${relations.assignments} WHERE id=$1`,
          [assignmentId],
        );
        if (existing.rows[0]) {
          const storedEvent = await client.query(
            `SELECT * FROM ${relations.events} WHERE run_id=$1 AND source_key=$2`,
            [runId, event.sourceKey],
          );
          const mappedEvent = storedEvent.rows[0] ? mapEventRow(storedEvent.rows[0]) : null;
          assertPreparedReplay(assignmentId, mappedEvent, prepared);
          return preparedResult(await readPostgresAssignment(client, assignmentId, relations), mappedEvent.payload, false);
        }
        assertRunAcceptsAssignment({
          runId, status: runResult.rows[0].status, workflowType: runResult.rows[0].workflow_type,
        }, assignment);
        if (parent.status !== "running" || attempt.outcome !== null
          || attempt.thread_id !== prepared.executionRef.threadId
          || (assignment.execution === "current-agent" && assignment.agentThreadId !== attempt.thread_id)) {
          throw conflict("Prepared assignment caller attempt is not active on its bound thread.");
        }
        for (const target of [...assignment.targets].sort((left, right) => {
          const a = `${left.type}:${left.id}`;
          const b = `${right.type}:${right.id}`;
          return a < b ? -1 : a > b ? 1 : 0;
        })) {
          await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))", [`${target.type}:${target.id}`]);
        }
        const conflicts = await client.query(`
          SELECT t.target_type,t.target_id,t.assignment_id
          FROM ${relations.assignmentTargets} t
          JOIN ${relations.assignments} a ON a.id=t.assignment_id
          WHERE a.status IN ('dispatching','running','unknown')
            AND (t.target_type,t.target_id) IN (
              SELECT value->>'type',value->>'id' FROM jsonb_array_elements($1::jsonb) value
            )
          LIMIT 1
        `, [JSON.stringify(assignment.targets)]);
        if (conflicts.rows[0]) {
          const row = conflicts.rows[0];
          throw conflict(`Target ${row.target_type}:${row.target_id} is already assigned by ${row.assignment_id}.`);
        }
        const ordinal = await client.query(`
          SELECT coalesce(max(assignment_ordinal),0)+1 AS ordinal
          FROM ${relations.assignments} WHERE run_id=$1
        `, [runId]);
        await client.query(`
          INSERT INTO ${relations.assignments} (
            id,run_id,parent_assignment_id,assignment_ordinal,marker,step_key,
            assigned_to,execution,status,agent_thread_id,identity_sha256,
            created_at,started_at,updated_at
          ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$12)
        `, [
          assignmentId, runId, assignment.parentAssignmentId, Number(ordinal.rows[0].ordinal),
          assignment.marker, assignment.stepKey, assignment.assignedTo, assignment.execution,
          assignment.status, assignment.agentThreadId, identityHash, timestamp, assignment.startedAt,
        ]);
        for (const target of assignment.targets) {
          await client.query(`
            INSERT INTO ${relations.assignmentTargets} (
              run_id,assignment_id,target_ordinal,target_type,target_id,expected_version
            ) VALUES ($1,$2,$3,$4,$5,$6)
          `, [runId, assignmentId, target.ordinal, target.type, target.id, target.expectedVersion]);
        }
        await client.query(`
          INSERT INTO ${relations.events}
            (run_id,assignment_id,attempt_id,source_key,event_type,thread_id,received_at,payload,payload_sha256)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9)
        `, [runId, event.assignmentId, event.attemptId, event.sourceKey, event.eventType, event.threadId,
          timestamp, JSON.stringify(event.payload), event.payloadHash]);
        await client.query(`UPDATE ${relations.runs} SET updated_at=$2 WHERE id=$1`, [runId, timestamp]);
        return preparedResult(await readPostgresAssignment(client, assignmentId, relations), event.payload, true);
      });
    },

    async startAttempt(input) {
      const assignmentId = requireText(input?.assignmentId, "Assignment id");
      const attemptId = input?.attemptId == null ? createId() : requireText(input.attemptId, "Attempt id");
      const timestamp = now();
      const prompt = assertSafeAuditPrompt(input?.prompt, { label: "Attempt prompt" });
      const requested = normalizeModelSettings(input?.requested, { requireModel: true });
      const capabilityProfile = cloneJson(input?.capabilityProfile ?? { allowedTools: [] });
      return database.transaction(async (client) => {
        let initialRunId = null;
        let familyRootId = null;
        let useFamilyLock = Boolean(familyRelations);
        if (!useFamilyLock && hasCleanJournalRelations) {
          const helper = await client.query(
            "SELECT pg_catalog.to_regprocedure($1) IS NOT NULL AS available",
            [`${runSchema}.lock_agent_workflow_run_family(text)`],
          );
          useFamilyLock = helper.rows[0]?.available === true;
        }
        if (useFamilyLock) {
          const lineage = await client.query(
            `SELECT run_id FROM ${relations.assignments} WHERE id=$1`, [assignmentId],
          );
          initialRunId = lineage.rows[0]?.run_id ?? null;
          if (!initialRunId) throw new TypeError(`Unknown workflow assignment: ${assignmentId}.`);
          const lockedFamily = await client.query(
            `SELECT ${runSchema}.lock_agent_workflow_run_family($1) AS root_run_id`,
            [initialRunId],
          );
          familyRootId = lockedFamily.rows[0]?.root_run_id ?? null;
        }
        const familyVersion = familyRootId && familyRelations
          ? Number((await client.query(`SELECT budget_version FROM ${familyRelations.families} WHERE root_run_id=$1`, [familyRootId])).rows[0]?.budget_version)
          : null;
        const resourceReservation = normalizeResourceReservation(input?.resourceReservation, familyVersion === 3);
        const assignmentResult = await client.query(`
          SELECT id,run_id,status FROM ${relations.assignments} WHERE id=$1 FOR UPDATE
        `, [assignmentId]);
        const assignment = assignmentResult.rows[0];
        if (!assignment) throw new TypeError(`Unknown workflow assignment: ${assignmentId}.`);
        if (useFamilyLock && assignment.run_id !== initialRunId) {
          throw conflict(`Assignment ${assignmentId} changed workflow run during attempt creation.`);
        }
        if (familyRelations && familyRootId) {
          const lineage = await client.query(`
            SELECT root_run_id FROM ${familyRelations.families} WHERE root_run_id=$1
            UNION ALL
            SELECT root_run_id FROM ${familyRelations.members} WHERE run_id=$1
          `, [assignment.run_id]);
          if (lineage.rows.length !== 1 || lineage.rows[0].root_run_id !== familyRootId) {
            throw conflict(`Assignment ${assignmentId} changed run family during attempt creation.`);
          }
        }
        await assertPostgresExpectedClaim(client, relations, assignment.run_id, input?.expectedClaim);
        if (!openAssignmentStatuses.has(assignment.status)) throw conflict(`Assignment ${assignmentId} is terminal.`);
        const active = await client.query(
          `SELECT id FROM ${relations.attempts} WHERE assignment_id=$1 AND outcome IS NULL`,
          [assignmentId],
        );
        if (active.rows[0]) throw conflict(`Assignment ${assignmentId} already has an active attempt.`);
        const existing = await client.query(`SELECT id FROM ${relations.attempts} WHERE id=$1`, [attemptId]);
        if (existing.rows[0]) throw conflict(`Attempt ${attemptId} already exists.`);
        const ordinal = await client.query(`
          SELECT coalesce(max(attempt_ordinal),0)+1 AS ordinal
          FROM ${relations.attempts} WHERE assignment_id=$1
        `, [assignmentId]);
        await client.query(`
          INSERT INTO ${relations.attempts} (
            id,run_id,assignment_id,attempt_ordinal,prompt,prompt_sha256,
            requested_model,requested_reasoning_effort,requested_service_tier,
            capability_profile,capability_sha256,thread_id,turn_id,started_at${familyRelations ? ",resource_reserved_total_tokens" : ""}
          ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11,$12,$13,$14${familyRelations ? ",$15" : ""})
        `, [
          attemptId, assignment.run_id, assignmentId, Number(ordinal.rows[0].ordinal),
          prompt, hashValue(prompt), requested.model, requested.reasoningEffort,
          requested.serviceTier, JSON.stringify(capabilityProfile), hashValue(capabilityProfile),
          optionalText(input?.threadId), optionalText(input?.turnId), timestamp,
          ...(familyRelations ? [resourceReservation?.totalTokens ?? null] : []),
        ]);
        await client.query(`
          UPDATE ${relations.assignments}
          SET status='running',started_at=coalesce(started_at,$2),updated_at=$2 WHERE id=$1
        `, [assignmentId, timestamp]);
        await client.query(`UPDATE ${relations.runs} SET updated_at=$2 WHERE id=$1`, [assignment.run_id, timestamp]);
        const result = await client.query(`SELECT * FROM ${relations.attempts} WHERE id=$1`, [attemptId]);
        return mapAttemptRow(result.rows[0]);
      });
    },

    async bindAttempt(input) {
      const attemptId = requireText(input?.attemptId, "Attempt id");
      const threadId = optionalText(input?.threadId);
      const turnId = optionalText(input?.turnId);
      if (!threadId && !turnId) throw new TypeError("Attempt binding requires a thread id or turn id.");
      const timestamp = now();
      return database.transaction(async (client) => {
        const identity = await client.query(
          `SELECT assignment_id FROM ${relations.attempts} WHERE id=$1`,
          [attemptId],
        );
        if (!identity.rows[0]) throw new TypeError(`Unknown workflow attempt: ${attemptId}.`);
        const assignmentResult = await client.query(`
          SELECT id,run_id,agent_thread_id
          FROM ${relations.assignments}
          WHERE id=$1 FOR UPDATE
        `, [identity.rows[0].assignment_id]);
        const assignment = assignmentResult.rows[0];
        if (!assignment) throw new TypeError(`Unknown workflow attempt: ${attemptId}.`);
        const selected = await client.query(`
          SELECT * FROM ${relations.attempts}
          WHERE id=$1 AND assignment_id=$2 FOR UPDATE
        `, [attemptId, assignment.id]);
        const attempt = selected.rows[0];
        if (!attempt) throw new TypeError(`Unknown workflow attempt: ${attemptId}.`);
        await assertPostgresExpectedClaim(client, relations, attempt.run_id, input?.expectedClaim);
        if (attempt.thread_id && threadId && attempt.thread_id !== threadId) throw conflict(`Attempt ${attemptId} is already bound to another thread.`);
        if (attempt.turn_id && turnId && attempt.turn_id !== turnId) throw conflict(`Attempt ${attemptId} is already bound to another turn.`);
        if (assignment.agent_thread_id && threadId && assignment.agent_thread_id !== threadId) {
          throw conflict(`Assignment ${attempt.assignment_id} is already bound to another thread.`);
        }
        await client.query(`
          UPDATE ${relations.attempts}
          SET thread_id=coalesce(thread_id,$2),turn_id=coalesce(turn_id,$3)
          WHERE id=$1
        `, [attemptId, threadId, turnId]);
        await client.query(`
          UPDATE ${relations.assignments}
          SET agent_thread_id=coalesce(agent_thread_id,$2),updated_at=$3 WHERE id=$1
        `, [attempt.assignment_id, threadId, timestamp]);
        await client.query(`UPDATE ${relations.runs} SET updated_at=$2 WHERE id=$1`, [attempt.run_id, timestamp]);
        const result = await client.query(`SELECT * FROM ${relations.attempts} WHERE id=$1`, [attemptId]);
        return mapAttemptRow(result.rows[0]);
      });
    },

    async observeAssignment(input) {
      const assignmentId = requireText(input?.assignmentId, "Assignment id");
      const observationId = boundedText(input?.observationId, "Assignment observation id", 500);
      const status = enumValue(input?.status, "Assignment observation status", new Set([...openAssignmentStatuses, ...terminalAssignmentStatuses]));
      const threadId = requireText(input?.threadId, "Observed agent thread id");
      const message = optionalText(input?.message);
      const sourceKey = `assignment-observation:${observationId}`;
      const payload = { observationId, status, threadId, message };
      const payloadHash = hashValue(payload);
      const timestamp = now();
      return database.transaction(async (client) => {
        const selected = await client.query(`SELECT * FROM ${relations.assignments} WHERE id=$1 FOR UPDATE`, [assignmentId]);
        const assignment = selected.rows[0];
        if (!assignment) throw new TypeError(`Unknown workflow assignment: ${assignmentId}.`);
        await assertPostgresExpectedClaim(client, relations, assignment.run_id, input?.expectedClaim);
        const prior = await client.query(
          `SELECT payload_sha256 FROM ${relations.events} WHERE run_id=$1 AND source_key=$2`,
          [assignment.run_id, sourceKey],
        );
        if (prior.rows[0]) {
          if (prior.rows[0].payload_sha256 !== payloadHash) throw conflict(`Workflow event ${sourceKey} already has a different payload.`);
          return readPostgresAssignment(client, assignmentId, relations);
        }
        if (terminalAssignmentStatuses.has(assignment.status) && openAssignmentStatuses.has(status)) {
          throw conflict(`Terminal assignment ${assignmentId} cannot return to ${status}.`);
        }
        if (assignment.agent_thread_id && assignment.agent_thread_id !== threadId) {
          throw conflict(`Assignment ${assignmentId} is already bound to another thread.`);
        }
        if (terminalAssignmentStatuses.has(status)) {
          const active = await client.query(
            `SELECT id FROM ${relations.attempts} WHERE assignment_id=$1 AND outcome IS NULL`,
            [assignmentId],
          );
          if (active.rows[0]) throw conflict(`Assignment ${assignmentId} still has an active attempt.`);
        }
        await client.query(`
          INSERT INTO ${relations.events}
            (run_id,assignment_id,source_key,event_type,thread_id,occurred_at,received_at,payload,payload_sha256)
          VALUES ($1,$2,$3,'workflow.assignment-observed',$4,$5,$6,$7::jsonb,$8)
        `, [assignment.run_id, assignmentId, sourceKey, threadId,
          input?.occurredAt == null ? null : isoTimestamp(input.occurredAt, "Assignment observation time"),
          timestamp, JSON.stringify(payload), payloadHash]);
        await client.query(`
          UPDATE ${relations.assignments}
          SET status=$2,agent_thread_id=$3,
              started_at=CASE WHEN $2='running' THEN coalesce(started_at,$4) ELSE started_at END,
              completed_at=CASE WHEN $2='completed' THEN coalesce(completed_at,$4) ELSE completed_at END,
              finished_at=CASE WHEN $2 IN ('completed','failed','interrupted') THEN coalesce(finished_at,$4) ELSE finished_at END,
              failure=CASE WHEN $2='failed' THEN coalesce($5,'Agent execution failed.') ELSE failure END,
              updated_at=$4
          WHERE id=$1
        `, [assignmentId, status, threadId, timestamp, message]);
        await client.query(`UPDATE ${relations.runs} SET updated_at=$2 WHERE id=$1`, [assignment.run_id, timestamp]);
        return readPostgresAssignment(client, assignmentId, relations);
      });
    },

    appendEventInTransaction,

    async appendEvent(input) {
      return database.transaction(client => appendEventInTransaction(client, input));
    },

    async finishAttempt(input) {
      const attemptId = requireText(input?.attemptId, "Attempt id");
      const outcome = enumValue(input?.outcome, "Attempt outcome", terminalAttemptOutcomes);
      const resultPayload = cloneJson(input?.result ?? {});
      const finish = {
        outcome,
        observed: normalizeModelSettings(input?.observed, { requireModel: false }),
        usage: null,
        telemetryStatus: enumValue(input?.telemetryStatus, "Attempt telemetry status", new Set(["complete", "incomplete", "inconsistent"])),
        result: resultPayload,
        error: optionalText(input?.error),
        price: input?.price == null ? null : normalizeAttemptPrice(input.price),
        apiEquivalentCost: input?.apiEquivalentCost == null ? null : normalizeMoney(input.apiEquivalentCost, "API-equivalent cost"),
        billedCost: input?.billedCost == null ? null : normalizeMoney(input.billedCost, "Billed cost"),
      };
      const timestamp = now();
      return database.transaction(async (client) => {
        let initialRunId = null;
        let familyRootId = null;
        if (familyRelations) {
          const lineage = await client.query(`SELECT run_id FROM ${relations.attempts} WHERE id=$1`, [attemptId]);
          initialRunId = lineage.rows[0]?.run_id ?? null;
          if (!initialRunId) throw new TypeError(`Unknown workflow attempt: ${attemptId}.`);
          const family = await client.query(`
            SELECT budget_version,root_run_id FROM ${familyRelations.families} WHERE root_run_id=$1
            UNION ALL
            SELECT declaration.budget_version,declaration.root_run_id
            FROM ${familyRelations.members} member
            JOIN ${familyRelations.families} declaration ON declaration.root_run_id=member.root_run_id
            WHERE member.run_id=$1
          `, [initialRunId]);
          if (Number(family.rows[0]?.budget_version) === 3) {
            const locked = await client.query(`SELECT ${runSchema}.lock_agent_workflow_run_family($1) AS root_run_id`, [initialRunId]);
            familyRootId = locked.rows[0]?.root_run_id ?? null;
            if (familyRootId !== family.rows[0].root_run_id) {
              throw conflict(`Attempt ${attemptId} changed run family during completion.`);
            }
          } else {
            familyRootId = family.rows[0]?.root_run_id ?? null;
          }
        }
        const selected = await client.query(
          `SELECT * FROM ${relations.attempts} WHERE id=$1 FOR UPDATE`,
          [attemptId],
        );
        const attempt = selected.rows[0];
        if (!attempt) throw new TypeError(`Unknown workflow attempt: ${attemptId}.`);
        if (initialRunId && attempt.run_id !== initialRunId) throw conflict(`Attempt ${attemptId} changed workflow run during completion.`);
        const familyVersion = familyRootId
          ? Number((await client.query(`SELECT budget_version FROM ${familyRelations.families} WHERE root_run_id=$1`, [familyRootId])).rows[0]?.budget_version)
          : null;
        finish.usage = input?.usage == null ? null
          : familyVersion === 3 && input?.resourceUsageBasis === "unknown"
            ? normalizeUnknownUsage(input.usage) : normalizeUsage(input.usage);
        const resourceUsageBasis = normalizeResourceUsageBasis(input?.resourceUsageBasis, familyVersion === 3, finish, input?.usage);
        if (familyVersion === 3) finish.resourceUsageBasis = resourceUsageBasis;
        const finishHash = hashValue(finish);
        await assertPostgresExpectedClaim(client, relations, attempt.run_id, input?.expectedClaim);
        if (attempt.outcome) {
          if (attempt.finish_sha256 !== finishHash) {
            throw conflict(`Attempt ${attemptId} is already complete with a different result.`);
          }
          return mapAttemptRow(attempt);
        }
        const usage = finish.usage;
        const observed = finish.observed;
        const updated = await client.query(`
          UPDATE ${relations.attempts}
          SET outcome=$2,observed_provider=$3,observed_model=$4,
              observed_reasoning_effort=$5,observed_service_tier=$6,
              input_tokens=$7,cached_input_tokens=$8,cache_write_input_tokens=$9,
              output_tokens=$10,reasoning_output_tokens=$11,total_tokens=$12,
              telemetry_status=$13,result_payload=$14::jsonb,result_sha256=$15,
              error=$16,price_version_id=$17,api_equivalent_cost=$18,
              api_equivalent_cost_currency=$19,billed_cost=$20,billed_cost_currency=$21,
              completed_at=$22,finish_sha256=$23${familyRelations ? ",resource_usage_basis=$24" : ""}
          WHERE id=$1 RETURNING *
        `, [
          attemptId, outcome, observed?.provider, observed?.model, observed?.reasoningEffort,
          observed?.serviceTier, usage?.inputTokens, usage?.cachedInputTokens,
          usage?.cacheWriteInputTokens, usage?.outputTokens, usage?.reasoningOutputTokens,
          usage?.totalTokens, finish.telemetryStatus, JSON.stringify(resultPayload),
          hashValue(resultPayload), finish.error, finish.price?.versionId,
          finish.apiEquivalentCost?.amount, finish.apiEquivalentCost?.currency,
          finish.billedCost?.amount, finish.billedCost?.currency, timestamp, finishHash,
          ...(familyRelations ? [resourceUsageBasis] : []),
        ]);
        await observeAttemptOutput(client, attempt, resultPayload, timestamp);
        await client.query(`UPDATE ${relations.runs} SET updated_at=$2 WHERE id=$1`, [attempt.run_id, timestamp]);
        return mapAttemptRow(updated.rows[0]);
      });
    },

    async finishAssignment(input) {
      const assignmentId = requireText(input?.assignmentId, "Assignment id");
      const status = enumValue(input?.status, "Assignment status", terminalAssignmentStatuses);
      const failure = optionalText(input?.failure);
      const finishHash = hashValue({ status, failure });
      const timestamp = now();
      return database.transaction(async (client) => {
        const selected = await client.query(
          `SELECT * FROM ${relations.assignments} WHERE id=$1 FOR UPDATE`,
          [assignmentId],
        );
        const assignment = selected.rows[0];
        if (!assignment) throw new TypeError(`Unknown workflow assignment: ${assignmentId}.`);
        await assertPostgresExpectedClaim(client, relations, assignment.run_id, input?.expectedClaim);
        const active = await client.query(
          `SELECT id FROM ${relations.attempts} WHERE assignment_id=$1 AND outcome IS NULL`,
          [assignmentId],
        );
        if (active.rows[0]) throw conflict(`Assignment ${assignmentId} still has an active attempt.`);
        if (terminalAssignmentStatuses.has(assignment.status)) {
          if (assignment.finish_sha256 !== finishHash) {
            throw conflict(`Assignment ${assignmentId} is already complete with a different result.`);
          }
          return readPostgresAssignment(client, assignmentId, relations);
        }
        await client.query(`
          UPDATE ${relations.assignments}
          SET status=$2,failure=$3,completed_at=CASE WHEN $2='completed' THEN $4::timestamptz ELSE NULL END,
              finished_at=$4,updated_at=$4,finish_sha256=$5
          WHERE id=$1
        `, [assignmentId, status, failure, timestamp, finishHash]);
        await client.query(`UPDATE ${relations.runs} SET updated_at=$2 WHERE id=$1`, [assignment.run_id, timestamp]);
        return readPostgresAssignment(client, assignmentId, relations);
      });
    },

    async transitionRun(input) {
      const runId = requireText(input?.runId, "Workflow run id");
      const status = enumValue(input?.status, "Workflow run status", runStatuses);
      const timestamp = now();
      return database.transaction(async (client) => {
        const selected = await client.query(`SELECT * FROM ${relations.runs} WHERE id=$1 FOR UPDATE`, [runId]);
        const row = selected.rows[0];
        if (!row) throw notFoundRun();
        await assertPostgresExpectedClaim(client, relations, runId, input?.expectedClaim);
        if (row.status === status) return readPostgresRun(client, runId, relations);
        assertCancellationTransition(await readCancellationEvents(client, runId, relations), status);
        const shape = {
          runId,
          workflowType: row.workflow_type,
          status: row.status,
          assignments: (await client.query(
            `SELECT status FROM ${relations.assignments} WHERE run_id=$1`,
            [runId],
          )).rows,
        };
        assertRunTransition(shape, status, input);
        if (status === "completed") {
          const run = await readPostgresRun(client, runId, relations);
          const versionResult = await client.query(`
            SELECT workflow_type,definition,definition_sha256
            FROM ${relations.versions} WHERE id=$1
          `, [run.workflowVersionId]);
          const version = versionResult.rows[0] ? {
            workflowType: versionResult.rows[0].workflow_type,
            definition: clone(versionResult.rows[0].definition),
            definitionHash: versionResult.rows[0].definition_sha256,
          } : null;
          await assertCompletion(run, version, input?.finalSummary);
        }
        const terminal = ["admitted", "completed", "failed", "cancelled", "blocked"].includes(status);
        const manifestHash = status === "ready-for-admission"
          ? requireHash(input.manifestHash, "Validated manifest hash")
          : null;
        await client.query(`
          UPDATE ${relations.runs}
          SET status=$2,updated_at=$3,
              validated_manifest_sha256=CASE WHEN $2='ready-for-admission' THEN $4 ELSE validated_manifest_sha256 END,
              last_validation_passed_at=CASE WHEN $2='ready-for-admission' THEN $3 ELSE last_validation_passed_at END,
              admitted_manifest_sha256=CASE WHEN $2='admitted' THEN $8 ELSE admitted_manifest_sha256 END,
              failure=CASE WHEN $5 THEN $6::jsonb ELSE failure END,
              final_summary=CASE WHEN $5 OR $2='completed' THEN $7::jsonb ELSE final_summary END,
              completed_at=CASE WHEN $5 OR $2='completed' THEN $3 ELSE completed_at END,
              claimed_by=CASE WHEN $5 THEN NULL ELSE claimed_by END,
              claimed_at=CASE WHEN $5 THEN NULL ELSE claimed_at END,
              heartbeat_at=CASE WHEN $5 THEN NULL ELSE heartbeat_at END,
              lease_expires_at=CASE WHEN $5 THEN NULL ELSE lease_expires_at END
          WHERE id=$1
        `, [
          runId, status, timestamp, manifestHash, terminal,
          input.failure == null ? null : JSON.stringify(cloneJson(input.failure)),
          input.finalSummary == null ? null : JSON.stringify(cloneJson(input.finalSummary)),
          status === "admitted" ? requireHash(input.manifestHash, "Admitted manifest hash") : null,
        ]);
        return readPostgresRun(client, runId, relations);
      });
    },

    getRunSummary(input, options) {
      return authorizedRunSummary(input, options, async (request, signal) => {
        const owner = postgresOwnerValues(request.owner);
        return database.transaction(async client => {
          signal?.throwIfAborted();
          await client.query("SET TRANSACTION READ ONLY");
          const result = await client.query(`
            SELECT id,workflow_version_id,workflow_type,status,created_at,updated_at,started_at,completed_at
            FROM ${relations.runs}
            WHERE id=$1 AND owner_kind=$2
              AND owner_principal_id IS NOT DISTINCT FROM $3
              AND owner_account_id IS NOT DISTINCT FROM $4
              AND owner_assistant_session_id IS NOT DISTINCT FROM $5
            LIMIT 1
          `, [request.runId, request.owner.kind, owner.principalId, owner.accountId, owner.assistantSessionId]);
          signal?.throwIfAborted();
          const row = result.rows[0];
          return row ? projectRunSummary({ runId: row.id, workflowVersionId: row.workflow_version_id,
            workflowType: row.workflow_type, status: row.status, createdAt: row.created_at, updatedAt: row.updated_at,
            startedAt: row.started_at, completedAt: row.completed_at }) : null;
        }, { signal });
      });
    },
    async inspectRun(input) {
      const runId = requireText(input?.runId, "Workflow run id");
      const run = await readPostgresRun(database, runId, relations);
      if (!run) throw notFoundRun();
      if (input?.owner) assertOwner(run.owner, normalizeOwner(input.owner));
      return withMetrics(run);
    },
    async inspectAssignment(input) {
      const runId = requireText(input?.runId, "Workflow run id");
      const assignmentId = requireText(input?.assignmentId, "Assignment id");
      return readPostgresAssignmentForRun(database, runId, assignmentId, relations, input?.owner);
    },
    async inspectRunFamilyBudget(input) {
      const runId = requireText(input?.runId, "Workflow run id");
      const run = await readPostgresRun(database, runId, relations);
      if (!run) throw notFoundRun();
      if (input?.owner) assertOwner(run.owner, normalizeOwner(input.owner));
      if (!familyRelations) return null;
      const result = await database.query(`
        WITH family_root AS (
          SELECT root_run_id FROM ${familyRelations.families} WHERE root_run_id=$1
          UNION ALL
          SELECT root_run_id FROM ${familyRelations.members} WHERE run_id=$1
        ), family AS (
          SELECT declaration.* FROM ${familyRelations.families} declaration
          JOIN family_root root ON root.root_run_id=declaration.root_run_id
        ), attempts AS (
          SELECT attempt.* FROM ${relations.attempts} attempt CROSS JOIN family
          WHERE attempt.run_id=family.root_run_id OR EXISTS (
            SELECT 1 FROM ${familyRelations.members} member
            WHERE member.run_id=attempt.run_id AND member.root_run_id=family.root_run_id
          )
        )
        SELECT family.root_run_id,family.budget_version,family.max_descendant_runs,
          family.max_total_attempts,family.max_total_tokens,
          (SELECT count(*) FROM ${familyRelations.members} member
            WHERE member.root_run_id=family.root_run_id) AS descendant_runs,
          (SELECT count(*) FROM attempts) AS total_attempts,
          (SELECT coalesce(sum(total_tokens),0) FROM attempts
            WHERE resource_usage_basis='attempt') AS measured_tokens,
          (SELECT coalesce(sum(resource_reserved_total_tokens),0) FROM attempts
            WHERE resource_usage_basis IS DISTINCT FROM 'attempt') AS held_tokens,
          (SELECT count(*) FROM attempts WHERE outcome IS NOT NULL
            AND resource_usage_basis='unknown') AS unresolved_attempts,
          (SELECT coalesce(sum(greatest(total_tokens-resource_reserved_total_tokens,0)),0)
            FROM attempts WHERE resource_usage_basis='attempt') AS overrun_tokens
        FROM family
      `, [runId]);
      const row = result.rows[0];
      if (!row) return null;
      const budgetVersion = Number(row.budget_version);
      const measuredTokens = budgetVersion === 3 ? budgetInteger(row.measured_tokens) : null;
      const heldTokens = budgetVersion === 3 ? budgetInteger(row.held_tokens) : null;
      const maxTotalTokens = row.max_total_tokens == null ? null : Number(row.max_total_tokens);
      const committedTokens = budgetVersion === 3 ? BigInt(row.measured_tokens) + BigInt(row.held_tokens) : null;
      const tokenLimit = maxTotalTokens == null ? null : BigInt(maxTotalTokens);
      const remainingTokens = tokenLimit == null ? null
        : budgetInteger(tokenLimit > committedTokens ? tokenLimit - committedTokens : 0n);
      const totalAttempts = Number(row.total_attempts);
      const maxTotalAttempts = row.max_total_attempts == null ? null : Number(row.max_total_attempts);
      const descendantRuns = Number(row.descendant_runs);
      const maxDescendantRuns = Number(row.max_descendant_runs);
      return {
        runId, rootRunId: row.root_run_id, budgetVersion,
        maxDescendantRuns, maxTotalAttempts, maxTotalTokens,
        descendantRuns, totalAttempts, measuredTokens, heldTokens,
        unresolvedAttempts: budgetVersion === 3 ? Number(row.unresolved_attempts) : null,
        remainingTokens, overrunTokens: budgetVersion === 3 ? budgetInteger(row.overrun_tokens) : null,
        budgetExceededTokens: budgetVersion === 3
          ? budgetInteger(committedTokens > tokenLimit ? committedTokens - tokenLimit : 0n) : null,
        descendantsExhausted: descendantRuns >= maxDescendantRuns,
        attemptsExhausted: maxTotalAttempts == null ? null : totalAttempts >= maxTotalAttempts,
        tokensExhausted: remainingTokens == null ? null : remainingTokens === 0,
        tokenAdmissionBlocked: budgetVersion === 3 ? BigInt(row.overrun_tokens) > 0n : null,
      };
    },
  };
}

function normalizeAssignment(input, { assignmentId, timestamp }) {
  if (!input || typeof input !== "object") throw new TypeError("Workflow assignment is required.");
  const execution = enumValue(input.execution ?? "subagent", "Assignment execution", new Set(["subagent", "current-agent"]));
  const targets = normalizeTargets(input.targets);
  return {
    assignmentId,
    marker: assignmentMarker(assignmentId),
    parentAssignmentId: optionalText(input.parentAssignmentId),
    stepKey: requireText(input.stepKey, "Assignment step key"),
    assignedTo: requireText(input.assignedTo, "Assignment assignee"),
    execution,
    status: execution === "current-agent" ? "running" : "dispatching",
    targets,
    agentThreadId: optionalText(input.threadId),
    createdAt: timestamp,
    startedAt: execution === "current-agent" ? timestamp : null,
    updatedAt: timestamp,
    completedAt: null,
    finishedAt: null,
    failure: null,
    finishHash: null,
    attempts: [],
  };
}

function normalizePreparedAssignment(input) {
  const requestFingerprint = requireText(input?.requestFingerprint, "Prepared assignment request fingerprint");
  const ref = input?.executionRef;
  if (!ref || ref.kind !== "caller-attempt") {
    throw new TypeError("Prepared assignment requires a caller-attempt execution reference.");
  }
  const executionRef = {
    kind: "caller-attempt",
    assignmentId: requireText(ref.assignmentId, "Caller assignment id"),
    attemptId: requireText(ref.attemptId, "Caller attempt id"),
    threadId: requireText(ref.threadId, "Caller thread id"),
  };
  const preparedExecution = cloneJson(input?.preparedExecution);
  if (!preparedExecution || typeof preparedExecution !== "object" || Array.isArray(preparedExecution)) {
    throw new TypeError("Prepared assignment execution must be an object.");
  }
  for (const field of ["assignmentBrief", "assignmentPrompt"]) {
    assertSafeAuditPrompt(preparedExecution[field], { label: `Prepared ${field}` });
  }
  return { requestFingerprint, executionRef, preparedExecution };
}

function assignmentIdentityHash(runId, assignment) {
  return hashValue({
    runId,
    parentAssignmentId: assignment.parentAssignmentId,
    stepKey: assignment.stepKey,
    assignedTo: assignment.assignedTo,
    execution: assignment.execution,
    targets: assignment.targets,
  });
}

function preparedAssignmentSourceKey(assignmentId) {
  return `assignment-prepared:${assignmentId}`;
}

function preparedAssignmentEvent({ runId, assignmentId, prepared, timestamp }) {
  const sourceKey = preparedAssignmentSourceKey(assignmentId);
  const payload = { schemaVersion: 1, childAssignmentId: assignmentId, ...clone(prepared) };
  return {
    eventId: `event-${hashValue({ runId, sourceKey }).slice(0, 24)}`,
    runId,
    assignmentId: prepared.executionRef.assignmentId,
    attemptId: prepared.executionRef.attemptId,
    sourceKey,
    eventType: "workflow.assignment-prepared",
    threadId: prepared.executionRef.threadId,
    turnId: null,
    occurredAt: null,
    receivedAt: timestamp,
    payload,
    payloadHash: hashValue(payload),
  };
}

function preparedResult(assignment, payload, created) {
  return {
    assignment: clone(assignment),
    executionRef: clone(payload.executionRef),
    preparedExecution: clone(payload.preparedExecution),
    created,
  };
}

function assertPreparedReplay(assignmentId, event, prepared) {
  if (!event || event.eventType !== "workflow.assignment-prepared"
    || event.payload?.childAssignmentId !== assignmentId
    || event.assignmentId !== event.payload?.executionRef?.assignmentId
    || event.attemptId !== event.payload?.executionRef?.attemptId) {
    throw conflict(`Assignment ${assignmentId} has no trusted prepared execution.`);
  }
  if (event.payload?.requestFingerprint !== prepared.requestFingerprint
    || hashValue(event.payload.executionRef) !== hashValue(prepared.executionRef)) {
    throw conflict(`Assignment ${assignmentId} already exists with different input.`);
  }
}

function memoryPreparedResult(run, assignment, prepared) {
  const event = run.events.find(({ sourceKey }) => sourceKey === preparedAssignmentSourceKey(assignment.assignmentId));
  assertPreparedReplay(assignment.assignmentId, event, prepared);
  return preparedResult(assignment, event.payload, false);
}

function assertPreparedExecutionRef({ run, assignment, executionRef }) {
  if (assignment.parentAssignmentId !== executionRef.assignmentId
    || (assignment.execution === "current-agent" && assignment.agentThreadId !== executionRef.threadId)) {
    throw conflict("Prepared assignment caller does not match its parent assignment and thread.");
  }
  const parent = run.assignments.find(({ assignmentId }) => assignmentId === executionRef.assignmentId);
  const attempt = parent?.attempts.find(({ attemptId }) => attemptId === executionRef.attemptId);
  if (!attempt || parent.status !== "running" || attempt.outcome !== null
    || attempt.threadId !== executionRef.threadId) {
    throw conflict("Prepared assignment caller attempt is not active on its bound thread.");
  }
}

function normalizeTargets(value) {
  if (!Array.isArray(value) || value.length === 0) throw new TypeError("Assignment requires one or more targets.");
  const seen = new Set();
  return value.map((target, ordinal) => {
    if (!target || typeof target !== "object") throw new TypeError("Assignment targets must be objects.");
    const normalized = {
      ordinal,
      type: requireText(target.type, "Assignment target type"),
      id: requireText(target.id, "Assignment target id"),
      expectedVersion: target.expectedVersion ?? null,
    };
    const key = `${normalized.type}:${normalized.id}`;
    if (seen.has(key)) throw new TypeError("Assignment targets must be unique.");
    seen.add(key);
    if (normalized.expectedVersion !== null && (!Number.isInteger(normalized.expectedVersion) || normalized.expectedVersion < 0)) {
      throw new TypeError("Assignment target expected version must be a non-negative integer or null.");
    }
    return normalized;
  });
}

function findAssignment(runs, value, { required = true } = {}) {
  const assignmentId = requireText(value, "Assignment id");
  for (const run of runs.values()) {
    const assignment = run.assignments.find((candidate) => candidate.assignmentId === assignmentId);
    if (assignment) return { run, assignment };
  }
  if (!required) return null;
  throw new TypeError(`Unknown workflow assignment: ${assignmentId}.`);
}

function findAttempt(runs, value, { required = true } = {}) {
  const attemptId = requireText(value, "Attempt id");
  for (const run of runs.values()) {
    for (const assignment of run.assignments) {
      const attempt = assignment.attempts.find((candidate) => candidate.attemptId === attemptId);
      if (attempt) return { run, assignment, attempt };
    }
  }
  if (!required) return null;
  throw new TypeError(`Unknown workflow attempt: ${attemptId}.`);
}

async function readCancellationEvents(client, runId, relations) {
  const result = await client.query(`SELECT * FROM ${relations.events}
    WHERE run_id=$1 AND source_key IN ($2,$3)`, [runId, cancellationRequestIdentity, cancellationConfirmationIdentity]);
  return result.rows.map(mapEventRow);
}

async function readPostgresRun(queryable, runId, relations) {
  const runResult = await queryable.query(`SELECT * FROM ${relations.runs} WHERE id=$1`, [runId]);
  const row = runResult.rows[0];
  if (!row) return null;
  const assignmentResult = await queryable.query(`
      SELECT * FROM ${relations.assignments}
      WHERE run_id=$1 ORDER BY assignment_ordinal
    `, [runId]);
  const targetResult = await queryable.query(`
      SELECT * FROM ${relations.assignmentTargets}
      WHERE run_id=$1 ORDER BY assignment_id,target_ordinal
    `, [runId]);
  const attemptResult = await queryable.query(`
      SELECT * FROM ${relations.attempts}
      WHERE run_id=$1 ORDER BY assignment_id,attempt_ordinal
    `, [runId]);
  const eventResult = await queryable.query(`
      SELECT * FROM ${relations.events}
      WHERE run_id=$1 ORDER BY id
    `, [runId]);
  const stagedResult = await queryable.query(`
      SELECT * FROM ${relations.stagedCommands}
      WHERE run_id=$1 ORDER BY command_ordinal
    `, [runId]);
  const validationResult = await queryable.query(`
      SELECT * FROM ${relations.validations}
      WHERE run_id=$1 ORDER BY id
    `, [runId]);
  const admissionResult = await queryable.query(`
      SELECT * FROM ${relations.admissions}
      WHERE run_id=$1
    `, [runId]);
  const targets = new Map();
  for (const target of targetResult.rows) {
    const list = targets.get(target.assignment_id) ?? [];
    list.push(mapTargetRow(target));
    targets.set(target.assignment_id, list);
  }
  const attempts = new Map();
  for (const attempt of attemptResult.rows) {
    const list = attempts.get(attempt.assignment_id) ?? [];
    list.push(mapAttemptRow(attempt));
    attempts.set(attempt.assignment_id, list);
  }
  return {
    runId: row.id,
    workflowVersionId: row.workflow_version_id,
    workflowType: row.workflow_type,
    owner: mapOwnerRow(row),
    input: clone(row.input_payload),
    inputHash: row.input_sha256,
    authorization: clone(row.authorization_snapshot),
    identityHash: row.identity_sha256,
    status: row.status,
    claim: mapClaim(row),
    queuePriority: Number(row.queue_priority),
    recoveryCount: Number(row.recovery_count),
    resumedFromRunId: row.resumed_from_run_id,
    validatedManifestHash: row.validated_manifest_sha256,
    lastValidationPassedAt: asIso(row.last_validation_passed_at),
    admittedManifestHash: row.admitted_manifest_sha256,
    createdAt: asIso(row.created_at),
    updatedAt: asIso(row.updated_at),
    startedAt: asIso(row.started_at),
    completedAt: asIso(row.completed_at),
    failure: row.failure == null ? null : clone(row.failure),
    finalSummary: row.final_summary == null ? null : clone(row.final_summary),
    assignments: assignmentResult.rows.map((assignment) => mapAssignmentRow(
      assignment,
      targets.get(assignment.id) ?? [],
      attempts.get(assignment.id) ?? [],
    )),
    events: eventResult.rows.map(mapEventRow),
    stagedCommands: stagedResult.rows.map(mapJournalStagedRow),
    validations: validationResult.rows.map(mapValidationRow),
    admission: admissionResult.rows[0] ? clone(admissionResult.rows[0].receipt) : null,
  };
}

async function readPostgresAssignment(queryable, assignmentId, relations) {
  const assignmentResult = await queryable.query(
    `SELECT * FROM ${relations.assignments} WHERE id=$1`,
    [assignmentId],
  );
  const targetResult = await queryable.query(`
      SELECT * FROM ${relations.assignmentTargets}
      WHERE assignment_id=$1 ORDER BY target_ordinal
    `, [assignmentId]);
  const attemptResult = await queryable.query(`
      SELECT * FROM ${relations.attempts}
      WHERE assignment_id=$1 ORDER BY attempt_ordinal
    `, [assignmentId]);
  if (!assignmentResult.rows[0]) return null;
  return mapAssignmentRow(
    assignmentResult.rows[0],
    targetResult.rows.map(mapTargetRow),
    attemptResult.rows.map(mapAttemptRow),
  );
}

async function readPostgresAssignmentForRun(queryable, runId, assignmentId, relations, owner) {
  const assignmentResult = await queryable.query(`
      SELECT a.*, r.owner_kind, r.owner_principal_id, r.owner_account_id, r.owner_assistant_session_id
      FROM ${relations.runs} r
      LEFT JOIN ${relations.assignments} a ON a.run_id=r.id AND a.id=$2
      WHERE r.id=$1
    `, [runId, assignmentId]);
  const assignment = assignmentResult.rows[0];
  if (!assignment) throw notFoundRun();
  if (owner) assertOwner(mapOwnerRow(assignment), normalizeOwner(owner));
  if (!assignment.id) throw new TypeError(`Unknown workflow assignment: ${assignmentId}.`);
  const targetResult = await queryable.query(`
      SELECT * FROM ${relations.assignmentTargets}
      WHERE run_id=$1 AND assignment_id=$2 ORDER BY target_ordinal
    `, [runId, assignmentId]);
  const attemptResult = await queryable.query(`
      SELECT * FROM ${relations.attempts}
      WHERE run_id=$1 AND assignment_id=$2 ORDER BY attempt_ordinal
    `, [runId, assignmentId]);
  return mapAssignmentRow(
    assignment,
    targetResult.rows.map(mapTargetRow),
    attemptResult.rows.map(mapAttemptRow),
  );
}

function mapVersionRow(row) {
  if (!row) return null;
  return {
    id: row.id,
    workflowType: row.workflow_type,
    predecessorId: row.predecessor_id,
    definition: clone(row.definition),
    definitionHash: row.definition_sha256,
    codeRevision: row.code_revision,
    changeReason: row.change_reason,
    improvementHypothesis: row.improvement_hypothesis,
    reviewedBy: row.reviewed_by,
    createdAt: asIso(row.created_at),
  };
}

function mapPriceRow(row) {
  if (!row) return null;
  return {
    id: row.id,
    provider: row.provider,
    model: row.model,
    serviceTier: row.service_tier,
    currency: row.currency,
    inputPerMillion: Number(row.input_per_million),
    cachedInputPerMillion: Number(row.cached_input_per_million),
    cacheWriteInputPerMillion: nullableNumber(row.cache_write_input_per_million),
    outputPerMillion: Number(row.output_per_million),
    reasoningOutputPerMillion: nullableNumber(row.reasoning_output_per_million),
    effectiveFrom: asIso(row.effective_from),
    effectiveUntil: asIso(row.effective_until),
    sourceReference: row.source_reference,
    reviewedBy: row.reviewed_by,
    priceHash: row.price_sha256,
    createdAt: asIso(row.created_at),
  };
}

function mapAssignmentRow(row, targets, attempts) {
  return {
    assignmentId: row.id,
    marker: row.marker,
    parentAssignmentId: row.parent_assignment_id,
    stepKey: row.step_key,
    assignedTo: row.assigned_to,
    execution: row.execution,
    status: row.status,
    targets,
    agentThreadId: row.agent_thread_id,
    identityHash: row.identity_sha256,
    createdAt: asIso(row.created_at),
    startedAt: asIso(row.started_at),
    updatedAt: asIso(row.updated_at),
    completedAt: asIso(row.completed_at),
    finishedAt: asIso(row.finished_at),
    failure: row.failure,
    finishHash: row.finish_sha256,
    attempts,
  };
}

function mapTargetRow(row) {
  return {
    ordinal: Number(row.target_ordinal),
    type: row.target_type,
    id: row.target_id,
    expectedVersion: nullableNumber(row.expected_version),
  };
}

function mapAttemptRow(row) {
  const partialUnknown = row.resource_reserved_total_tokens != null && row.resource_usage_basis === "unknown";
  const hasUsage = partialUnknown
    ? [row.input_tokens, row.cached_input_tokens, row.cache_write_input_tokens,
      row.output_tokens, row.reasoning_output_tokens, row.total_tokens].some(value => value != null)
    : row.input_tokens != null || row.total_tokens != null;
  const usage = !hasUsage ? null : {
    inputTokens: nullableNumber(row.input_tokens) ?? (partialUnknown ? null : 0),
    cachedInputTokens: nullableNumber(row.cached_input_tokens) ?? (partialUnknown ? null : 0),
    cacheWriteInputTokens: nullableNumber(row.cache_write_input_tokens) ?? (partialUnknown ? null : 0),
    outputTokens: nullableNumber(row.output_tokens) ?? (partialUnknown ? null : 0),
    reasoningOutputTokens: nullableNumber(row.reasoning_output_tokens) ?? (partialUnknown ? null : 0),
    totalTokens: nullableNumber(row.total_tokens) ?? (partialUnknown ? null : 0),
  };
  const observed = row.observed_model == null && row.observed_provider == null ? null : {
    model: row.observed_model,
    reasoningEffort: row.observed_reasoning_effort,
    serviceTier: row.observed_service_tier,
    provider: row.observed_provider,
  };
  const attempt = {
    attemptId: row.id,
    assignmentId: row.assignment_id,
    ordinal: Number(row.attempt_ordinal),
    prompt: row.prompt,
    promptHash: row.prompt_sha256,
    requested: {
      model: row.requested_model,
      reasoningEffort: row.requested_reasoning_effort,
      serviceTier: row.requested_service_tier,
      provider: null,
    },
    observed,
    capabilityProfile: clone(row.capability_profile),
    capabilityHash: row.capability_sha256,
    threadId: row.thread_id,
    turnId: row.turn_id,
    startedAt: asIso(row.started_at),
    completedAt: asIso(row.completed_at),
    outcome: row.outcome,
    telemetryStatus: row.telemetry_status,
    usage,
    price: row.price_version_id == null ? null : { versionId: row.price_version_id },
    apiEquivalentCost: row.api_equivalent_cost == null ? null : {
      amount: Number(row.api_equivalent_cost),
      currency: row.api_equivalent_cost_currency,
    },
    billedCost: row.billed_cost == null ? null : {
      amount: Number(row.billed_cost),
      currency: row.billed_cost_currency,
    },
    result: row.result_payload == null ? null : clone(row.result_payload),
    resultHash: row.result_sha256,
    error: row.error,
    finishHash: row.finish_sha256,
  };
  if (row.resource_reserved_total_tokens != null) {
    attempt.resourceReservation = { totalTokens: Number(row.resource_reserved_total_tokens) };
    attempt.resourceUsageBasis = row.resource_usage_basis;
  }
  return attempt;
}

function mapEventRow(row) {
  return {
    eventId: String(row.id),
    runId: row.run_id,
    assignmentId: row.assignment_id,
    attemptId: row.attempt_id,
    sourceKey: row.source_key,
    eventType: row.event_type,
    threadId: row.thread_id,
    turnId: row.turn_id,
    occurredAt: asIso(row.occurred_at),
    receivedAt: asIso(row.received_at),
    payload: clone(row.payload),
    payloadHash: row.payload_sha256,
  };
}

function eventIdentityHash(event) {
  return hashValue({
    runId: event.runId,
    assignmentId: event.assignmentId ?? null,
    attemptId: event.attemptId ?? null,
    sourceKey: event.sourceKey,
    eventType: event.eventType,
    threadId: event.threadId ?? null,
    turnId: event.turnId ?? null,
    occurredAt: event.occurredAt ?? null,
    payloadHash: event.payloadHash,
  });
}

function eventInputIdentityHash(input, payloadHash) {
  return hashValue({
    runId: requireText(input?.runId, "Workflow run id"),
    assignmentId: optionalText(input?.assignmentId),
    attemptId: optionalText(input?.attemptId),
    sourceKey: boundedText(input?.sourceKey, "Workflow event source key", 500),
    eventType: requireText(input?.eventType, "Workflow event type"),
    threadId: optionalText(input?.threadId),
    turnId: optionalText(input?.turnId),
    occurredAt: input?.occurredAt == null ? null : isoTimestamp(input.occurredAt, "Workflow event occurrence time"),
    payloadHash,
  });
}

function mapJournalStagedRow(row) {
  return {
    commandId: row.id,
    runId: row.run_id,
    assignmentId: row.assignment_id,
    attemptId: row.attempt_id,
    ordinal: Number(row.command_ordinal),
    commandType: row.command_type,
    schemaVersion: Number(row.schema_version),
    payload: clone(row.payload),
    payloadHash: row.payload_sha256,
    expectedVersions: clone(row.expected_versions),
    dependencyIds: [...row.dependency_ids],
    status: row.status,
    manifestHash: row.manifest_sha256,
    stagedAt: asIso(row.staged_at),
    validatedAt: asIso(row.validated_at),
    admittedAt: asIso(row.admitted_at),
  };
}

function mapValidationRow(row) {
  return {
    validationId: String(row.id),
    runId: row.run_id,
    manifestHash: row.manifest_sha256,
    validatorName: row.validator_name,
    validatorVersion: row.validator_version,
    passed: row.passed,
    issues: clone(row.issues),
    measurements: clone(row.measurements),
    completedAt: asIso(row.completed_at),
  };
}

function mapOwnerRow(row) {
  if (row.owner_kind === "authenticated") {
    return { kind: "authenticated", principalId: row.owner_principal_id, accountId: row.owner_account_id };
  }
  if (row.owner_kind === "public") {
    return { kind: "public", assistantSessionId: row.owner_assistant_session_id };
  }
  return { kind: "system", principalId: row.owner_principal_id };
}

function postgresOwnerValues(owner) {
  return {
    principalId: owner.kind === "public" ? null : owner.principalId,
    accountId: owner.kind === "authenticated" ? owner.accountId : null,
    assistantSessionId: owner.kind === "public" ? owner.assistantSessionId : null,
  };
}

function mapClaim(row) {
  if (!row.claimed_by) return null;
  return {
    workerId: row.claimed_by,
    claimedAt: asIso(row.claimed_at),
    heartbeatAt: asIso(row.heartbeat_at),
    expiresAt: asIso(row.lease_expires_at),
  };
}

function assertRunSummaryFields(value, fields) {
  if (!value || Object.getPrototypeOf(value) !== Object.prototype || Reflect.ownKeys(value).length !== fields.length
    || fields.some(field => {
      const property = Object.getOwnPropertyDescriptor(value, field);
      return !property?.enumerable || !Object.hasOwn(property, "value");
    })) throw new TypeError("Run summary fields must be explicit plain data.");
}

function projectRunSummary(run) {
  const summary = {};
  for (const field of ["runId", "workflowVersionId", "workflowType"]) {
    const value = run[field];
    if (typeof value !== "string" || !value.trim() || Buffer.byteLength(value) > 512) {
      throw new TypeError("Run summary contains an invalid bounded identity.");
    }
    summary[field] = value;
  }
  summary.status = enumValue(run.status, "Run summary status", runStatuses);
  for (const field of ["createdAt", "updatedAt", "startedAt", "completedAt"]) {
    if (run[field] == null && ["startedAt", "completedAt"].includes(field)) continue;
    if (run[field] == null) throw new TypeError("Run summary requires lifecycle timestamps.");
    summary[field] = asIso(run[field]);
  }
  return Object.freeze(summary);
}

function asIso(value) {
  if (value == null) return null;
  return value instanceof Date ? value.toISOString() : new Date(value).toISOString();
}

function nullableNumber(value) {
  return value == null ? null : Number(value);
}

function notFoundRun() {
  const error = new Error("This workflow run is no longer available.");
  error.statusCode = 404;
  return error;
}

function assertRunTransition(run, nextStatus, input) {
  const legal = {
    queued: new Set(["running", "cancelled", "failed"]),
    running: new Set(["validating", "completed", "failed", "cancelled", "blocked"]),
    validating: new Set(["ready-for-admission", "failed", "cancelled", "blocked"]),
    "ready-for-admission": new Set(["admitted", "failed", "cancelled", "blocked"]),
    admitted: new Set(),
    completed: new Set(),
    failed: new Set(),
    cancelled: new Set(),
    blocked: new Set(),
  };
  if (!legal[run.status]?.has(nextStatus)) {
    throw conflict(`Workflow run ${run.runId} cannot transition from ${run.status} to ${nextStatus}.`);
  }
  if (nextStatus === "validating" && run.assignments.some((assignment) => openAssignmentStatuses.has(assignment.status))) {
    throw conflict(`Workflow run ${run.runId} still has active assignments.`);
  }
  if (nextStatus === "ready-for-admission") requireHash(input?.manifestHash, "Validated manifest hash");
  if (nextStatus === "admitted") requireHash(input?.manifestHash, "Admitted manifest hash");
}

function identityAuthorization(input, authorization) {
  if (input.idempotencyKey == null) return authorization;
  // Reauthorization has a new observation timestamp, not necessarily new authority.
  // Retain the original complete snapshot in storage; compare all permission fields.
  const { decisionTime: _decisionTime, ...identity } = authorization;
  return identity;
}

function assertOrdinaryEvent(input) {
  if ([input?.sourceKey, input?.eventType].some(value => typeof value === 'string'
    && /^workflow\.cancellation-(?:requested|confirmed)(?:[.-]|$)/.test(value.trim()))) {
    throw new TypeError('Workflow cancellation event identities are reserved.');
  }
  if ([input?.sourceKey, input?.eventType].some(value => typeof value === "string"
    && /^workflow\.revision-handoff(?:[.-]|$)/.test(value.trim()))) {
    throw new TypeError("Workflow revision handoff event identities are reserved.");
  }
}

function normalizeRevisionOutcome(value) {
  requireExactFields(value, ["gate", "verdict", "manifestHash", "issues"], "Revision outcome");
  const gate = boundedText(value.gate, "Revision gate", 200);
  if (!/^[a-zA-Z0-9][a-zA-Z0-9_.:-]*$/.test(gate) || value.verdict !== "revision-required") {
    throw new TypeError("Revision handoff requires a valid gate and revision-required verdict.");
  }
  const manifestHash = requireHash(value.manifestHash, "Revision manifest hash");
  if (!Array.isArray(value.issues) || !value.issues.length || value.issues.length > 200) {
    throw new TypeError("Revision issues must contain from 1 through 200 entries.");
  }
  const issues = value.issues.map(issue => {
    if (!issue || typeof issue !== "object" || Array.isArray(issue)) throw new TypeError("Revision issue must be an object.");
    return { ...cloneJson(issue), code: requireText(issue.code, "Revision issue code"),
      path: typeof issue.path === "string" ? issue.path : "",
      severity: typeof issue.severity === "string" ? issue.severity : "blocking" };
  }).sort((left, right) => left.code.localeCompare(right.code) || left.path.localeCompare(right.path)
    || left.severity.localeCompare(right.severity));
  return { gate, verdict: "revision-required", manifestHash, issues };
}

function requireExactFields(value, fields, label) {
  if (!value || typeof value !== "object" || Array.isArray(value)
    || Object.keys(value).sort().join("|") !== [...fields].sort().join("|")) {
    throw new TypeError(`${label} has an invalid shape.`);
  }
}

function revisionGrant(input) {
  const claim = normalizeExpectedClaim(input?.expectedClaim);
  if (!claim || input.runId !== claim.runId) throw claimLostError();
  for (const field of ["failure", "finalSummary"]) {
    if (!input[field] || typeof input[field] !== "object" || Array.isArray(input[field])) throw new TypeError(`Revision ${field} must be an object.`);
  }
  return { version: 1, kind: "blocked-revision-handoff", claim,
    outcome: normalizeRevisionOutcome(input.outcome), failure: cloneJson(input.failure), finalSummary: cloneJson(input.finalSummary) };
}

function domainEvent(runId, identity, payload, timestamp) {
  return { eventId: `event-${hashValue({ runId, sourceKey: identity }).slice(0, 24)}`,
    runId, assignmentId: null, attemptId: null, sourceKey: identity, eventType: identity,
    threadId: null, turnId: null, occurredAt: null, receivedAt: timestamp,
    payload: clone(payload), payloadHash: hashValue(payload) };
}

function decodeDomainEvent(event, runId, identity, label = 'revision handoff') {
  if (!event || event.runId !== runId || event.sourceKey !== identity || event.eventType !== identity
    || event.assignmentId != null || event.attemptId != null || event.threadId != null
    || event.turnId != null || event.occurredAt != null || !Number.isFinite(Date.parse(event.receivedAt))
    || event.payloadHash !== hashValue(event.payload)) throw conflict(`Invalid persisted ${label} event.`);
  return event.payload;
}

function decodeRevisionGrant(event, runId) {
  const payload = decodeDomainEvent(event, runId, revisionGrantIdentity);
  requireExactFields(payload, ["version", "kind", "claim", "outcome", "failure", "finalSummary"], "Stored revision grant");
  if (payload.version !== 1 || payload.kind !== "blocked-revision-handoff") throw conflict("Unsupported persisted revision grant.");
  const decoded = revisionGrant({ runId, expectedClaim: payload.claim, outcome: payload.outcome,
    failure: payload.failure, finalSummary: payload.finalSummary });
  if (stableJson(payload) !== stableJson(decoded)) throw conflict("Noncanonical persisted revision grant.");
  return decoded;
}

function assertRevisionGrant(run, event, expected, outcome) {
  if (!event) throw claimLostError();
  const grant = decodeRevisionGrant(event, run.runId);
  if (stableJson(grant.claim) !== stableJson(expected)) throw claimLostError();
  if (run.status !== "blocked" || run.claim != null
    || stableJson(run.failure) !== stableJson(grant.failure)
    || stableJson(run.finalSummary) !== stableJson(grant.finalSummary)) throw conflict("Revision parent no longer matches its blocked outcome.");
  if (stableJson(grant.outcome) !== stableJson(outcome)) throw conflict("Revision outcome does not match the durable blocked outcome.");
  return grant;
}

function normalizeRevisionRequest(input) {
  const combined = requireCombinedRunEvent(input);
  const expected = normalizeExpectedClaim(input?.expectedClaim ?? input?.event?.expectedClaim);
  if (!expected || expected.runId !== combined.parentRunId) throw claimLostError();
  if (input.event.expectedClaim != null && stableJson(normalizeExpectedClaim(input.event.expectedClaim)) !== stableJson(expected)) throw claimLostError();
  const outcome = normalizeRevisionOutcome(input.outcome);
  const childRunId = requireText(input.run.runId, "Revision child run id");
  if (childRunId === combined.parentRunId || input.run.idempotencyKey != null) throw new TypeError("Revision child requires its own explicit run id.");
  const revision = input.run.input?.revision;
  if (!revision || typeof revision !== "object" || Array.isArray(revision)
    || Object.hasOwn(revision, "childRunId") || revision.parentRunId !== combined.parentRunId
    || stableJson(normalizeRevisionOutcome({ gate: revision.gate, verdict: revision.verdict,
      manifestHash: revision.manifestHash, issues: revision.issues })) !== stableJson(outcome)
    || input.event.eventType !== "workflow.semantic-revision-created"
    || stableJson(input.event.payload) !== stableJson({ ...revision, childRunId })) {
    throw conflict("Revision child and event must describe the exact blocked outcome and parent.");
  }
  assertOrdinaryEvent(input.event);
  const { expectedClaim: _runClaim, ...runInput } = input.run;
  const { expectedClaim: _eventClaim, ...eventInput } = input.event;
  // Persist and retry with the same canonical keys. Otherwise a first write may
  // trim a key while its consumed retry searches for the original padded form.
  const run = { ...runInput, runId: childRunId, resumedFromRunId: combined.parentRunId };
  const event = { ...eventInput, runId: combined.parentRunId,
    sourceKey: boundedText(eventInput.sourceKey, "Workflow event source key", 500),
    eventType: requireText(eventInput.eventType, "Workflow event type") };
  // Snapshot the whole request before any asynchronous lock acquisition. Ignore
  // only authority fields; all retained child/event details bind an exact retry.
  const snapshot = cloneJson({ run, event, outcome });
  const consumption = { version: 1, kind: "revision-handoff-consumed", claim: expected,
    outcome, childRunId, requestHash: hashValue(snapshot) };
  return { ...snapshot, parentRunId: combined.parentRunId, expected, consumption };
}

function assertRevisionConsumption(event, grant, desired) {
  if (!event) return false;
  const payload = decodeDomainEvent(event, grant.claim.runId, revisionConsumptionIdentity);
  requireExactFields(payload, ["version", "kind", "claim", "outcome", "childRunId", "requestHash"], "Stored revision consumption");
  if (payload.version !== 1 || payload.kind !== "revision-handoff-consumed"
    || stableJson(normalizeExpectedClaim(payload.claim)) !== stableJson(grant.claim)
    || stableJson(normalizeRevisionOutcome(payload.outcome)) !== stableJson(grant.outcome)) throw conflict("Invalid persisted revision consumption.");
  requireText(payload.childRunId, "Consumed revision child id");
  requireHash(payload.requestHash, "Consumed revision request hash");
  if (stableJson(payload) !== stableJson(desired)) throw conflict("Revision handoff has already been consumed by a different request.");
  return true;
}

function requireCombinedRunEvent(input) {
  if (!input || typeof input !== "object" || Array.isArray(input)
    || !input.run || typeof input.run !== "object" || Array.isArray(input.run)
    || !input.event || typeof input.event !== "object" || Array.isArray(input.event)) {
    throw new TypeError("A resumed workflow run and its parent event are required.");
  }
  assertOrdinaryEvent(input.event);
  const parentRunId = requireText(input.run.resumedFromRunId, "Resumed workflow run id");
  if (requireText(input.event.runId, "Workflow event run id") !== parentRunId) {
    throw new TypeError("Revision event must belong to the resumed parent run.");
  }
  if (input.event.assignmentId != null || input.event.attemptId != null) {
    throw new TypeError("Revision event cannot reference an assignment or attempt.");
  }
  const expected = normalizeExpectedClaim(input.expectedClaim);
  if (expected && input.event.expectedClaim !== undefined
    && stableJson(normalizeExpectedClaim(input.event.expectedClaim)) !== stableJson(expected)) throw claimLostError();
  return { run: input.run, event: expected ? { ...input.event, expectedClaim: expected } : input.event, parentRunId };
}

function resolveRunId(input, createId) {
  if (input?.idempotencyKey == null) {
    const runId = input?.runId == null ? createId() : requireText(input.runId, "Workflow run id");
    if (typeof runId === "string" && /^request:[0-9a-f]{64}$/.test(runId)) {
      throw new TypeError("Workflow run ids in the request hash namespace are reserved for idempotency keys.");
    }
    return runId;
  }
  if (input.runId != null) throw new TypeError("Specify a run id or an idempotency key, not both.");
  const key = requireText(input.idempotencyKey, "Workflow idempotency key");
  if (key.length > 512) throw new TypeError("Workflow idempotency key must not exceed 512 characters.");
  // The database primary key arbitrates concurrent retries, including across workers.
  // Keep authorization and request contents in the existing immutable identity hash:
  // changing either conflicts rather than silently granting authority on replay.
  return `request:${hashValue({ owner: normalizeOwner(input.owner), key })}`;
}

function runningCancellationInput(input, {authorize} = {}) {
  requireExactFields(input, ['runId', 'owner'], 'Running cancellation');
  if (typeof authorize !== 'function') throw new TypeError('Running cancellation requires fresh host authorization.');
  return {runId: boundedText(input.runId, 'Workflow run id', 512), owner: normalizeOwner(structuredClone(input.owner)), authorize};
}

async function authorizeRunningCancellation(run, authorize, version) {
  const target = Object.freeze({runId: run.runId, owner: structuredClone(run.owner), workflowType: run.workflowType,
    workflowVersionId: run.workflowVersionId, status: run.status, authorization: structuredClone(run.authorization),
    cancellationRequested: cancellationState(run).status !== 'none', version: structuredClone(version)});
  if (await authorize(target) !== true) throw Object.assign(new Error('Running cancellation is not authorized.'),
    {statusCode: 403, code: 'RUNNING_CANCELLATION_FORBIDDEN'});
}

function cancellationClaimInput(input) {
  requireExactFields(input, ['runId', 'expectedClaim'], 'Cancellation state read');
  const expected = normalizeExpectedClaim(input.expectedClaim);
  if (!expected || expected.runId !== input.runId) throw claimLostError();
  return expected;
}

function cancellationConfirmationInput(input) {
  requireExactFields(input, ['runId', 'expectedClaim', 'retirementConfirmed'], 'Cancellation confirmation');
  const expected = normalizeExpectedClaim(input.expectedClaim);
  if (!expected || expected.runId !== input.runId) throw claimLostError();
  if (input.retirementConfirmed !== true) throw new TypeError('Cancellation confirmation requires observed owned retirement.');
  return expected;
}

function cancellationIntent(run, timestamp) {
  return {schemaVersion: 1, requestedAt: timestamp,
    claim: {runId: run.runId, workerId: run.claim.workerId, claimedAt: run.claim.claimedAt}};
}

function cancellationRecord(run) {
  const event = run.events.find(event => event.sourceKey === cancellationRequestIdentity);
  if (!event) return null;
  const payload = decodeDomainEvent(event, run.runId, cancellationRequestIdentity, 'cancellation intent');
  requireExactFields(payload, ['schemaVersion', 'requestedAt', 'claim'], 'Stored cancellation intent');
  if (payload.schemaVersion !== 1 || !Number.isFinite(Date.parse(payload.requestedAt))
    || normalizeExpectedClaim(payload.claim)?.runId !== run.runId) throw conflict('Invalid stored cancellation intent.');
  return event;
}

function cancellationState(run) {
  const intent = cancellationRecord(run);
  if (!intent) return {status: 'none', requestedAt: null, confirmedAt: null};
  const confirmation = run.events.find(event => event.sourceKey === cancellationConfirmationIdentity);
  if (confirmation) {
    const payload = decodeDomainEvent(confirmation, run.runId, cancellationConfirmationIdentity, 'cancellation confirmation');
    requireExactFields(payload, ['schemaVersion', 'requestHash', 'confirmedAt'], 'Stored cancellation confirmation');
    if (payload.schemaVersion !== 1 || payload.requestHash !== intent.payloadHash || !Number.isFinite(Date.parse(payload.confirmedAt))
      || run.status !== 'cancelled') throw conflict('Invalid stored cancellation confirmation.');
    return {status: 'confirmed', requestedAt: intent.payload.requestedAt, confirmedAt: payload.confirmedAt};
  }
  return {status: run.status === 'running' ? 'requested' : 'unconfirmed', requestedAt: intent.payload.requestedAt, confirmedAt: null};
}

function assertCancellationRequest(run) {
  if (cancellationRecord(run)) return false;
  if (run.status === 'running' && run.startedAt != null && run.claim != null) return true;
  throw Object.assign(new Error('Only claimed running work can receive cancellation intent.'),
    {statusCode: 409, code: 'RUNNING_CANCELLATION_CONFLICT'});
}

function assertCancellationConfirmation(run, expected) {
  const intent = cancellationRecord(run);
  if (!intent || stableJson(normalizeExpectedClaim(intent.payload.claim)) !== stableJson(expected)) throw claimLostError();
  if (cancellationState(run).status === 'confirmed') return true;
  if (run.status !== 'running' || run.assignments.some(assignment => !terminalAssignmentStatuses.has(assignment.status)
    || assignment.attempts.some(attempt => !terminalAttemptOutcomes.has(attempt.outcome)))) {
    throw Object.assign(new Error('Cancellation cannot be confirmed while owned work remains open.'),
      {statusCode: 409, code: 'CANCELLATION_RETIREMENT_UNCONFIRMED'});
  }
  return false;
}

function cancellationConfirmation(run, timestamp) {
  return {schemaVersion: 1, requestHash: cancellationRecord(run).payloadHash, confirmedAt: timestamp};
}

function assertCancellationTransition(events, status) {
  if (events.some(event => event.sourceKey === cancellationRequestIdentity) && !['failed', 'blocked'].includes(status)) {
    throw Object.assign(new Error('Requested cancellation requires owned retirement confirmation before a terminal outcome.'),
      {statusCode: 409, code: 'RUN_CANCELLATION_REQUESTED'});
  }
}

function queuedCancellationInput(input, {authorize} = {}) {
  requireExactFields(input, ['runId', 'owner'], 'Queued cancellation');
  if (typeof authorize !== 'function') throw new TypeError('Queued cancellation requires fresh host authorization.');
  const runId = requireText(input.runId, 'Workflow run id');
  if (Buffer.byteLength(runId) > 512) throw new TypeError('Queued cancellation requires a bounded run id.');
  return {runId, owner: normalizeOwner(structuredClone(input.owner)), authorize};
}

async function authorizeQueuedCancellation(run, authorize) {
  const target = Object.freeze({runId: run.runId, owner: structuredClone(run.owner),
    workflowType: run.workflowType, workflowVersionId: run.workflowVersionId,
    status: run.status, authorization: structuredClone(run.authorization)});
  if (await authorize(target) !== true) {
    throw Object.assign(new Error('Queued cancellation is not authorized.'),
      {statusCode: 403, code: 'QUEUED_CANCELLATION_FORBIDDEN'});
  }
}

function assertQueuedCancellation(run) {
  const unstarted = run.startedAt == null && run.claim == null && run.assignments.length === 0;
  if (unstarted && run.status === 'cancelled' && run.failure?.code === 'WORKFLOW_QUEUED_CANCELLED') return false;
  if (unstarted && run.status === 'queued') return true;
  throw Object.assign(new Error('Only an unclaimed queued run can be cancelled.'),
    {statusCode: 409, code: 'QUEUED_CANCELLATION_CONFLICT'});
}

function normalizeOwner(owner) {
  if (!owner || typeof owner !== "object") throw new TypeError("Workflow run owner is required.");
  if (owner.kind === "authenticated") {
    return {
      kind: "authenticated",
      principalId: requireText(owner.principalId, "Owner principal id"),
      accountId: optionalText(owner.accountId),
    };
  }
  if (owner.kind === "public") {
    return { kind: "public", assistantSessionId: requireText(owner.assistantSessionId, "Owner assistant session id") };
  }
  if (owner.kind === "system") {
    return { kind: "system", principalId: requireText(owner.principalId, "Owner principal id") };
  }
  throw new TypeError(`Unsupported workflow owner kind: ${owner.kind ?? "missing"}.`);
}

function assertOwner(expected, actual) {
  if (stableJson(expected) !== stableJson(actual)) {
    const error = new Error("This workflow run is no longer available.");
    error.statusCode = 404;
    throw error;
  }
}

function assertResumeSource(source, { owner, workflowType }) {
  if (!source) throw new TypeError("The resumed workflow run does not exist.");
  if (!["failed", "cancelled", "blocked"].includes(source.status)) {
    throw conflict("Only a terminal failed, cancelled, or blocked workflow run can be resumed.");
  }
  if (source.workflowType !== workflowType) {
    throw conflict("A resumed workflow run must use the same workflow type.");
  }
  if (stableJson(source.owner) !== stableJson(owner)) {
    throw conflict("A resumed workflow run must have the same owner.");
  }
}

function workflowQueuePriority(value) {
  const priority = value ?? 0;
  if (!Number.isSafeInteger(priority) || priority < -1_000_000 || priority > 1_000_000) {
    throw new TypeError("Workflow queue priority must be an integer from -1000000 through 1000000.");
  }
  return priority;
}

function normalizeRunFamilyBudget(value) {
  if (value === undefined) return null;
  if (value?.version === 3) {
    if (typeof value !== "object" || Array.isArray(value)
      || Object.keys(value).sort().join("|") !== "maxDescendantRuns|maxTotalAttempts|maxTotalTokens|version"
      || ![value.maxDescendantRuns, value.maxTotalAttempts, value.maxTotalTokens]
        .every(number => Number.isSafeInteger(number) && number >= 0)) {
      throw new TypeError("Run family budget version 3 requires non-negative safe-integer maxDescendantRuns, maxTotalAttempts, and maxTotalTokens.");
    }
    return Object.freeze({ version: 3, maxDescendantRuns: value.maxDescendantRuns,
      maxTotalAttempts: value.maxTotalAttempts, maxTotalTokens: value.maxTotalTokens });
  }
  if (value?.version === 2) {
    if (typeof value !== "object" || Array.isArray(value)
      || Object.keys(value).sort().join("|") !== "maxDescendantRuns|maxTotalAttempts|version"
      || !Number.isSafeInteger(value.maxDescendantRuns) || value.maxDescendantRuns < 0
      || !Number.isSafeInteger(value.maxTotalAttempts) || value.maxTotalAttempts < 0) {
      throw new TypeError("Run family budget version 2 requires non-negative safe-integer maxDescendantRuns and maxTotalAttempts.");
    }
    return Object.freeze({ version: 2, maxDescendantRuns: value.maxDescendantRuns,
      maxTotalAttempts: value.maxTotalAttempts });
  }
  if (!value || typeof value !== "object" || Array.isArray(value)
    || Object.keys(value).length !== 2 || !Object.hasOwn(value, "version")
    || !Object.hasOwn(value, "maxDescendantRuns") || value.version !== 1
    || !Number.isSafeInteger(value.maxDescendantRuns) || value.maxDescendantRuns < 0) {
    throw new TypeError("Run family budget requires version 1 and a non-negative safe-integer maxDescendantRuns.");
  }
  return Object.freeze({ version: 1, maxDescendantRuns: value.maxDescendantRuns });
}

// A reservation controls journal admission only; providers may use more tokens.
function normalizeResourceReservation(value, required) {
  if (!required) {
    if (value !== undefined) throw new TypeError("Resource reservations require a version 3 run family budget.");
    return null;
  }
  if (!value || typeof value !== "object" || Array.isArray(value)
    || Object.keys(value).join("|") !== "totalTokens"
    || !Number.isSafeInteger(value.totalTokens) || value.totalTokens < 0) {
    throw new TypeError("Version 3 attempts require a non-negative safe-integer resourceReservation.totalTokens.");
  }
  return Object.freeze({ totalTokens: value.totalTokens });
}

function normalizeUnknownUsage(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError("Version 3 token usage must be an object when supplied.");
  }
  const usage = {};
  let supplied = false;
  for (const field of ["inputTokens", "cachedInputTokens", "cacheWriteInputTokens", "outputTokens", "reasoningOutputTokens", "totalTokens"]) {
    if (Object.hasOwn(value, field)) {
      if (!Number.isSafeInteger(value[field]) || value[field] < 0) {
        throw new TypeError(`Attempt ${field} must be a non-negative safe integer when supplied.`);
      }
      usage[field] = value[field];
      supplied = true;
    } else {
      usage[field] = null;
    }
  }
  if (!supplied) return null;
  if (usage.totalTokens != null && usage.inputTokens != null && usage.outputTokens != null
    && usage.totalTokens < usage.inputTokens + usage.outputTokens) {
    throw new TypeError("Total tokens cannot be less than input plus output tokens.");
  }
  if (usage.inputTokens != null
    && (usage.cachedInputTokens ?? 0) + (usage.cacheWriteInputTokens ?? 0) > usage.inputTokens) {
    throw new TypeError("Cached and cache-write input tokens cannot exceed input tokens.");
  }
  if (usage.outputTokens != null && usage.reasoningOutputTokens != null
    && usage.reasoningOutputTokens > usage.outputTokens) {
    throw new TypeError("Reasoning output tokens cannot exceed output tokens.");
  }
  return usage;
}

function normalizeResourceUsageBasis(value, required, finish, rawUsage) {
  if (!required) {
    if (value !== undefined) throw new TypeError("Resource usage basis requires a version 3 run family budget.");
    return null;
  }
  if (value !== "attempt" && value !== "unknown") {
    throw new TypeError("Version 3 attempts require resourceUsageBasis 'attempt' or 'unknown'.");
  }
  const fields = ["inputTokens", "cachedInputTokens", "cacheWriteInputTokens", "outputTokens", "reasoningOutputTokens", "totalTokens"];
  if (rawUsage != null) {
    if (typeof rawUsage !== "object" || Array.isArray(rawUsage)) {
      throw new TypeError("Version 3 token usage must be an object when supplied.");
    }
    for (const field of fields) {
      if (Object.hasOwn(rawUsage, field)
        && (!Number.isSafeInteger(rawUsage[field]) || rawUsage[field] < 0)) {
        throw new TypeError(`Attempt ${field} must be a non-negative safe integer when supplied.`);
      }
    }
  }
  if (value === "attempt") {
    if (finish.telemetryStatus !== "complete" || !finish.usage
      || !rawUsage || typeof rawUsage !== "object" || Array.isArray(rawUsage)) {
      throw new TypeError("Measured attempt resource usage requires complete telemetry and usage.");
    }
    for (const field of fields) {
      if (!Object.hasOwn(rawUsage, field) || !Number.isSafeInteger(rawUsage[field])
        || rawUsage[field] < 0) {
        throw new TypeError(`Measured attempt ${field} must be an explicitly supplied non-negative safe integer.`);
      }
    }
  }
  return value;
}

function memoryFamilyBudget(runs, members, rootRunId, family) {
  let attemptCount = 0;
  let settledTokens = 0n;
  let heldTokens = 0n;
  let unresolvedAttempts = 0;
  let overrunTokens = 0n;
  for (const run of runs.values()) {
    if (run.runId !== rootRunId && members.get(run.runId) !== rootRunId) continue;
    for (const assignment of run.assignments) for (const attempt of assignment.attempts) {
      attemptCount += 1;
      if (family.version !== 3) continue;
      if (attempt.resourceUsageBasis === "attempt") {
        settledTokens += BigInt(attempt.usage.totalTokens);
        overrunTokens += BigInt(Math.max(0, attempt.usage.totalTokens - attempt.resourceReservation.totalTokens));
      }
      else {
        heldTokens += BigInt(attempt.resourceReservation.totalTokens);
        if (attempt.outcome !== null) unresolvedAttempts += 1;
      }
    }
  }
  const tokenLimit = family.version === 3 ? BigInt(family.maxTotalTokens) : null;
  const remainingTokens = tokenLimit === null ? null
    : budgetInteger(tokenLimit > settledTokens + heldTokens ? tokenLimit - settledTokens - heldTokens : 0n);
  return {
    runId: rootRunId, rootRunId, budgetVersion: family.version,
    maxDescendantRuns: family.maxDescendantRuns, maxTotalAttempts: family.maxTotalAttempts,
    maxTotalTokens: family.maxTotalTokens, descendantRuns: family.memberCount,
    totalAttempts: attemptCount, measuredTokens: family.version === 3 ? budgetInteger(settledTokens) : null,
    heldTokens: family.version === 3 ? budgetInteger(heldTokens) : null,
    unresolvedAttempts: family.version === 3 ? unresolvedAttempts : null,
    remainingTokens, overrunTokens: family.version === 3 ? budgetInteger(overrunTokens) : null,
    budgetExceededTokens: family.version === 3
      ? budgetInteger(settledTokens + heldTokens > tokenLimit ? settledTokens + heldTokens - tokenLimit : 0n) : null,
    descendantsExhausted: family.memberCount >= family.maxDescendantRuns,
    attemptsExhausted: family.maxTotalAttempts == null ? null : attemptCount >= family.maxTotalAttempts,
    tokensExhausted: remainingTokens == null ? null : remainingTokens === 0,
    tokenAdmissionBlocked: family.version === 3 ? overrunTokens > 0n : null,
  };
}

function budgetInteger(value) {
  const integer = BigInt(value);
  // Aggregates can exceed JavaScript's exact number range after an overrun.
  return integer <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(integer) : integer.toString();
}

function requireRun(runs, value) {
  const runId = requireText(value, "Workflow run id");
  const run = runs.get(runId);
  if (!run) {
    const error = new Error("This workflow run is no longer available.");
    error.statusCode = 404;
    throw error;
  }
  return run;
}

function assertRunAcceptsAssignment(run, assignment) {
  if (run.status === "running") return;
  if (run.status === "ready-for-admission"
    && assignment.parentAssignmentId === null
    && assignment.execution === "current-agent"
    && assignment.stepKey === `${run.workflowType}.llm-review`) return;
  if (run.status === "ready-for-admission") {
    throw conflict(`Workflow run ${run.runId} only accepts its independent LLM reviewer after deterministic validation.`);
  }
  throw conflict(`Workflow run ${run.runId} is not accepting assignments.`);
}

function publicRunSummary(run) {
  return clone({
    runId: run.runId,
    workflowVersionId: run.workflowVersionId,
    workflowType: run.workflowType,
    status: run.status,
    createdAt: run.createdAt,
    updatedAt: run.updatedAt,
  });
}

function withMetrics(run) {
  return { ...run, metrics: projectMetrics(run) };
}

function isExpired(claim, timestamp) {
  return !claim || Date.parse(claim.expiresAt) <= Date.parse(timestamp);
}

function addSeconds(timestamp, seconds) {
  const milliseconds = Date.parse(timestamp);
  if (!Number.isFinite(milliseconds)) throw new TypeError(`Invalid workflow timestamp: ${timestamp}.`);
  return new Date(milliseconds + seconds * 1000).toISOString();
}

function hashValue(value) {
  return createHash("sha256").update(stableJson(value)).digest("hex");
}

function stableJson(value) {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stableJson(value[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

function cloneJson(value) {
  if (value == null || typeof value !== "object") throw new TypeError("Workflow JSON values must be objects or arrays.");
  try {
    return JSON.parse(JSON.stringify(value));
  } catch (error) {
    throw new TypeError("Workflow JSON values must be serializable.", { cause: error });
  }
}

function boundedInteger(value, label, minimum, maximum) {
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw new TypeError(`${label} must be an integer from ${minimum} through ${maximum}.`);
  }
  return value;
}

function nonNegativeNumber(value, label) {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    throw new TypeError(`${label} must be a non-negative finite number.`);
  }
  return value;
}

function boundedText(value, label, maximumLength) {
  const text = requireText(value, label);
  if (text.length > maximumLength) throw new TypeError(`${label} must be at most ${maximumLength} characters.`);
  return text;
}

function enumValue(value, label, allowed) {
  if (!allowed.has(value)) throw new TypeError(`Unsupported ${label.toLowerCase()}: ${value ?? "missing"}.`);
  return value;
}

function isoTimestamp(value, label) {
  const milliseconds = Date.parse(value);
  if (!Number.isFinite(milliseconds)) throw new TypeError(`${label} must be an ISO timestamp.`);
  return new Date(milliseconds).toISOString();
}

function requireHash(value, label) {
  const hash = requireText(value, label);
  if (!/^[0-9a-f]{64}$/.test(hash)) throw new TypeError(`${label} must be a lowercase SHA-256 hash.`);
  return hash;
}

function requireText(value, label) {
  if (typeof value !== "string" || !value.trim()) throw new TypeError(`${label} is required.`);
  return value.trim();
}

function optionalText(value) {
  if (value == null) return null;
  return requireText(value, "Optional text");
}

function conflict(message) {
  const error = new Error(message);
  error.code = "WORKFLOW_CONFLICT";
  error.statusCode = 409;
  return error;
}

function clone(value) {
  return structuredClone(value);
}

}
