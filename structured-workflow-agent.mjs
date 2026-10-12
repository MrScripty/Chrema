import { createCodexTelemetry } from "./codex-telemetry.mjs";
import { boundedAnalysisExecutionProfile as BOUNDED_ANALYSIS_EXECUTION_PROFILE } from "./agent-platform-bounded-analysis-execution.mjs";
import { workflowDefinitionHash } from './agent-platform-prompt.mjs';
import { resolveWorkflowToolAssignment, assignedProviderQualificationRequest,
  assignedProviderCapabilityRequestV2, validateAssignedProviderQualification } from './agent-platform-definition-tool-assignment.mjs';
import { resolveWorkflowExecutionDeadline } from './agent-platform-execution-deadline.mjs';
import {createHash} from 'node:crypto';
// The journal hashes the payload itself; runtime binding identities use a value wrapper.
function journalJson(value){if(Array.isArray(value))return `[${value.map(journalJson).join(',')}]`;
 if(value&&typeof value==='object')return `{${Object.keys(value).sort().map(key=>`${JSON.stringify(key)}:${journalJson(value[key])}`).join(',')}}`;
 return JSON.stringify(value);}
const journalHash=value=>createHash('sha256').update(journalJson(value)).digest('hex');

/** Executes one canned, structured model step through the journal seam. */
export function createStructuredWorkflowAgent({ journal, codex, prices = [], now = () => new Date().toISOString(), assignedExecution } = {}) {
  requireMethod(journal, "startAttempt", "AgentWorkflowJournal");
  requireMethod(journal, "finishAssignment", "AgentWorkflowJournal");
  requireMethod(journal, "inspectRun", "AgentWorkflowJournal");
  if (assignedExecution === undefined) {
    requireMethod(codex, "research", "CodexBridge");
    requireMethod(codex, "resolveCapabilitySnapshot", "CodexBridge");
  }
  requireMethod(codex, "subscribeLifecycle", "CodexBridge");
  const telemetry = createCodexTelemetry({ journal, prices, now });
  const assignedDefinition = assignedExecution === undefined ? null : structuredClone(assignedExecution.definition);
  const assignedParts = assignedExecution === undefined ? null : structuredClone(assignedExecution.parts);
  const resumeOriginalAttempt = assignedExecution?.resumeOriginalAttempt;
  const providerCapabilities = assignedExecution?.providerCapabilities === undefined ? undefined : structuredClone(assignedExecution.providerCapabilities);

  return {
    async run({ runId, assignmentId, prompt, executionProfile, validateOutput = (value) => value, forbiddenThreadIds = [], signal,
      requestedDeadlineAt = null, existingAttempt } = {}) {
      signal?.throwIfAborted();
      const rendered = normalizeRenderedPrompt(prompt);
      if (executionProfile !== undefined && rendered.executionProfile !== executionProfile) {
        throw new TypeError("Structured workflow execution profile does not match the rendered prompt.");
      }
      const selectedExecutionProfile = rendered.executionProfile ?? executionProfile;
      let assigned = null;
      if (rendered.toolAssignment !== undefined || assignedExecution !== undefined) {
        if (!assignedExecution || rendered.toolAssignment === undefined || selectedExecutionProfile !== undefined) {
          throw new TypeError('Assigned structured execution requires its explicit host and rendered selection.');
        }
        for (const method of ['ensureStarted', 'createToolAssignmentAttempt', 'cancelToolAssignmentAttempt', 'assignedAttempt']) {
          requireMethod(codex, method, 'Assigned CodexBridge');
        }
        const resolved = resolveWorkflowToolAssignment({ definition: assignedDefinition, parts: assignedParts,
          registrations: assignedExecution.registrations, hostTools: assignedExecution.hostTools, partId: rendered.toolAssignment.partId });
        if (workflowDefinitionHash({ value: rendered.toolAssignment }) !== workflowDefinitionHash({ value: resolved.selection })
          || JSON.stringify(rendered.allowedTools) !== JSON.stringify(resolved.selection.tools.map(tool => tool.toolId))
          || assignedDefinition.steps.find(step => step.id === resolved.selection.stepId).prompt.id !== rendered.promptVersionId) {
          throw new TypeError('Rendered tool assignment does not match the current host definition.');
        }
        const run = await journal.inspectRun({ runId });
        const assignment = run?.assignments?.find(value => value.assignmentId === assignmentId);
        if (run?.runId !== runId || run.workflowVersionId !== assignedDefinition.id || assignment?.stepKey !== resolved.selection.stepId) {
          throw new TypeError('Assigned execution does not match durable run and assignment lineage.');
        }
        let original;
        if (existingAttempt !== undefined) {
          if (!existingAttempt || Object.getPrototypeOf(existingAttempt) !== Object.prototype
            || Object.keys(existingAttempt).sort().join('|') !== 'attemptId|threadId') throw new TypeError('Original attempt identity is required.');
          if (typeof resumeOriginalAttempt !== 'function' || typeof assignedExecution.execute !== 'function') {
            throw new TypeError('Original attempt reentry requires accepted root-cycle recovery admission.');
          }
          original = assertOriginalAttempt(run, {assignmentId, attemptId: existingAttempt.attemptId,
            threadId: existingAttempt.threadId, rendered, requireOpen: true});
          const admission = await resumeOriginalAttempt.call(assignedExecution, Object.freeze({runId, assignmentId,
            attemptId: original.attemptId, threadId: original.threadId, signal}));
          const keys = ['mode', 'attemptId', 'threadId', 'recordHash'];
          if (!admission || Object.getPrototypeOf(admission) !== Object.prototype
            || Reflect.ownKeys(admission).length !== keys.length || keys.some(key => {
              const field = Object.getOwnPropertyDescriptor(admission, key);
              return !field?.enumerable || !Object.hasOwn(field, 'value');
            }) || !['accepted-root-cycles.v1', 'accepted-part-cycles.v1'].includes(admission.mode) || admission.attemptId !== original.attemptId
            || admission.threadId !== original.threadId || typeof admission.recordHash !== 'string'
            || !/^[a-f0-9]{64}$/.test(admission.recordHash)) {
            throw new TypeError('Original attempt recovery admission does not match accepted progress.');
          }
          signal?.throwIfAborted();
        }
        requireMethod(journal, 'inspectVersion', 'Assigned AgentWorkflowJournal');
        const version = await journal.inspectVersion({ workflowVersionId: run.workflowVersionId });
        if (version?.id !== assignedDefinition.id || version.workflowType !== assignedDefinition.workflowType
          || version.definitionHash !== assignedDefinition.definitionHash
          || workflowDefinitionHash(version.definition) !== version.definitionHash) {
          throw new TypeError('Assigned execution does not match the exact durable workflow definition.');
        }
        if (!Number.isSafeInteger(assignedExecution.maxRunDurationMs) || assignedExecution.maxRunDurationMs < 1
          || typeof assignedExecution.authorize !== 'function') throw new TypeError('Assigned execution requires bounded duration and host authorization.');
        const deadlineJournal = typeof journal.inspectAncestorRun === 'function'
          ? { inspectRun: input => journal.inspectAncestorRun(input) } : journal;
        const deadline = await resolveWorkflowExecutionDeadline({ journal: deadlineJournal, run, maxRunDurationMs: assignedExecution.maxRunDurationMs,
          requestedDeadlineAt, now });
        if (!deadline || deadline.remainingMs <= 0) throw new Error('Assigned workflow execution deadline has expired.');
        if (original && original.capabilityProfile.deadlineAtMs !== deadline.deadlineAtMs) throw new Error('Original workflow deadline changed.');
        const executionMode = assignedExecution.execute === undefined ? 'root-turn' : 'host-parts';
        // Native turns require qualification from the provider adapter itself;
        // a host-dispatch receipt cannot qualify the current unknown native catalog.
        const qualify = executionMode === 'root-turn' ? codex.qualifyAssignedExecution?.bind(codex) : assignedExecution.qualify;
        if (typeof qualify !== 'function') throw new Error('Assigned provider capability qualification is unavailable.');
        const request = providerCapabilities === undefined
          ? assignedProviderQualificationRequest(resolved, rendered, { executionMode })
          : assignedProviderCapabilityRequestV2(resolved, rendered, { executionMode,providerCapabilities });
        let receipt;
        try { receipt = await qualify(request, { signal, deadlineAtMs: deadline.deadlineAtMs }); }
        catch { throw new Error('Assigned provider capability qualification failed.'); }
        const qualification = validateAssignedProviderQualification(receipt, request);
        if (original) validateAssignedProviderQualification(original.capabilityProfile.providerQualification, request);
        signal?.throwIfAborted();
        if (Date.now() >= deadline.deadlineAtMs) throw new Error('Assigned workflow execution deadline has expired.');
        await codex.ensureStarted();
        assigned = { resolved, deadline, qualification, token: null, original };
      }
      if (existingAttempt !== undefined && !assigned) throw new TypeError('Original attempt reentry requires assigned execution.');
      const capabilitySnapshot = assigned ? {} : codex.resolveCapabilitySnapshot({
        webSearchMode: "disabled",
        allowedTools: rendered.allowedTools,
        workspaceAccess: rendered.workspaceAccess,
        networkAccess: rendered.networkAccess,
        ...(selectedExecutionProfile ? { executionProfile: selectedExecutionProfile } : {}),
      });
      if (typeof validateOutput !== "function") throw new TypeError("Structured workflow output validator must be a function.");
      const forbidden = new Set(normalizeTextArray(forbiddenThreadIds, "Forbidden workflow thread id"));
      const attempt = assigned?.original ?? await journal.startAttempt({
        assignmentId,
        prompt: rendered.text,
        requested: {
          model: rendered.model,
          reasoningEffort: rendered.reasoningEffort,
          serviceTier: rendered.serviceTier,
        },
        capabilityProfile: {
          ...capabilitySnapshot,
          promptVersionId: rendered.promptVersionId,
          allowedTools: rendered.allowedTools,
          workspaceAccess: rendered.workspaceAccess,
          networkAccess: rendered.networkAccess,
          structuredOutput: true,
          ...(assigned ? { toolAssignment: assigned.resolved.assignment, selectedToolPart: assigned.resolved.selection,
            providerQualification: assigned.qualification, deadlineAtMs: assigned.deadline.deadlineAtMs } : {}),
          ...(selectedExecutionProfile ? { executionProfile: selectedExecutionProfile } : {}),
        },
      });
      const lineage = { runId, assignmentId, attemptId: attempt.attemptId };
      const pending = new Set();
      let threadId = assigned?.original?.threadId ?? null;
      let binding = Promise.resolve();
      const unsubscribe = codex.subscribeLifecycle((message) => {
        if (!threadId || message?.params?.threadId !== threadId) return undefined;
        const task = telemetry.capture(message, lineage).finally(() => pending.delete(task));
        pending.add(task);
        return task;
      });
      let rawText = null;
      try {
        if (assigned) {
          assigned.token = codex.createToolAssignmentAttempt({ declarations: assigned.resolved.declarations,
            parentParts: assigned.resolved.parentParts, attemptId: attempt.attemptId, deadline: assigned.deadline,
            authorize: assignedExecution.authorize, signal, workspaceAccess: rendered.workspaceAccess, networkAccess: rendered.networkAccess });
        }
        const request = {
          signal,
          prompt: rendered.text,
          settings: {
            model: rendered.model,
            reasoningEffort: rendered.reasoningEffort,
            serviceTier: rendered.serviceTier,
          },
          workspaceAccess: rendered.workspaceAccess,
          networkAccess: rendered.networkAccess,
          webSearchMode: "disabled",
          allowedTools: rendered.allowedTools,
          capabilitySnapshot,
          ...(selectedExecutionProfile ? { executionProfile: selectedExecutionProfile } : {}),
          onThread(value) {
            const selected = requireText(value, "Structured workflow thread id");
            if (assigned?.original && selected !== assigned.original.threadId) throw new Error('Original root thread changed.');
            if (forbidden.has(selected)) throw new Error(`Structured workflow step reused forbidden thread ${selected}.`);
            threadId = selected;
            binding = assigned?.original ? Promise.resolve() : journal.bindAttempt({ attemptId: attempt.attemptId, threadId: selected });
            return binding;
          },
        };
        let response;
        if (assigned) {
          const options = { ...request, attempt: assigned.token, partId: assigned.resolved.selection.partId,
            qualification: assigned.qualification };
          if (assignedExecution.execute !== undefined) {
            if (typeof assignedExecution.execute !== 'function') throw new TypeError('Assigned host executor must be a function.');
            response = await assignedExecution.execute(options);
          } else {
            requireMethod(codex, 'researchAssignedPart', 'Assigned CodexBridge');
            const result = await codex.researchAssignedPart(options);
            response = { ...result, attemptId: attempt.attemptId, assignmentHash: assigned.resolved.assignment.assignmentHash,
              partId: assigned.resolved.selection.partId };
          }
          if (response?.attemptId !== attempt.attemptId || response.assignmentHash !== assigned.resolved.assignment.assignmentHash
            || response.partId !== assigned.resolved.selection.partId || threadId === null || response.threadId !== threadId) {
            throw new Error('Assigned provider response lineage mismatch.');
          }
          if (assignedExecution.execute !== undefined) {
            requireMethod(codex, 'authorizeAssignedAnswer', 'Assigned CodexBridge');
            await codex.authorizeAssignedAnswer({ attempt: assigned.token, partId: assigned.resolved.selection.partId,
              threadId: response.threadId });
          }
          signal?.throwIfAborted();
          // Retirement is an execution boundary: no output is finalized before owned work drains.
          await codex.cancelToolAssignmentAttempt(assigned.token);
          signal?.throwIfAborted();
          if (Date.now() >= assigned.deadline.deadlineAtMs) throw new Error('Assigned workflow execution deadline has expired.');
        } else response = await codex.research(request);
        rawText = response.text;
        threadId ??= requireText(response.threadId, "Structured workflow thread id");
        if (forbidden.has(threadId)) throw new Error(`Structured workflow step reused forbidden thread ${threadId}.`);
        await binding;
        await Promise.allSettled([...pending]);
        const output = validateOutput(parseClosedJson(rawText));
        const finalized = await telemetry.finalizeAttempt({ ...lineage, result: { output, rawText } });
        if (finalized.outcome !== "completed" || finalized.telemetryStatus !== "complete") {
          throw new Error(`Structured workflow step telemetry is ${finalized.telemetryStatus}.`);
        }
        await journal.finishAssignment({ assignmentId, status: "completed" });
        return { output, attempt: finalized, threadId };
      } catch (error) {
        const lostClaim = error?.code === 'WORKFLOW_CLAIM_LOST';
        let retirementFailed = error?.assignedRetirementUnconfirmed === true;
        if (assigned?.token) {
          try { await codex.cancelToolAssignmentAttempt(assigned.token); }
          catch { retirementFailed = true; }
        }
        const unconfirmed = cleanupError => cleanupError?.code === 'WORKFLOW_CLAIM_LOST'
          ? Object.assign(new Error('Workflow claim is no longer held by this worker.'), {
            code: 'WORKFLOW_CLAIM_LOST', statusCode: 409, assignedRetirementUnconfirmed: true,
          })
          : Object.assign(new Error('Assigned workflow retirement failed.'), {assignedRetirementUnconfirmed: true});
        if (assigned && !lostClaim) error = retirementFailed ? unconfirmed(error) : new Error('Assigned structured workflow execution failed.');
        try {
          await binding.catch(() => undefined);
          await Promise.allSettled([...pending]);
          if (lostClaim) throw retirementFailed ? unconfirmed(error) : error;
          const current = await findAttempt(journal, lineage);
          if (current?.outcome == null) {
            await telemetry.finalizeAttempt({
              ...lineage,
              result: rawText == null ? {} : { rawText },
              error: error instanceof Error ? error.message : String(error),
            }).catch(() => undefined);
          }
          const run = await journal.inspectRun({ runId });
          const assignment = run.assignments.find(({ assignmentId: id }) => id === assignmentId);
          if (assignment && !["completed", "failed", "interrupted"].includes(assignment.status)) {
            await journal.finishAssignment({ assignmentId, status: "failed", failure: error instanceof Error ? error.message : String(error) });
          }
        } catch (cleanupError) {
          // Claim loss cannot hide a retirement failure, including one known
          // before a later claimed journal transition rejects cleanup.
          throw retirementFailed ? unconfirmed(cleanupError) : cleanupError;
        }
        throw error;
      } finally {
        unsubscribe?.();
      }
    },
  };
}

function assertOriginalAttempt(run, {assignmentId, attemptId, threadId, rendered, requireOpen = false}) {
  const refuse = () => {throw new Error('Original structured attempt is unavailable.');};
  const assignment = run?.assignments?.[0], attempt = assignment?.attempts?.[0];
  if (run?.status !== 'running' || run.resumedFromRunId !== null || run.assignments.length !== 1
    || !assignment || assignment.assignmentId !== assignmentId || assignment.parentAssignmentId !== null
    || assignment.execution !== 'current-agent' || !['running', 'completed'].includes(assignment.status)
    || assignment.attempts.length !== 1 || !attempt || attempt.attemptId !== attemptId || attempt.assignmentId !== assignmentId
    || attempt.ordinal !== 1 || attempt.threadId !== threadId || assignment.agentThreadId !== threadId
    || typeof threadId !== 'string' || !threadId || attempt.prompt !== rendered.text
    || attempt.promptHash !== journalHash(rendered.text)
    || attempt.capabilityHash !== journalHash(attempt.capabilityProfile)
    || workflowDefinitionHash({value: attempt.requested}) !== workflowDefinitionHash({value: {
      model: rendered.model, reasoningEffort: rendered.reasoningEffort, serviceTier: rendered.serviceTier, provider: null}})) refuse();
  const capability = attempt.capabilityProfile;
  if (capability.structuredOutput !== true || capability.workspaceAccess !== 'read-only' || capability.networkAccess !== false
    || capability.executionProfile !== undefined || capability.promptVersionId !== rendered.promptVersionId
    || JSON.stringify(capability.allowedTools) !== JSON.stringify(rendered.allowedTools)
    || workflowDefinitionHash({value: capability.selectedToolPart}) !== workflowDefinitionHash({value: rendered.toolAssignment})
    || capability.toolAssignment?.assignmentHash !== run.authorization?.assignedRuntime?.assignmentHash
    || capability.toolAssignment.definitionId !== run.workflowVersionId
    || capability.toolAssignment.definitionHash !== run.authorization.assignedRuntime.definitionHash
    || capability.selectedToolPart.stepId !== assignment.stepKey || !Number.isSafeInteger(capability.deadlineAtMs)) refuse();
  if (requireOpen) {
    if (attempt.outcome !== null || assignment.status !== 'running' || capability.deadlineAtMs <= Date.now()) refuse();
  } else if (attempt.outcome !== null && attempt.outcome !== 'completed') refuse();
  if (assignment.status === 'completed' && attempt.outcome !== 'completed') refuse();
  return attempt;
}

/** Finalize the already accepted root answer without constructing a provider.
 * Its caller must first validate the progress owner's accepted cursor and body. */
export async function finalizeAcceptedStructuredWorkflowAttempt({journal, prices = [], now = () => new Date().toISOString(),
  runId, assignmentId, attemptId, threadId, prompt, output, authorizeFinalization, signal} = {}) {
  for (const method of ['inspectRun', 'inspectVersion', 'assertExpectedClaim', 'finishAssignment']) requireMethod(journal, method, 'Accepted structured journal');
  if (typeof authorizeFinalization !== 'function' || !output || Object.getPrototypeOf(output) !== Object.prototype) {
    throw new TypeError('Accepted output and fresh finalization authority are required.');
  }
  const rendered = normalizeRenderedPrompt(prompt), rawText = JSON.stringify(output);
  const outputHash = workflowDefinitionHash({value: output});
  async function fresh() {
    signal?.throwIfAborted();
    await journal.assertExpectedClaim({runId});
    if (await authorizeFinalization(Object.freeze({runId, assignmentId, attemptId, threadId, outputHash})) !== true) {
      throw new Error('Accepted root finalization is not authorized.');
    }
    signal?.throwIfAborted();
    await journal.assertExpectedClaim({runId});
  }
  await fresh();
  let run = await journal.inspectRun({runId});
  let attempt = assertOriginalAttempt(run, {assignmentId, attemptId, threadId, rendered});
  const version = await journal.inspectVersion({workflowVersionId: run.workflowVersionId});
  if (version?.definitionHash !== run.authorization.assignedRuntime.definitionHash
    || workflowDefinitionHash(version.definition) !== version.definitionHash) throw new Error('Original immutable definition is unavailable.');
  const capability = attempt.capabilityProfile, resolved = {assignment: capability.toolAssignment, selection: capability.selectedToolPart};
  const request = capability.providerQualification?.schemaVersion === 2
    ? assignedProviderCapabilityRequestV2(resolved, rendered, {executionMode: 'host-parts', providerCapabilities: capability.providerQualification.providerCapabilities})
    : assignedProviderQualificationRequest(resolved, rendered, {executionMode: 'host-parts'});
  validateAssignedProviderQualification(capability.providerQualification, request);
  await fresh();
  const telemetry = createCodexTelemetry({journal, prices, now});
  attempt = await telemetry.finalizeAttempt({runId, assignmentId, attemptId, result: {output, rawText}});
  const capture = attempt.result?.outputCapture;
  const finish = Object.fromEntries(['outcome', 'observed', 'usage', 'telemetryStatus', 'result', 'error', 'price', 'apiEquivalentCost', 'billedCost'].map(key => [key, attempt[key]]));
  if (attempt.resourceReservation) finish.resourceUsageBasis = attempt.resourceUsageBasis;
  if (attempt.outcome !== 'completed' || attempt.telemetryStatus !== 'complete' || attempt.error !== null
    || attempt.resultHash !== journalHash(attempt.result) || attempt.finishHash !== journalHash(finish)
    || attempt.result?.telemetryIssues?.length !== 0 || workflowDefinitionHash({value: attempt.result.output}) !== outputHash
    || capture?.status !== 'complete' || capture.source !== 'runtime-response' || capture.text !== rawText
    || capture.byteLength !== Buffer.byteLength(rawText) || capture.sha256 !== createHash('sha256').update(rawText).digest('hex')) {
    throw new Error('Accepted root telemetry and output do not match.');
  }
  await fresh();
  run = await journal.inspectRun({runId});
  assertOriginalAttempt(run, {assignmentId, attemptId, threadId, rendered});
  const assignment = run.assignments[0];
  if (assignment.status === 'completed') {
    if (assignment.failure !== null || !assignment.completedAt || !assignment.finishedAt
      || assignment.finishHash !== journalHash({status: 'completed', failure: null})) {
      throw new Error('Accepted root assignment finish does not match.');
    }
  } else {
    await fresh();
    await journal.finishAssignment({assignmentId, status: 'completed'});
  }
  await fresh();
  return {output, attempt, threadId};
}

export function parseClosedJson(text) {
  const input = requireText(text, "Structured workflow output");
  const fenced = input.match(/^```(?:json)?\s*([\s\S]*?)\s*```$/i);
  const source = fenced ? fenced[1] : input;
  let parsed;
  try {
    parsed = JSON.parse(source);
  } catch {
    throw new TypeError("Structured workflow output must be one JSON value.");
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new TypeError("Structured workflow output must be one JSON object.");
  }
  return parsed;
}

async function findAttempt(journal, { runId, assignmentId, attemptId }) {
  const run = await journal.inspectRun({ runId });
  return run.assignments.find(({ assignmentId: id }) => id === assignmentId)
    ?.attempts.find(({ attemptId: id }) => id === attemptId) ?? null;
}

function normalizeRenderedPrompt(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new TypeError("A rendered workflow prompt is required.");
  if (value.toolAssignment !== undefined && !Array.isArray(value.allowedTools)) {
    throw new TypeError('Assigned rendered prompts require an explicit tool selection.');
  }
  const executionProfile = value.executionProfile === undefined
    ? undefined
    : requireText(value.executionProfile, "Rendered workflow execution profile");
  if (executionProfile !== undefined && executionProfile !== BOUNDED_ANALYSIS_EXECUTION_PROFILE) {
    throw new TypeError(`Unsupported structured workflow execution profile: ${executionProfile}.`);
  }
  return {
    text: requireText(value.text, "Rendered workflow prompt text"),
    promptVersionId: requireText(value.promptVersionId, "Rendered workflow prompt version"),
    model: requireText(value.model, "Rendered workflow model"),
    reasoningEffort: requireText(value.reasoningEffort, "Rendered workflow reasoning effort"),
    serviceTier: requireText(value.serviceTier, "Rendered workflow service tier"),
    ...(executionProfile === undefined ? {} : { executionProfile }),
    allowedTools: normalizeTextArray(value.allowedTools ?? [], "Rendered workflow allowed tool"),
    ...(value.toolAssignment === undefined ? {} : { toolAssignment: structuredClone(value.toolAssignment) }),
    workspaceAccess: value.workspaceAccess === "read-only" ? value.workspaceAccess : invalid("Structured workflow steps require read-only workspace access."),
    networkAccess: value.networkAccess === false ? false : invalid("Structured workflow steps must disable direct network access."),
  };
}

function normalizeTextArray(values, label) {
  if (!Array.isArray(values)) throw new TypeError(`${label}s must be an array.`);
  const result = values.map((value) => requireText(value, label));
  if (new Set(result).size !== result.length) throw new TypeError(`${label}s must be unique.`);
  return result;
}

function requireMethod(value, method, label) {
  if (!value || typeof value[method] !== "function") throw new TypeError(`${label}.${method} is required.`);
}

function requireText(value, label) {
  if (typeof value !== "string" || !value.trim()) throw new TypeError(`${label} is required.`);
  return value.trim();
}

function invalid(message) {
  throw new TypeError(message);
}
