import { randomUUID } from "node:crypto";
import { claimLostError, createClaimBoundJournal } from "./agent-platform-claim-bound-journal.mjs";
import { resolveWorkflowExecutionDeadline, WorkflowExecutionDeadlineError } from "./agent-platform-execution-deadline.mjs";

const workflowType = "bounded-analysis";
const executionProfile = "bounded-analysis-workflow.v1";

export function createBoundedAnalysisWorkerFactory({
  validateDefinition,
  executeAnalysis,
  createStructuredAgent,
} = {}) {
  if (typeof validateDefinition !== "function") {
    throw new TypeError("Bounded analysis validateDefinition adapter is required.");
  }
  if (typeof executeAnalysis !== "function") {
    throw new TypeError("Bounded analysis executeAnalysis adapter is required.");
  }
  if (typeof createStructuredAgent !== "function") {
    throw new TypeError("Bounded analysis createStructuredAgent adapter is required.");
  }
  const validateBoundedAnalysisDefinition = validateDefinition;
  const executeWorkflowPreparation = executeAnalysis;
  const createStructuredWorkflowAgent = createStructuredAgent;

  /**
   * Execute a small, read-only analysis workflow.
   *
   * This is intentionally a deep module at the worker seam: callers provide the
   * journal, pinned catalog, model adapter, and authorization check; the worker
   * owns claim/recovery, lease heartbeat, deadlines, output validation, and
   * terminalization. It never receives a staging, admission, or dynamic-tool
   * adapter, so adding a new analysis workflow cannot accidentally gain a
   * research mutation path.
   */
  return function createBoundedAnalysisWorker({
    journal,
    codex,
    workflowCatalog,
    structuredAgent: suppliedStructuredAgent,
    createStructuredAgentForRun,
    authorizationRecheck,
    workerId = `bounded-analysis-worker:${process.pid}:${randomUUID()}`,
    leaseSeconds = 120,
    heartbeatMs = 30_000,
    maxRunDurationMs = 600_000,
    prices = [],
    now = () => new Date().toISOString(),
  } = {}) {
    requireMethod(journal, "claimRun", "AgentWorkflowJournal");
    requireMethod(journal, "inspectRun", "AgentWorkflowJournal");
    requireMethod(journal, "createAssignment", "AgentWorkflowJournal");
    requireMethod(journal, "heartbeatRun", "AgentWorkflowJournal");
    requireMethod(journal, "transitionRun", "AgentWorkflowJournal");
    requireMethod(codex, "research", "CodexBridge");
    requireMethod(workflowCatalog, "get", "Bounded analysis catalog");
    requireMethod(workflowCatalog, "render", "Bounded analysis catalog");
    if (typeof authorizationRecheck !== "function") {
      throw new TypeError("Bounded analysis authorization recheck is required.");
    }
    if (suppliedStructuredAgent && createStructuredAgentForRun) {
      throw new TypeError("Supply a structuredAgent or createStructuredAgentForRun, not both.");
    }
    if (createStructuredAgentForRun !== undefined && typeof createStructuredAgentForRun !== "function") {
      throw new TypeError("Bounded analysis createStructuredAgentForRun must be callable.");
    }
    if (!Number.isInteger(leaseSeconds) || leaseSeconds < 30 || leaseSeconds > 3600) {
      throw new TypeError("Bounded analysis leaseSeconds must be from 30 through 3600.");
    }
    if (!Number.isInteger(heartbeatMs) || heartbeatMs < 0 || heartbeatMs >= leaseSeconds * 1000) {
      throw new TypeError("Bounded analysis heartbeatMs must be a non-negative integer shorter than the lease.");
    }
    if (maxRunDurationMs !== null && (!Number.isSafeInteger(maxRunDurationMs)
      || maxRunDurationMs < 1 || maxRunDurationMs > 86_400_000)) {
      throw new TypeError("Bounded analysis maxRunDurationMs must be from 1 through 86400000 or null.");
    }

    // A preconstructed structuredAgent is a trusted compatibility adapter: its
    // private journal closure cannot be rebound. New adapters receive a bound
    // journal through createStructuredAgentForRun after each claim.
    let stopped = false;
    let activeController = null;

    async function runOnce({ signal: parentSignal } = {}) {
      if (activeController) throw new Error("This bounded analysis worker already has an active run.");
      const controller = new AbortController();
      const abort = () => controller.abort(parentSignal?.reason);
      parentSignal?.addEventListener("abort", abort, { once: true });
      if (parentSignal?.aborted) abort();
      activeController = controller;
      let deadline = null;
      try {
        return await executeOnce(controller.signal, controller, async (run) => {
          let resolved;
          try {
            resolved = await resolveWorkflowExecutionDeadline({
              journal, run, maxRunDurationMs,
              requestedDeadlineAt: run.input?.executionDeadlineAt,
              now,
            });
          } catch (error) {
            if (error instanceof WorkflowExecutionDeadlineError) {
              if (error.reason === "invalid_lineage") throw new Error("Invalid bounded analysis root lineage.");
              if (error.reason === "invalid_timestamps") {
                throw new Error("Bounded analysis deadline requires valid durable timestamps.");
              }
              if (error.reason === "invalid_requested_deadline") {
                throw boundedError("INVALID_EXECUTION_DEADLINE", "Bounded analysis executionDeadlineAt is invalid.");
              }
            }
            throw error;
          }
          if (!resolved) return;
          const remaining = resolved.remainingMs;
          const exhaust = () => controller.abort(Object.assign(
            new Error("Bounded analysis execution deadline exceeded."),
            { code: "EXECUTION_DEADLINE_EXCEEDED" },
          ));
          if (remaining <= 0) exhaust();
          else deadline = setTimeout(exhaust, remaining);
        });
      } finally {
        if (deadline) clearTimeout(deadline);
        parentSignal?.removeEventListener("abort", abort);
        activeController = null;
      }
    }

    async function executeOnce(signal, controller, initializeDeadline) {
      signal.throwIfAborted();
      const claimed = await journal.claimRun({ workerId, leaseSeconds, workflowType });
      if (!claimed) return null;
      const expectedClaim = { runId: claimed.runId, workerId, claimedAt: claimed.claim?.claimedAt };
      const boundJournal = createClaimBoundJournal(journal, expectedClaim);
      let run = await boundJournal.inspectRun({ runId: claimed.runId });
      const heartbeat = startRunHeartbeat({ journal, runId: run.runId, workerId,
        claimedAt: expectedClaim.claimedAt, leaseSeconds, heartbeatMs,
        onError: (error) => controller.abort(error?.code === "WORKFLOW_CLAIM_LOST" ? error : Object.assign(
          new Error("Bounded analysis lease heartbeat failed."),
          { code: "LEASE_HEARTBEAT_FAILED", cause: error },
        )) });
      try {
        const structuredAgent = suppliedStructuredAgent ?? await (
          createStructuredAgentForRun
            ? createStructuredAgentForRun({ journal: boundJournal, run, signal })
            : createStructuredWorkflowAgent({ journal: boundJournal, codex, prices, now, run, signal })
        );
        requireMethod(structuredAgent, "run", "Structured workflow agent");
        try {
          await initializeDeadline(run);
          signal.throwIfAborted();
          await workflowCatalog.loadVersion?.(run.workflowVersionId);
          const definition = validateBoundedAnalysisDefinition(workflowCatalog.get(run.workflowVersionId));
          if (definition.workflowType !== workflowType || definition.executionProfile !== executionProfile) {
            throw boundedError("BOUNDED_ANALYSIS_PIN_INVALID", "Pinned workflow is not a bounded-analysis workflow.");
          }
          if (definition.id !== run.workflowVersionId) {
            throw boundedError("BOUNDED_ANALYSIS_PIN_INVALID", "Pinned workflow identity does not match the claimed run.");
          }
        } catch (error) {
          return failRun({ journal: boundJournal, runId: run.runId, signal, error,
            code: error?.code ?? "BOUNDED_ANALYSIS_PIN_INVALID" });
        }

        const definition = validateBoundedAnalysisDefinition(workflowCatalog.get(run.workflowVersionId));
        if (run.owner?.kind === "public") {
          return failRun({ journal: boundJournal, runId: run.runId, signal,
            error: boundedError("OWNER_NOT_AUTHORIZED", "Bounded analysis requires an authenticated or system owner."),
            code: "OWNER_NOT_AUTHORIZED" });
        }
        if (await authorizationRecheck({ phase: "claim", run }) !== true) {
          return failRun({ journal: boundJournal, runId: run.runId, signal,
            error: boundedError("AUTHORIZATION_REVOKED", "Bounded analysis authorization changed before execution."),
            code: "AUTHORIZATION_REVOKED" });
        }

        if (run.recoveryCount > 0) {
          try {
            await reconcileExpiredRun({ journal: boundJournal, codex, run, signal });
          } catch (error) {
            if (error?.code === "WORKFLOW_CLAIM_LOST") throw error;
            throw Object.assign(boundedError("RECOVERY_UNAVAILABLE", "Bounded-analysis recovery could not safely reconcile the previous worker."), { cause: error });
          }
          run = await boundJournal.inspectRun({ runId: run.runId });
        }

        let results;
        try {
          results = await executeWorkflowPreparation({
            run,
            workflowDefinition: definition,
            workflowCatalog,
            journal: boundJournal,
            structuredAgent,
            authorizationRecheck: async (context) => !signal.aborted && await authorizationRecheck(context),
            signal,
          });
        } catch (error) {
          if (error?.code === "WORKFLOW_CLAIM_LOST") throw error;
          return failRun({ journal: boundJournal, runId: run.runId, signal, error,
            code: signal.aborted ? signal.reason?.code ?? "EXECUTION_STOPPED"
              : error?.code ?? "BOUNDED_ANALYSIS_FAILED" });
        }

        const current = await boundJournal.inspectRun({ runId: run.runId });
        assertCompletedResults(current, definition, results);
        if (await authorizationRecheck({ phase: "complete", run: current, results }) !== true) {
          return failRun({ journal: boundJournal, runId: run.runId, signal,
            error: boundedError("AUTHORIZATION_REVOKED", "Bounded analysis authorization changed before completion."),
            code: "AUTHORIZATION_REVOKED" });
        }
        signal.throwIfAborted();
        const finalSummary = {
          schemaVersion: 1,
          kind: "bounded-analysis-completion",
          definitionHash: definition.definitionHash,
          results: results.map(({ stepId, output }) => ({ stepId, output })),
        };
        return await boundJournal.transitionRun({
          runId: run.runId,
          status: "completed",
          definitionHash: definition.definitionHash,
          finalSummary,
        });
      } catch (error) {
        if (["WORKFLOW_CLAIM_LOST", "RECOVERY_UNAVAILABLE"].includes(error?.code)) throw error;
        return failRun({ journal: boundJournal, runId: run.runId, signal, error,
          code: signal.aborted ? signal.reason?.code ?? "EXECUTION_STOPPED"
            : error?.code ?? "BOUNDED_ANALYSIS_FAILED" });
      } finally {
        if (heartbeat) clearInterval(heartbeat);
      }
    }

    async function runForever({ pollIntervalMs = 1_000, signal } = {}) {
      if (!Number.isInteger(pollIntervalMs) || pollIntervalMs < 50 || pollIntervalMs > 60_000) {
        throw new TypeError("Bounded analysis pollIntervalMs must be from 50 through 60000.");
      }
      stopped = false;
      while (!stopped && !signal?.aborted) {
        let run;
        try {
          run = await runOnce({ signal });
        } catch (error) {
          if (error?.code !== "WORKFLOW_CLAIM_LOST") throw error;
          run = null;
        }
        if (!run && !stopped && !signal?.aborted) await wait(pollIntervalMs, signal);
      }
    }

    return {
      workerId,
      runOnce,
      runForever,
      stop() {
        stopped = true;
        activeController?.abort(Object.assign(new Error("Bounded analysis worker stopped."), { code: "EXECUTION_STOPPED" }));
      },
    };
  };
}

async function reconcileExpiredRun({ journal, codex, run, signal }) {
  const open = run.assignments.filter(({ status }) => ["dispatching", "running", "unknown"].includes(status));
  if (!open.length) return;
  const plan = open.map((assignment) => {
    const attempt = [...(assignment.attempts ?? [])].reverse().find(({ outcome }) => outcome === null) ?? null;
    return { assignment, attempt, threadId: attempt?.threadId ?? assignment.agentThreadId ?? null };
  });
  const requiredThreadIds = new Set(plan.map(({ threadId }) => threadId).filter(Boolean));
  const threads = new Map();
  if (requiredThreadIds.size) {
    if (typeof codex.listThreads !== "function") throw boundedError("RECOVERY_UNAVAILABLE", "Cannot reconcile bounded analysis without thread inspection.");
    let cursor = null;
    const seenCursors = new Set();
    for (let pageCount = 0; pageCount < 1_000; pageCount += 1) {
      signal?.throwIfAborted();
      const page = await codex.listThreads({
        limit: 100,
        sourceKinds: ["appServer", "subAgent", "subAgentReview", "subAgentThreadSpawn", "subAgentOther"],
        useStateDbOnly: false,
        ...(cursor ? { cursor } : {}),
      });
      if (!page || typeof page !== "object" || Array.isArray(page) || !Array.isArray(page.data)) {
        throw boundedError("RECOVERY_UNAVAILABLE", "Codex thread listing returned a malformed page.");
      }
      for (const thread of page.data) {
        if (!thread || typeof thread !== "object" || Array.isArray(thread)
          || typeof thread.id !== "string" || !thread.id.trim() || threads.has(thread.id)) {
          throw boundedError("RECOVERY_UNAVAILABLE", "Codex thread listing returned a malformed or duplicate thread.");
        }
        threads.set(thread.id, thread);
      }
      const nextCursor = page.nextCursor ?? null;
      if (nextCursor !== null && (typeof nextCursor !== "string" || !nextCursor.trim()
        || seenCursors.has(nextCursor))) {
        throw boundedError("RECOVERY_UNAVAILABLE", "Codex thread listing returned a malformed or repeated cursor.");
      }
      if ([...requiredThreadIds].every((id) => threads.has(id)) || nextCursor === null) break;
      cursor = nextCursor;
      seenCursors.add(cursor);
      if (pageCount === 999) throw boundedError("RECOVERY_UNAVAILABLE", "Codex thread listing exceeded its page limit.");
    }
  }
  for (const item of plan) {
    item.status = threadStatus(threads.get(item.threadId));
    if (item.status === "unknown") {
      throw boundedError("RECOVERY_UNAVAILABLE", "Codex thread has no reliable recovery status.");
    }
    if (item.status === "active" && (!item.attempt?.turnId || typeof codex.interruptTurn !== "function")) {
      throw boundedError("RECOVERY_UNAVAILABLE", "An active bounded-analysis thread cannot be safely interrupted.");
    }
  }
  const interruptedTurns = new Set();
  for (const { attempt, threadId, status } of plan) {
    signal?.throwIfAborted();
    if (status === "active") {
      const key = JSON.stringify([threadId, attempt.turnId]);
      if (!interruptedTurns.has(key)) {
        await codex.interruptTurn({ threadId, turnId: attempt.turnId });
        interruptedTurns.add(key);
      }
    }
  }
  for (const { assignment, attempt, threadId, status } of plan) {
    signal?.throwIfAborted();
    if (attempt) {
      await journal.finishAttempt({
        attemptId: attempt.attemptId,
        outcome: "interrupted",
        telemetryStatus: "incomplete",
        ...(attempt.resourceReservation == null ? {} : { resourceUsageBasis: "unknown" }),
        result: { reconciliation: { observedStatus: status } },
        error: "Reconciled after the bounded-analysis worker lease expired.",
      });
    }
    await journal.finishAssignment({ assignmentId: assignment.assignmentId, status: "interrupted",
      failure: "Reconciled after the bounded-analysis worker lease expired." });
    await journal.appendEvent?.({
      runId: run.runId,
      assignmentId: assignment.assignmentId,
      ...(attempt ? { attemptId: attempt.attemptId } : {}),
      sourceKey: `bounded-analysis-reconciliation:${run.recoveryCount}:${assignment.assignmentId}`,
      eventType: "workflow.reconciliation",
      threadId: threadId ?? undefined,
      payload: { recoveryCount: run.recoveryCount, observedStatus: status, action: "interrupt-and-retry" },
    });
  }
}

function assertCompletedResults(run, definition, results) {
  if (!Array.isArray(results) || results.length !== definition.steps.length) {
    throw boundedError("BOUNDED_ANALYSIS_OUTPUT_INVALID", "Bounded analysis did not produce one result per pinned step.");
  }
  for (let index = 0; index < results.length; index += 1) {
    if (results[index]?.stepId !== definition.steps[index]?.id || !results[index]?.output) {
      throw boundedError("BOUNDED_ANALYSIS_OUTPUT_INVALID", "Bounded analysis result order does not match its pinned definition.");
    }
  }
  const assignments = run.assignments.filter(({ stepKey }) => stepKey.startsWith(`${workflowType}.`));
  const expectedStepIds = new Set(definition.steps.map(({ id }) => id));
  for (const assignment of assignments) {
    const stepId = assignment.stepKey.slice(`${workflowType}.`.length);
    if (!expectedStepIds.has(stepId)
      || !["completed", "failed", "interrupted"].includes(assignment.status)
      || (assignment.attempts ?? []).some(attempt => attempt.outcome == null)) {
      throw boundedError("BOUNDED_ANALYSIS_OUTPUT_INVALID", "Bounded analysis contains unresolved or undeclared assignment history.");
    }
  }
  for (const step of definition.steps) {
    const completed = assignments.filter(({ stepKey, status }) => stepKey === `${workflowType}.${step.id}` && status === "completed");
    if (completed.length !== 1) {
      throw boundedError("BOUNDED_ANALYSIS_OUTPUT_INVALID", `Bounded analysis step ${step.id} does not have exactly one completed assignment.`);
    }
  }
}

async function failRun({ journal, runId, error, code }) {
  const message = error instanceof Error ? error.message : String(error);
  const current = await journal.inspectRun({ runId });
  if (current && ["running", "validating", "ready-for-admission"].includes(current.status)) {
    await journal.transitionRun({ runId, status: "failed", failure: { code, message } });
  }
  return journal.inspectRun({ runId });
}

function startRunHeartbeat({ journal, runId, workerId, claimedAt, leaseSeconds, heartbeatMs, onError }) {
  if (heartbeatMs <= 0) return null;
  const heartbeat = setInterval(() => {
    void journal.heartbeatRun({ runId, workerId, claimedAt, leaseSeconds })
      .then((claim) => {
        if (!claim) throw claimLostError();
      })
      .catch((error) => onError?.(error));
  }, heartbeatMs);
  heartbeat.unref?.();
  return heartbeat;
}

function threadStatus(thread) {
  if (!thread) return "missing";
  const status = typeof thread.status === "string" ? thread.status : thread.status?.type;
  return ["active", "idle", "systemError"].includes(status) ? status : "unknown";
}

function wait(milliseconds, signal) {
  return new Promise((resolve) => {
    let settled = false;
    const finish = () => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      signal?.removeEventListener?.("abort", finish);
      resolve();
    };
    const timer = setTimeout(finish, milliseconds);
    if (signal?.aborted) finish();
    else signal?.addEventListener?.("abort", finish, { once: true });
  });
}

function boundedError(code, message) {
  return Object.assign(new Error(message), { code });
}

function requireMethod(value, method, label) {
  if (!value || typeof value[method] !== "function") throw new TypeError(`${label}.${method} is required.`);
}
