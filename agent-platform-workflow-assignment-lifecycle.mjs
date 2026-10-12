import { createHash } from "node:crypto";

export function createWorkflowAssignmentLifecycle({ journal, projectRun = async () => {} } = {}) {
  for (const method of ["createPreparedAssignment", "observeAssignment", "inspectRun"]) {
    if (typeof journal?.[method] !== "function") {
      throw new TypeError(`Workflow assignment lifecycle requires journal.${method}.`);
    }
  }
  if (typeof projectRun !== "function") throw new TypeError("Workflow run projection must be callable.");

  const queues = new Map();

  function assignmentIdFor({ runId, idempotencyKey } = {}) {
    const id = requireText(runId, "Workflow run id");
    const key = requireText(idempotencyKey, "Assignment idempotency key");
    return `workflow-assignment:${createHash("sha256").update(JSON.stringify([id, key])).digest("hex")}`;
  }

  async function lookupInvocation({ runId, idempotencyKey, requestFingerprint } = {}) {
    const id = requireText(runId, "Workflow run id");
    const assignmentId = assignmentIdFor({ runId: id, idempotencyKey });
    const fingerprint = requireText(requestFingerprint, "Prepared assignment request fingerprint");
    const run = await journal.inspectRun({ runId: id });
    const assignment = run.assignments.find(({ assignmentId: candidate }) => candidate === assignmentId);
    if (!assignment) return null;
    const event = run.events.find(({ sourceKey }) => sourceKey === `assignment-prepared:${assignmentId}`);
    if (!event || event.eventType !== "workflow.assignment-prepared"
      || event.payload?.childAssignmentId !== assignmentId
      || event.assignmentId !== event.payload?.executionRef?.assignmentId
      || event.attemptId !== event.payload?.executionRef?.attemptId) {
      throw new Error(`Assignment ${assignmentId} has no trusted prepared execution.`);
    }
    if (event.payload?.requestFingerprint !== fingerprint) {
      throw new Error(`Assignment ${assignmentId} already exists with different input.`);
    }
    return {
      assignment,
      executionRef: structuredClone(event.payload.executionRef),
      preparedExecution: structuredClone(event.payload.preparedExecution),
    };
  }

  function reconcile({ runId } = {}) {
    const id = requireText(runId, "Workflow run id");
    const previous = queues.get(id) ?? Promise.resolve();
    const task = previous.catch(() => undefined).then(async () => {
      const run = await journal.inspectRun({ runId: id });
      await projectRun({ run: deepFreeze(structuredClone(run)) });
      return run;
    });
    queues.set(id, task);
    void task.finally(() => {
      if (queues.get(id) === task) queues.delete(id);
    }).catch(() => undefined);
    return task;
  }

  return {
    assignmentIdFor,
    lookupInvocation,

    async prepare({ runId, idempotencyKey, ...input } = {}) {
      const id = requireText(runId, "Workflow run id");
      const assignmentId = assignmentIdFor({ runId: id, idempotencyKey });
      const result = await journal.createPreparedAssignment({ assignmentId, runId: id, ...input });
      await reconcile({ runId: id });
      return result;
    },

    async observe({ runId, assignmentId, observationId, status, threadId, message } = {}) {
      const id = requireText(runId, "Workflow run id");
      const assignment = await journal.observeAssignment({
        assignmentId,
        observationId,
        status,
        threadId,
        ...(message == null ? {} : { message }),
      });
      await reconcile({ runId: id });
      return assignment;
    },

    reconcile,
  };
}

function deepFreeze(value) {
  if (value && typeof value === "object" && !Object.isFrozen(value)) {
    Object.freeze(value);
    for (const child of Object.values(value)) deepFreeze(child);
  }
  return value;
}

function requireText(value, label) {
  if (typeof value !== "string" || !value.trim()) throw new TypeError(`${label} is required.`);
  return value.trim();
}
