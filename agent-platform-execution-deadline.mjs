const messages = Object.freeze({
  invalid_lineage: "Invalid workflow execution root lineage.",
  invalid_timestamps: "Workflow execution deadline requires valid durable timestamps.",
  invalid_requested_deadline: "Workflow requested execution deadline is invalid.",
});

export class WorkflowExecutionDeadlineError extends Error {
  constructor(reason) {
    super(messages[reason]);
    this.name = "WorkflowExecutionDeadlineError";
    this.reason = reason;
  }
}

/** Resolve the oldest durable run anchor without owning a worker timer. */
export async function resolveWorkflowExecutionDeadline({
  journal, run, maxRunDurationMs, requestedDeadlineAt = null, now,
}) {
  let root = run;
  const visited = new Set();
  if (!root || typeof root !== "object") {
    throw new WorkflowExecutionDeadlineError("invalid_lineage");
  }
  while (root.resumedFromRunId) {
    if (visited.has(root.runId) || visited.size >= 100) {
      throw new WorkflowExecutionDeadlineError("invalid_lineage");
    }
    visited.add(root.runId);
    root = await journal.inspectRun({ runId: root.resumedFromRunId });
    if (!root || typeof root !== "object") {
      throw new WorkflowExecutionDeadlineError("invalid_lineage");
    }
  }

  const anchor = Date.parse(root.startedAt ?? root.createdAt);
  const currentTime = Date.parse(now());
  if (!Number.isFinite(anchor) || !Number.isFinite(currentTime)) {
    throw new WorkflowExecutionDeadlineError("invalid_timestamps");
  }
  const configuredDeadline = maxRunDurationMs === null ? null : anchor + maxRunDurationMs;
  const requestedDeadline = requestedDeadlineAt == null ? null : Date.parse(requestedDeadlineAt);
  if (requestedDeadlineAt != null && !Number.isFinite(requestedDeadline)) {
    throw new WorkflowExecutionDeadlineError("invalid_requested_deadline");
  }
  const deadlines = [configuredDeadline, requestedDeadline].filter(Number.isFinite);
  if (deadlines.length === 0) return null;
  const deadlineAtMs = Math.min(...deadlines);
  return { deadlineAtMs, remainingMs: deadlineAtMs - currentTime };
}
