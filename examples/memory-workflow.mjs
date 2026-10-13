import assert from "node:assert/strict";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { createAgentWorkflowJournalFactory } from "../src/agent-platform/agent-platform-journal.mjs";

// Deterministic components stand in for application-owned tools or agents.
// No provider is called. In-memory state disappears when this process exits.
export async function runMemoryWorkflow() {
  const steps = [
    { id: "normalize", execute: text => text.trim().replace(/\s+/g, " ") },
    { id: "summarize", execute: text => ({ text, wordCount: text.split(" ").length }) },
  ];
  const createJournal = createAgentWorkflowJournalFactory({
    assertCompletion(run, _version, summary) {
      assert.equal(run.assignments.length, steps.length);
      assert.ok(run.assignments.every(assignment => assignment.status === "completed"));
      assert.equal(summary.results.length, steps.length);
    },
    projectMetrics: run => ({ completedSteps: run.assignments.filter(a => a.status === "completed").length }),
    observeJournalEvent: async () => {},
    observeAttemptOutput: async () => {},
    assignmentMarker: id => `EXAMPLE:${id}`,
  });
  const journal = createJournal(); // No database selects the memory implementation.
  const version = await journal.registerVersion({
    id: "text-review.v1", workflowType: "text-review",
    definition: { steps: steps.map(({ id }) => id) },
    codeRevision: "memory-example-v1", changeReason: "Demonstrate reusable component composition.",
    improvementHypothesis: "Retain each step's input and output for inspection.", reviewedBy: "example-host",
  });
  const input = "  Keep   useful information.  ";
  const run = await journal.createRun({
    workflowVersionId: version.id, owner: { kind: "system", principalId: "example-host" }, input: { text: input },
  });
  await journal.claimRun({ workerId: "example-worker", workflowType: version.workflowType });
  let value = input;
  const results = [];
  for (const step of steps) {
    const assignment = await journal.createAssignment({
      runId: run.runId, stepKey: step.id, assignedTo: "example-host", execution: "current-agent",
      targets: [{ type: "text", id: "example-text", expectedVersion: null }],
    });
    const output = step.execute(value);
    await journal.appendEvent({
      runId: run.runId, assignmentId: assignment.assignmentId,
      sourceKey: `step:${step.id}`, eventType: "example.step-completed",
      payload: { step: step.id, input: value, output },
    });
    await journal.finishAssignment({ assignmentId: assignment.assignmentId, status: "completed" });
    results.push({ step: step.id, output });
    value = output;
  }
  await journal.transitionRun({ runId: run.runId, status: "completed", finalSummary: { results } });
  const retained = await journal.inspectRun({ runId: run.runId });
  return {
    status: retained.status, workflowVersion: retained.workflowVersionId,
    originalInput: retained.input, results: retained.finalSummary.results,
    trace: retained.events.map(({ eventType, payload }) => ({ eventType, ...payload })),
    metrics: retained.metrics,
  };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  console.log(JSON.stringify(await runMemoryWorkflow(), null, 2));
}
