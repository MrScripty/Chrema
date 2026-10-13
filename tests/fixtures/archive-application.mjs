import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";

const applicationNamespace = "synthetic.archive";
const recordedAt = "2026-09-23T18:00:00.000Z";
const requester = { kind: "agent", id: "archive-curator-17" };
const destination = { kind: "archive-vault", id: "regional-vault-03", action: "store.archive-record" };
const trustedContext = Object.freeze({ session: "synthetic-archive-session-1" });
const boundedWorkflowId = "synthetic-archive-analysis.v1";
const boundedOutputContract = "synthetic-archive-analysis-output.v1";
const syntheticTestUsageByStep = Object.freeze({
  extract: Object.freeze({ inputTokens: 80, cachedInputTokens: 8, cacheWriteInputTokens: 0,
    outputTokens: 20, reasoningOutputTokens: 5, totalTokens: 100 }),
  summarize: Object.freeze({ inputTokens: 60, cachedInputTokens: 6, cacheWriteInputTokens: 0,
    outputTokens: 20, reasoningOutputTokens: 4, totalTokens: 80 }),
});
const syntheticTestReservationTokens = 300;
const body = JSON.stringify({ archiveRecord: {
  recordCode: "SYNTH-LEASE-0042",
  jurisdiction: "Example District",
  holders: ["Synthetic Cooperative"],
  term: { start: "2040-01-01", end: "2070-12-31" },
  preservation: ["retain-original", "review-restrictions"],
} });

/** Run a dependency-free in-memory smoke path, then PostgreSQL acceptance when a host supplies it. */
export async function runSyntheticArchiveApplication({ agentPlatform, contentLedger, pg = null } = {}) {
  assertAgentPlatform(agentPlatform);
  assertContentLedger(contentLedger);
  if (pg != null) assertAgentPostgresReader(agentPlatform?.postgresReader);
  const memory = Object.freeze({ ...await runMemoryAssignment(agentPlatform),
    revisionTransition: await runMemoryRevisionTransition(agentPlatform),
    tokenBudget: await runMemoryTokenBudget(agentPlatform),
    ...(agentPlatform.workflowReview?.createWorkflowReviewFactory
      ? { workflowReview: await runMemoryWorkflowReview(agentPlatform.workflowReview) } : {}) });
  const boundedAnalysis = await runMemoryBoundedAnalysis(agentPlatform);
  const boundedTokenAccounting = await runMemoryBoundedTokenAccounting(agentPlatform);
  if (pg == null) return Object.freeze({ mode: "memory-only", memory, boundedAnalysis, boundedTokenAccounting });
  assert.equal(typeof pg.Pool, "function", "the host must supply a PostgreSQL Pool constructor");
  assert.equal(typeof pg.databaseAdapter, "function", "the host must supply its PostgreSQL database adapter");
  assert.equal(typeof pg.openRolePool, "function", "the host must supply role-scoped PostgreSQL pools");
  assert.equal(typeof pg.createRole, "function", "the host must supply creation of disposable runtime roles");
  assert.equal(typeof pg.connectionString, "string");
  assert.ok(pg.connectionString.length > 0, "the host must supply the exact disposable PostgreSQL connection string");
  const postgres = await runPostgresAcceptance({
    agentPlatform,
    contentLedger,
    postgresReader: agentPlatform.postgresReader,
    pg,
  });
  return Object.freeze({ mode: "postgres", memory, boundedAnalysis, boundedTokenAccounting, postgres });
}

async function runMemoryAssignment(agentPlatform) {
  const now = tickingClock();
  const factory = makeJournalFactory(agentPlatform, null);
  const journal = factory({ now });
  const workflowVersion = await journal.registerVersion({
    id: "synthetic-archive-review.v1",
    workflowType: "synthetic-archive-review",
    definition: { kind: "archive-review", policy: "preservation-check.v1" },
    codeRevision: "standalone-synthetic-app",
    changeReason: "Exercise the reusable journal from an independent archive application.",
    improvementHypothesis: "Caller-owned workflow policy remains separate from generic persistence.",
    reviewedBy: "archive-fixture-reviewer",
  });
  const run = await journal.createRun({
    runId: "synthetic-memory-archive-run",
    workflowVersionId: workflowVersion.id,
    owner: { kind: "system", principalId: "archive-memory-worker" },
    input: { recordId: "synthetic-memory-record-01", requestedField: "summary" },
  });
  await journal.claimRun({ workerId: "archive-memory-worker", workflowType: workflowVersion.workflowType });
  const caller = await journal.createAssignment({
    assignmentId: "synthetic-memory-caller-assignment",
    runId: run.runId,
    stepKey: "archive-review.inspect-record",
    assignedTo: "archive-curator-17",
    execution: "current-agent",
    threadId: "archive-memory-thread",
    targets: [{ type: "archive-record", id: "synthetic-memory-record-01", expectedVersion: 1 }],
  });
  const callerAttempt = await journal.startAttempt({
    attemptId: "synthetic-memory-caller-attempt",
    assignmentId: caller.assignmentId,
    prompt: "Review the synthetic archive record.",
    requested: { model: "synthetic-model", reasoningEffort: "low" },
    capabilityProfile: { allowedTools: [] },
    threadId: "archive-memory-thread",
    turnId: "archive-memory-turn",
  });
  const lifecycle = agentPlatform.assignmentLifecycle.createWorkflowAssignmentLifecycle({ journal });
  const input = preparedInput(run.runId, caller.assignmentId, callerAttempt.attemptId, "archive-memory-thread");
  const prepared = await lifecycle.prepare(input);
  const replay = await lifecycle.prepare(input);
  assert.equal(prepared.created, true);
  assert.equal(prepared.executionRef.attemptId, callerAttempt.attemptId);
  assert.equal(replay.created, false);
  assert.equal(replay.assignment.assignmentId, prepared.assignment.assignmentId);
  return Object.freeze({
    preparedAssignmentId: prepared.assignment.assignmentId,
    callerAttemptId: prepared.executionRef.attemptId,
    replayed: !replay.created,
  });
}

async function runMemoryBoundedAnalysis(agentPlatform) {
  const journalFactory = makeJournalFactory(agentPlatform, null);
  const journal = journalFactory({ now: tickingClock() });
  const version = await registerBoundedAnalysisVersion(journal);
  const run = await journal.createRun({
    runId: "synthetic-memory-bounded-analysis-run",
    workflowVersionId: version.id,
    owner: { kind: "system", principalId: "synthetic-archive-analysis-worker" },
    input: { documentId: "synthetic-memory-archive-record", archiveText: "Retain the original and review restrictions." },
  });
  const execution = createBoundedAnalysisWorker({
    agentPlatform,
    journal,
    workerId: "synthetic-memory-bounded-worker",
    now: tickingClock(),
  });
  const completed = await execution.worker.runOnce();
  assert.equal(completed.status, "completed");
  assert.equal(completed.runId, run.runId);
  return summarizeBoundedAnalysis(completed, execution.traceEvents);
}

async function runMemoryBoundedTokenAccounting(agentPlatform) {
  const journal = makeJournalFactory(agentPlatform, null)({ now: tickingClock() });
  const version = await registerBoundedAnalysisVersion(journal);
  return runBoundedTokenAccountingAcceptance({
    agentPlatform,
    journal,
    workflowVersionId: version.id,
    suffix: "memory",
    now: tickingClock(),
  });
}

function syntheticRevisionVersion() {
  return {
    id: "synthetic-archive-revision.v1",
    workflowType: "synthetic-archive-revision",
    definition: { kind: "archive-metadata-repair",
      revisionPolicy: { maximumSemanticRevisions: 2,
        correctableIssueCodes: ["ARCHIVE_METADATA_MISSING"] } },
    codeRevision: "standalone-synthetic-revision-v1",
    changeReason: "Exercise an atomic bounded archive revision.",
    improvementHypothesis: "One reviewed metadata repair can retain its exact parent evidence.",
    reviewedBy: "archive-fixture-reviewer",
  };
}

function syntheticRevisionOutcome(label = "normal") {
  return { gate: "archive-metadata-check", verdict: "revision-required",
    manifestHash: sha256(`synthetic-archive-revision:${label}`),
    issues: [{ code: "ARCHIVE_METADATA_MISSING", path: "metadata.recordCode", severity: "blocking" }] };
}

function makeSyntheticRevisionController(agentPlatform, journal) {
  const version = syntheticRevisionVersion();
  return agentPlatform.revisionController.createWorkflowRevisionControllerFactory({
    gates: [{ id: "archive-metadata-check", defaultVerdict: "revision-required",
      requiresRevisionVerdict: false }],
    selectCarryForwardCommands: async () => ["archive-source"],
    carriedCommandId: (childRunId, parentCommandId) => `${childRunId}:${parentCommandId}`,
    continuationThread: run => run.input.threadId ?? null,
  })({
    journal,
    workflowCatalog: { get: id => id === version.id ? version.definition : null },
    authorizationRecheck: async () => true,
  });
}

async function runMemoryRevisionTransition(agentPlatform) {
  const journal = makeJournalFactory(agentPlatform, null)({ now: tickingClock() });
  const version = await journal.registerVersion(syntheticRevisionVersion());
  const parent = await journal.createRun({ runId: "synthetic-memory-revision-parent",
    workflowVersionId: version.id,
    owner: { kind: "system", principalId: "synthetic-archive-revision-worker" },
    input: { archiveId: "synthetic-memory-revision-record", threadId: "archive-memory-revision-thread" },
    runFamilyBudget: { version: 3, maxDescendantRuns: 1, maxTotalAttempts: 2, maxTotalTokens: 1_200 } });
  await journal.transitionRun({ runId: parent.runId, status: "running" });
  await journal.transitionRun({ runId: parent.runId, status: "blocked",
    failure: { code: "ARCHIVE_METADATA_MISSING" } });
  const controller = makeSyntheticRevisionController(agentPlatform, journal);
  const [first, replay] = await Promise.all([
    controller.advance({ runId: parent.runId, outcome: syntheticRevisionOutcome() }),
    controller.advance({ runId: parent.runId, outcome: syntheticRevisionOutcome() }),
  ]);
  const inspectedParent = await journal.inspectRun({ runId: parent.runId });
  const child = await journal.inspectRun({ runId: first.childRunId });
  const childBudget = await journal.inspectRunFamilyBudget({ runId: child.runId });
  assert.equal(first.childRunId, replay.childRunId);
  assert.equal(inspectedParent.events.length, 1);
  assert.equal(inspectedParent.events[0].payload.childRunId, child.runId);
  assert.equal(child.resumedFromRunId, parent.runId);
  assert.equal(child.status, "queued");
  assert.equal(childBudget.rootRunId, parent.runId);
  assert.equal(childBudget.maxTotalTokens, 1_200,
    "the semantic revision child inherits the root's token budget through the installed generic journal");
  return Object.freeze({ childRunId: child.runId, concurrentExactRetry: true,
    exactLineage: true, parentEventCount: inspectedParent.events.length,
    inheritedTokenLimit: childBudget.maxTotalTokens });
}

async function runMemoryTokenBudget(agentPlatform) {
  const journal = makeJournalFactory(agentPlatform, null)({ now: tickingClock() });
  const version = await journal.registerVersion({
    id: "synthetic-archive-token-budget.v1",
    workflowType: "synthetic-archive-token-budget",
    definition: {
      kind: "archive-preservation-review",
      evidencePolicy: { fields: ["recordCode", "retentionEnd"], quoteRequired: true },
      resourcePolicy: { maximumTotalTokens: 1_000, reservationSlackAllowed: true },
    },
    codeRevision: "standalone-synthetic-token-budget",
    changeReason: "Exercise the installed generic journal's family token accounting with archive-owned policy and evidence.",
    improvementHypothesis: "A caller can reserve conservatively and release unused capacity from measured attempt usage.",
    reviewedBy: "synthetic-archive-budget-reviewer",
  });
  const run = await journal.createRun({
    runId: "synthetic-memory-archive-token-budget-run",
    workflowVersionId: version.id,
    owner: { kind: "system", principalId: "synthetic-archive-token-budget-worker" },
    input: {
      recordCode: "SYNTH-RET-317",
      jurisdiction: "Example District",
      retentionEnd: "2068-06-30",
      requestedEvidence: ["recordCode", "retentionEnd"],
    },
    authorization: {
      policy: "archive-preservation-review.v2",
      permittedEvidence: ["recordCode", "retentionEnd"],
      outcomeAuthority: "human-curator",
    },
    runFamilyBudget: {
      version: 3,
      maxDescendantRuns: 0,
      maxTotalAttempts: 2,
      maxTotalTokens: 1_000,
    },
  });
  await journal.claimRun({ workerId: "synthetic-archive-token-budget-worker", workflowType: version.workflowType });
  const assignment = await journal.createAssignment({
    assignmentId: "synthetic-memory-archive-token-budget-assignment",
    runId: run.runId,
    stepKey: "archive-preservation.verify-retention",
    assignedTo: "synthetic-archive-preservation-reviewer",
    execution: "current-agent",
    targets: [{ type: "archive-record", id: "SYNTH-RET-317#retention", expectedVersion: 4 }],
  });
  const attempt = await journal.startAttempt({
    attemptId: "synthetic-memory-archive-token-budget-attempt",
    assignmentId: assignment.assignmentId,
    prompt: "Quote the retention end date and verify it matches the archive record.",
    requested: { model: "synthetic-archive-model", reasoningEffort: "low", serviceTier: "standard" },
    capabilityProfile: {
      allowedTools: [],
      evidencePolicy: "quoted-source-fields-only",
      workspaceAccess: "none",
      networkAccess: "none",
    },
    resourceReservation: { totalTokens: 640 },
  });
  const finish = {
    attemptId: attempt.attemptId,
    outcome: "completed",
    telemetryStatus: "complete",
    resourceUsageBasis: "attempt",
    usage: {
      inputTokens: 260,
      cachedInputTokens: 20,
      cacheWriteInputTokens: 0,
      outputTokens: 130,
      reasoningOutputTokens: 10,
      totalTokens: 390,
    },
    result: {
      evidence: { recordCode: "SYNTH-RET-317", retentionEnd: "2068-06-30" },
      policyDisposition: "ready-for-curator-review",
    },
  };
  await journal.finishAttempt(finish);
  await journal.finishAttempt(finish);
  const familyBudget = await journal.inspectRunFamilyBudget({ runId: run.runId });
  assert.equal(familyBudget.rootRunId, run.runId);
  assert.equal(familyBudget.budgetVersion, 3);
  assert.equal(familyBudget.measuredTokens, 390);
  assert.equal(familyBudget.heldTokens, 0);
  assert.equal(familyBudget.remainingTokens, 610);
  assert.equal(familyBudget.totalAttempts, 1,
    "replaying the same completed attempt does not charge the family a second time");
  return Object.freeze({
    runId: run.runId,
    attemptId: attempt.attemptId,
    measuredTokens: familyBudget.measuredTokens,
    heldTokens: familyBudget.heldTokens,
    remainingTokens: familyBudget.remainingTokens,
    exactFinishReplay: true,
  });
}

async function runMemoryWorkflowReview(workflowReview) {
  const createReview = workflowReview.createWorkflowReviewFactory({
    hashPayload: hashSyntheticArchivePayload,
    renderReviewPrompt: renderSyntheticArchiveReviewPrompt,
    validateVerdict: validateSyntheticArchiveReviewVerdict,
  });
  const approvedFixture = createSyntheticArchiveReviewRuntime({
    runId: "synthetic-memory-archive-review-approved",
    verdict: "approved",
  });
  const approvedReview = createReview({
    ...approvedFixture.dependencies,
    createId: () => "synthetic-archive-review-approved-01",
    now: () => "2026-09-23T18:30:00.000Z",
  });
  const manifest = approvedFixture.manifest;
  const deterministicReport = approvedFixture.deterministicReport;
  const first = await approvedReview.review({ runId: approvedFixture.run.runId, manifest, deterministicReport });
  const callsAfterFirstReview = { model: approvedFixture.calls.model, staging: approvedFixture.calls.staging };
  assert.deepEqual(callsAfterFirstReview, { model: 1, staging: 1 },
    "an absent review invokes the archive reviewer once and loads its staged evidence once");
  const replay = await approvedReview.review({ runId: approvedFixture.run.runId, manifest, deterministicReport });
  assert.equal(first.reviewId, replay.reviewId);
  assert.equal(approvedFixture.calls.model, callsAfterFirstReview.model,
    "an exact review replay returns the immutable result without invoking the structured agent again");
  assert.equal(approvedFixture.calls.staging, callsAfterFirstReview.staging,
    "an exact review replay returns the immutable result without listing staged archive evidence again");
  assert.equal(await approvedReview.requireApproval({
    runId: approvedFixture.run.runId,
    manifestHash: manifest.manifestHash,
  }), true, "approval is bound to the exact reviewed archive manifest");

  const otherManifest = syntheticArchiveReviewManifest(approvedFixture.run.runId, "SYNTH-LEASE-OTHER");
  await assert.rejects(approvedReview.review({
    runId: approvedFixture.run.runId,
    manifest: otherManifest,
    deterministicReport: syntheticArchiveDeterministicReport(otherManifest),
  }), error => error?.code === "WORKFLOW_CONFLICT",
  "the same ready run rejects review of a different manifest");
  await assert.rejects(approvedReview.requireApproval({
    runId: approvedFixture.run.runId,
    manifestHash: otherManifest.manifestHash,
  }), error => error?.code === "WORKFLOW_REVIEW_REQUIRED",
  "an approved review cannot authorize a different manifest hash");

  const reviewerAssignment = approvedFixture.run.assignments.find(({ stepKey }) =>
    stepKey === "synthetic-archive-review.llm-review");
  assert.ok(reviewerAssignment);
  reviewerAssignment.attempts[0].threadId = "synthetic-archive-tampered-reviewer-thread";
  await assert.rejects(approvedReview.requireApproval({
    runId: approvedFixture.run.runId,
    manifestHash: manifest.manifestHash,
  }), error => error?.code === "WORKFLOW_REVIEW_REQUIRED",
  "approval rejects a reviewer attempt whose persisted thread lineage was tampered with");

  const revisionFixture = createSyntheticArchiveReviewRuntime({
    runId: "synthetic-memory-archive-review-revision",
    verdict: "revision-required",
  });
  const revisionReview = createReview({
    ...revisionFixture.dependencies,
    createId: () => "synthetic-archive-review-revision-01",
    now: () => "2026-09-23T18:31:00.000Z",
  });
  const revision = await revisionReview.review({
    runId: revisionFixture.run.runId,
    manifest: revisionFixture.manifest,
    deterministicReport: revisionFixture.deterministicReport,
  });
  assert.equal(revision.verdict, "revision-required");
  assert.equal(revisionFixture.run.status, "blocked");
  await assert.rejects(revisionReview.requireApproval({
    runId: revisionFixture.run.runId,
    manifestHash: revisionFixture.manifest.manifestHash,
  }), error => error?.code === "WORKFLOW_REVIEW_REQUIRED",
  "a revision-required archive review blocks approval");

  return Object.freeze({
    exactManifestApproved: true,
    exactReplayReusedReview: true,
    replayDidNotRerunModel: approvedFixture.calls.model === callsAfterFirstReview.model,
    replayDidNotRelistStaging: approvedFixture.calls.staging === callsAfterFirstReview.staging,
    otherManifestRejected: true,
    lineageTamperingRejected: true,
    revisionRequiredBlocksApproval: true,
  });
}

function createSyntheticArchiveReviewRuntime({ runId, verdict }) {
  const workflow = Object.freeze({
    id: "synthetic-archive-review.v1",
    workflowType: "synthetic-archive-review",
    admissionPolicy: Object.freeze({ llmReviewRequired: true }),
    reviewPolicy: Object.freeze({ requiredEvidence: ["recordCode", "preservation"] }),
  });
  const manifest = syntheticArchiveReviewManifest(runId);
  const run = {
    runId,
    workflowVersionId: workflow.id,
    status: "ready-for-admission",
    validatedManifestHash: manifest.manifestHash,
    assignments: [{
      assignmentId: `${runId}-coordinator-assignment`,
      parentAssignmentId: null,
      stepKey: "synthetic-archive-review.inspect-record",
      execution: "current-agent",
      status: "completed",
      attempts: [{
        assignmentId: `${runId}-coordinator-assignment`,
        attemptId: `${runId}-coordinator-attempt`,
        outcome: "completed",
        telemetryStatus: "complete",
        threadId: "synthetic-archive-coordinator-thread",
      }],
    }],
    events: [],
  };
  const calls = { model: 0, staging: 0, assignment: 0 };
  const stagedCommands = manifest.commands.map(command => ({
    ...command,
    payload: {
      recordCode: "SYNTH-LEASE-0042",
      jurisdiction: "Example District",
      preservation: ["retain-original", "review-restrictions"],
    },
  }));
  const eventBySourceKey = new Map();
  const dependencies = {
    journal: {
      async inspectRun({ runId: requestedRunId }) {
        assert.equal(requestedRunId, runId);
        return run;
      },
      async createAssignment(input) {
        calls.assignment += 1;
        const assignment = {
          ...input,
          assignmentId: `${runId}-review-assignment-${calls.assignment}`,
          parentAssignmentId: null,
          status: "running",
          attempts: [],
        };
        run.assignments.push(assignment);
        return assignment;
      },
      async appendEvent(input) {
        const existing = eventBySourceKey.get(input.sourceKey);
        if (existing) return existing;
        const event = { ...input, eventId: `${runId}-event-${run.events.length + 1}` };
        eventBySourceKey.set(input.sourceKey, event);
        run.events.push(event);
        return event;
      },
      async transitionRun(input) {
        assert.equal(input.runId, runId);
        run.status = input.status;
        run.failure = input.failure ?? null;
        run.finalSummary = input.finalSummary ?? null;
        return run;
      },
    },
    catalog: {
      get(id) { return id === workflow.id ? workflow : null; },
      render(input) { return input; },
    },
    structuredAgent: {
      async run({ assignmentId, forbiddenThreadIds, validateOutput }) {
        calls.model += 1;
        assert.ok(forbiddenThreadIds.includes("synthetic-archive-coordinator-thread"));
        assert.equal(forbiddenThreadIds.includes("synthetic-archive-reviewer-thread"), false);
        const assignment = run.assignments.find(value => value.assignmentId === assignmentId);
        assert.ok(assignment);
        const threadId = "synthetic-archive-reviewer-thread";
        const attemptId = `${runId}-review-attempt-${calls.model}`;
        const output = validateOutput({
          manifestHash: manifest.manifestHash,
          verdict,
          summary: verdict === "approved"
            ? "The synthetic preservation evidence supports the requested archive action."
            : "The synthetic archive record needs a metadata repair before approval.",
          issues: verdict === "approved" ? [] : [{
            code: "ARCHIVE_METADATA_REQUIRES_REVISION",
            path: "archiveRecord.recordCode",
            severity: "blocking",
          }],
        });
        assignment.status = "completed";
        assignment.attempts.push({
          assignmentId,
          attemptId,
          outcome: "completed",
          telemetryStatus: "complete",
          threadId,
        });
        return { output, threadId, attempt: { attemptId } };
      },
    },
    staging: {
      async list({ runId: requestedRunId }) {
        calls.staging += 1;
        assert.equal(requestedRunId, runId);
        return stagedCommands;
      },
    },
  };
  return { run, calls, manifest, deterministicReport: syntheticArchiveDeterministicReport(manifest), dependencies };
}

function syntheticArchiveReviewManifest(runId, recordCode = "SYNTH-LEASE-0042") {
  const payload = {
    recordCode,
    jurisdiction: "Example District",
    preservation: ["retain-original", "review-restrictions"],
  };
  const command = { commandId: `${runId}-archive-review-command`, payloadHash: hashSyntheticArchivePayload(payload) };
  return { runId, manifestHash: hashSyntheticArchivePayload({ runId, commands: [command] }), commands: [command] };
}

function syntheticArchiveDeterministicReport(manifest) {
  return { runId: manifest.runId, manifestHash: manifest.manifestHash, passed: true, issues: [] };
}

function renderSyntheticArchiveReviewPrompt({ workflow, reviewPacket, deterministicReport }) {
  return {
    promptVersionId: "synthetic-archive-preservation-review.v1",
    text: [
      `Apply ${workflow.reviewPolicy.requiredEvidence.join(" and ")} to the staged synthetic archive record.`,
      `Manifest: ${reviewPacket.manifestHash}`,
      `Deterministic report passed: ${deterministicReport.passed}`,
      stableJson(reviewPacket.stagedCommands),
    ].join("\n"),
  };
}

function validateSyntheticArchiveReviewVerdict(output, { manifestHash }) {
  if (!output || typeof output !== "object" || output.manifestHash !== manifestHash
    || !["approved", "revision-required", "rejected"].includes(output.verdict)
    || typeof output.summary !== "string" || !output.summary.trim() || !Array.isArray(output.issues)) {
    throw new TypeError("Synthetic archive review verdict does not match its declared archive policy.");
  }
  if (output.verdict === "approved" && output.issues.length !== 0) {
    throw new TypeError("An approved synthetic archive review cannot contain blocking issues.");
  }
  if (output.verdict === "revision-required"
    && !output.issues.some(issue => issue.code === "ARCHIVE_METADATA_REQUIRES_REVISION")) {
    throw new TypeError("A synthetic archive revision verdict requires its archive metadata issue.");
  }
  return output;
}

function hashSyntheticArchivePayload(value) {
  return sha256(stableJson(value));
}

async function registerBoundedAnalysisVersion(journal) {
  const definition = boundedAnalysisDefinition();
  return journal.registerVersion({
    id: definition.id,
    workflowType: definition.workflowType,
    definition,
    codeRevision: "standalone-synthetic-bounded-analysis",
    changeReason: "Exercise the reusable bounded-analysis worker with an independent synthetic archive application.",
    improvementHypothesis: "Application-owned prompts and output rules can drive the generic claim-bound worker.",
    reviewedBy: "synthetic-analysis-reviewer",
  });
}

function boundedAnalysisDefinition() {
  const identity = {
    id: boundedWorkflowId,
    workflowType: "bounded-analysis",
    executionProfile: "bounded-analysis-workflow.v1",
    steps: ["extract", "summarize"].map(id => ({
      id,
      kind: "read-only-analysis",
      outputContract: boundedOutputContract,
    })),
  };
  return Object.freeze({ ...identity, definitionHash: sha256(stableJson(identity)) });
}

function validateBoundedAnalysisDefinition(value) {
  assert.equal(value?.id, boundedWorkflowId);
  assert.equal(value.workflowType, "bounded-analysis");
  assert.equal(value.executionProfile, "bounded-analysis-workflow.v1");
  assert.deepEqual(value.steps?.map(({ id, kind, outputContract }) => ({ id, kind, outputContract })),
    ["extract", "summarize"].map(id => ({ id, kind: "read-only-analysis", outputContract: boundedOutputContract })));
  assert.match(value.definitionHash, /^[0-9a-f]{64}$/);
  return value;
}

function createBoundedAnalysisWorker({ agentPlatform, journal, workerId, now,
  heartbeatMs = 0, authorizationRecheck = async () => true,
  listThreads = async () => ({ data: [], nextCursor: null }), interruptTurn = async () => {},
  onAgentCreated = async () => {}, onAttemptStarted = async () => {},
  onTransportResult = async () => {}, tokenAccounting = null }) {
  const definition = boundedAnalysisDefinition();
  const traceEvents = [];
  const resourceObservations = [];
  const providerCalls = tokenAccounting?.providerCalls ?? [];
  const codex = {
    async research(input) {
      const previousResults = JSON.parse(input.prompt.previousResultsJson);
      assert.equal(input.runId, input.run.runId);
      assert.equal(input.prompt.executionProfile, "bounded-analysis.v1");
      assert.equal(input.prompt.allowedTools.length, 0);
      assert.equal(input.prompt.workspaceAccess, "read-only");
      assert.equal(input.prompt.networkAccess, false);
      const stepId = input.prompt.stepId;
      if (tokenAccounting) {
        providerCalls.push({ runId: input.runId, stepId });
        if (tokenAccounting.providerFailureStep === stepId) {
          throw Object.assign(new Error("Deterministic synthetic provider failure."), {
            code: "SYNTHETIC_PROVIDER_FAILURE",
          });
        }
      }
      const previousFinding = previousResults[0]?.output.finding ?? null;
      const response = {
        output: Object.freeze({
          schemaVersion: 1,
          documentId: input.run.input.documentId,
          stepId,
          sourceStepCount: previousResults.length,
          finding: stepId === "extract"
            ? `Extracted the Café retention instruction from ${input.run.input.documentId}.`
            : `Summarized the Café archive evidence: ${previousFinding}`,
        }),
        modelExecuted: false,
        qualityEvaluated: false,
      };
      if (tokenAccounting) {
        const usageByStep = Object.hasOwn(tokenAccounting, "usageByStep")
          ? tokenAccounting.usageByStep : syntheticTestUsageByStep;
        const usageScopeByStep = tokenAccounting.usageScopeByStep ?? {};
        const usage = usageByStep?.[stepId];
        response.testTelemetry = Object.freeze({
          ...(usage === undefined ? {} : { usage }),
          usageScope: Object.hasOwn(usageScopeByStep, stepId) ? usageScopeByStep[stepId] : "attempt",
          source: "deterministic-test-telemetry",
        });
      }
      return Object.freeze(response);
    },
    async listThreads(input) { return listThreads(input); },
    async interruptTurn(input) { return interruptTurn(input); },
  };
  const workflowCatalog = {
    async loadVersion(versionId) { assert.equal(versionId, boundedWorkflowId); },
    get(versionId) {
      assert.equal(versionId, boundedWorkflowId);
      return structuredClone(definition);
    },
    render(versionId, stepId, variables) {
      assert.equal(versionId, boundedWorkflowId);
      assert.ok(definition.steps.some(step => step.id === stepId));
      assert.equal(typeof variables.requestJson, "string");
      assert.equal(typeof variables.previousResultsJson, "string");
      return Object.freeze({
        text: `Synthetic archive ${stepId}: ${variables.requestJson}; prior=${variables.previousResultsJson}`,
        promptVersionId: `synthetic-archive-analysis.${stepId}.v1`,
        stepId,
        previousResultsJson: variables.previousResultsJson,
        model: "synthetic-codex-transport",
        reasoningEffort: "low",
        serviceTier: "standard",
        executionProfile: "bounded-analysis.v1",
        allowedTools: Object.freeze([]),
        workspaceAccess: "read-only",
        networkAccess: false,
      });
    },
  };
  const workerFactory = agentPlatform.boundedAnalysisWorker.createBoundedAnalysisWorkerFactory({
    validateDefinition: validateBoundedAnalysisDefinition,
    async executeAnalysis({ run, workflowDefinition, workflowCatalog: catalog, journal: boundJournal,
      structuredAgent, authorizationRecheck: checkAuthorization, signal }) {
      assert.equal(workflowDefinition.id, boundedWorkflowId);
      const results = [];
      for (const step of workflowDefinition.steps) {
        signal?.throwIfAborted();
        const rendered = catalog.render(workflowDefinition.id, step.id, {
          requestJson: JSON.stringify(run.input),
          previousResultsJson: JSON.stringify(results),
        });
        if (await checkAuthorization({ phase: "synthetic-analysis-step", run, step }) !== true) {
          throw Object.assign(new Error("Synthetic archive analysis authorization changed."), {
            code: "AUTHORIZATION_REVOKED",
          });
        }
        const assignment = await boundJournal.createAssignment({
          runId: run.runId,
          stepKey: `bounded-analysis.${step.id}`,
          assignedTo: `synthetic-archive-analysis-${step.id}`,
          execution: "current-agent",
          targets: [{ type: "synthetic-archive-document", id: `${run.input.documentId}:${step.id}`, expectedVersion: 1 }],
        });
        const execution = await structuredAgent.run({
          runId: run.runId,
          assignmentId: assignment.assignmentId,
          prompt: rendered,
          executionProfile: "bounded-analysis.v1",
          validateOutput: value => validateBoundedAnalysisOutput(value, step.id, run.input.documentId),
          signal,
        });
        results.push({ stepId: step.id, output: validateBoundedAnalysisOutput(execution.output, step.id, run.input.documentId) });
      }
      return results;
    },
    createStructuredAgent() {
      throw new Error("The synthetic app must use the per-run claim-bound adapter.");
    },
  });
  const worker = workerFactory({
    journal,
    codex,
    workflowCatalog,
    createStructuredAgentForRun: async ({ journal: boundJournal, run, signal }) => {
      assert.equal(Object.isFrozen(boundJournal), true);
      assert.equal(run.workflowType, "bounded-analysis");
      await onAgentCreated({ journal: boundJournal, run, signal });
      return {
        async run({ runId, assignmentId, prompt, executionProfile, validateOutput, signal: runSignal }) {
          assert.equal(runId, run.runId);
          assert.equal(executionProfile, "bounded-analysis.v1");
          runSignal?.throwIfAborted();
          const attemptId = `synthetic-analysis-attempt-${sha256(`${runId}:${prompt.stepId}`).slice(0, 20)}`;
          const threadId = `synthetic-analysis-thread-${sha256(`${runId}:${prompt.stepId}`).slice(0, 16)}`;
          const turnId = `synthetic-analysis-turn-${sha256(`${runId}:${prompt.stepId}`).slice(0, 16)}`;
          const familyBudget = await boundJournal.inspectRunFamilyBudget({ runId });
          const resourceReservation = familyBudget?.budgetVersion === 3
            ? { totalTokens: tokenAccounting?.reservationTokens ?? syntheticTestReservationTokens }
            : undefined;
          if (familyBudget?.budgetVersion === 3 && !tokenAccounting) {
            throw new Error("The synthetic archive v3 run requires its trusted deterministic test allocator.");
          }
          const startInput = {
            attemptId,
            assignmentId,
            prompt: prompt.text,
            requested: { model: prompt.model, reasoningEffort: prompt.reasoningEffort, serviceTier: prompt.serviceTier },
            capabilityProfile: {
              promptVersionId: prompt.promptVersionId,
              executionProfile,
              allowedTools: [],
              workspaceAccess: prompt.workspaceAccess,
              networkAccess: prompt.networkAccess,
            },
            threadId,
            turnId,
            ...(resourceReservation ? { resourceReservation } : {}),
          };
          let attempt;
          try {
            attempt = await boundJournal.startAttempt(startInput);
          } catch (error) {
            resourceObservations.push({ type: "admission-rejected", runId, stepId: prompt.stepId,
              requestedTokens: resourceReservation?.totalTokens ?? null,
              providerCallsBeforeAdmission: providerCalls.length, code: error?.code ?? null });
            throw error;
          }
          resourceObservations.push({ type: "admitted", runId, stepId: prompt.stepId,
            attemptId: attempt.attemptId,
            reservedTokens: attempt.resourceReservation?.totalTokens ?? null });
          let attemptFinished = false;
          let testTelemetry = null;
          let usage = null;
          let output;
          let finishInput;
          try {
            await onAttemptStarted({ journal: boundJournal, run, assignmentId, attempt, signal: runSignal });
            runSignal?.throwIfAborted();
            await boundJournal.appendEvent({
              runId,
              assignmentId,
              attemptId: attempt.attemptId,
              threadId,
              turnId,
              sourceKey: `synthetic-analysis-transport-input:${attempt.attemptId}`,
              eventType: "synthetic.archive-transport-input",
              payload: { transportInput: prompt.text, modelExecuted: false, qualityEvaluated: false },
            });
            const transport = await codex.research({ runId, run, prompt, signal: runSignal });
            testTelemetry = transport.testTelemetry ?? null;
            const rawUsage = testTelemetry?.usage;
            usage = sanitizeSyntheticUsage(rawUsage);
            assert.equal(transport.modelExecuted, false, "the installed fixture never calls an external model");
            assert.equal(transport.qualityEvaluated, false, "the transport result is not a quality assessment");
            output = validateOutput(transport.output);
            assert.equal(output.finding, transport.output.finding,
              "the validated finding is the exact synthetic transport output leaf");
            await onTransportResult({ journal: boundJournal, run, assignmentId, attempt, prompt,
              transport, output, signal: runSignal });
            const hasMeasuredAttemptUsage = resourceReservation != null
              && testTelemetry?.usageScope === "attempt"
              && hasCompleteSyntheticAttemptUsage(usage);
            finishInput = {
              attemptId: attempt.attemptId,
              outcome: "completed",
              telemetryStatus: hasMeasuredAttemptUsage ? "complete" : "incomplete",
              result: { output, syntheticTransport: { modelExecuted: false, qualityEvaluated: false,
                ...(testTelemetry ? { usageSource: testTelemetry.source } : {}) } },
              ...(resourceReservation ? {
                resourceUsageBasis: hasMeasuredAttemptUsage ? "attempt" : "unknown",
                ...(usage === null ? {} : { usage }),
              } : {}),
            };
            await boundJournal.finishAttempt(finishInput);
            attemptFinished = true;
          } catch (error) {
            if (resourceReservation && !attemptFinished && error?.code !== "WORKFLOW_CLAIM_LOST") {
              const message = error instanceof Error ? error.message : String(error);
              try {
                const persistedRun = await boundJournal.inspectRun({ runId });
                const persistedAttempt = persistedRun.assignments
                  .flatMap(value => value.attempts).find(value => value.attemptId === attempt.attemptId);
                if (persistedAttempt?.outcome == null) {
                  await boundJournal.finishAttempt({
                    attemptId: attempt.attemptId,
                    outcome: "failed",
                    telemetryStatus: "incomplete",
                    resourceUsageBasis: "unknown",
                    ...(usage === null ? {} : { usage }),
                    result: { syntheticTransport: { modelExecuted: false, qualityEvaluated: false,
                      usageSource: "deterministic-test-telemetry", settlement: "post-admission-failure" } },
                    error: message,
                  });
                  await boundJournal.finishAssignment({ assignmentId, status: "failed", failure: message });
                  resourceObservations.push({ type: "settled", runId, stepId: prompt.stepId,
                    attemptId: attempt.attemptId, basis: "unknown", errorCode: error?.code ?? null });
                }
              } catch (settlementError) {
                if (settlementError?.code === "WORKFLOW_CLAIM_LOST") throw settlementError;
                throw new AggregateError([error, settlementError],
                  "The synthetic archive could not settle an admitted token reservation after failure.");
              }
            }
            throw error;
          }
          if (resourceReservation && tokenAccounting?.finishReplayStep === prompt.stepId) {
            await boundJournal.finishAttempt(finishInput);
            const conflictingFinish = {
              ...finishInput,
              usage: { ...finishInput.usage, outputTokens: finishInput.usage.outputTokens + 1,
                totalTokens: finishInput.usage.totalTokens + 1 },
            };
            const replayConflict = await boundJournal.finishAttempt(conflictingFinish)
              .then(() => null, error => error);
            assert.equal(replayConflict?.code, "WORKFLOW_CONFLICT",
              "a conflicting finalization cannot replace the first measured result");
            resourceObservations.push({ type: "replay-verified", runId, stepId: prompt.stepId,
              exactReplay: true, conflictingReplayRejected: true });
          }
          if (resourceReservation) {
            resourceObservations.push({ type: "settled", runId, stepId: prompt.stepId,
              attemptId: attempt.attemptId,
              basis: finishInput.resourceUsageBasis,
              usageSource: testTelemetry?.source ?? null });
          }
          await boundJournal.finishAssignment({ assignmentId, status: "completed" });
          const event = await boundJournal.appendEvent({
            runId,
            assignmentId,
            attemptId: attempt.attemptId,
            sourceKey: `synthetic-analysis-trace:${attempt.attemptId}`,
            eventType: "synthetic.archive-analysis-trace",
            payload: {
              promptVersionId: prompt.promptVersionId,
              modelExecuted: false,
              qualityEvaluated: false,
              outputSha256: sha256(stableJson(output)),
            },
          });
          traceEvents.push({ runId, assignmentId, attemptId: attempt.attemptId, eventId: event.eventId,
            modelExecuted: false, qualityEvaluated: false });
          return { output };
        },
      };
    },
    authorizationRecheck,
    workerId,
    leaseSeconds: 30,
    heartbeatMs,
    maxRunDurationMs: null,
    now: now ?? (() => new Date().toISOString()),
  });
  return { worker, traceEvents, resourceObservations, providerCalls };
}

function hasCompleteSyntheticAttemptUsage(value) {
  return value && typeof value === "object" && !Array.isArray(value)
    && ["inputTokens", "cachedInputTokens", "cacheWriteInputTokens", "outputTokens",
      "reasoningOutputTokens", "totalTokens"].every(field =>
      Number.isSafeInteger(value[field]) && value[field] >= 0);
}

function sanitizeSyntheticUsage(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const usage = Object.fromEntries(["inputTokens", "cachedInputTokens", "cacheWriteInputTokens", "outputTokens",
    "reasoningOutputTokens", "totalTokens"].filter(field =>
    Number.isSafeInteger(value[field]) && value[field] >= 0).map(field => [field, value[field]]));
  if (usage.inputTokens != null
    && (usage.cachedInputTokens ?? 0) + (usage.cacheWriteInputTokens ?? 0) > usage.inputTokens) {
    delete usage.cachedInputTokens;
    delete usage.cacheWriteInputTokens;
  }
  if (usage.outputTokens != null && usage.reasoningOutputTokens != null
    && usage.reasoningOutputTokens > usage.outputTokens) delete usage.reasoningOutputTokens;
  if (usage.totalTokens != null && usage.inputTokens != null && usage.outputTokens != null
    && usage.totalTokens < usage.inputTokens + usage.outputTokens) delete usage.totalTokens;
  return Object.keys(usage).length === 0 ? null : usage;
}

function validateBoundedAnalysisOutput(value, stepId, documentId) {
  assert.deepEqual(Object.keys(value ?? {}).sort(), [
    "documentId", "finding", "schemaVersion", "sourceStepCount", "stepId",
  ]);
  assert.equal(value.schemaVersion, 1);
  assert.equal(value.documentId, documentId);
  assert.equal(value.stepId, stepId);
  assert.equal(typeof value.finding, "string");
  assert.ok(value.finding.length > 0);
  assert.equal(value.sourceStepCount, stepId === "extract" ? 0 : 1,
    "the summary step consumes the separately validated extraction result");
  return value;
}

function summarizeBoundedAnalysis(run, traceEvents) {
  const assignments = run.assignments.filter(value => value.stepKey.startsWith("bounded-analysis."));
  const completedAssignments = assignments.filter(value => value.status === "completed");
  const attemptRecords = assignments.flatMap(assignment => assignment.attempts.map(attempt => ({ assignment, attempt })));
  const completedAttemptRecords = attemptRecords.filter(({ attempt }) => attempt.outcome === "completed");
  const events = run.events.filter(event => event.eventType === "synthetic.archive-analysis-trace");
  const expectedSteps = ["extract", "summarize"];
  const exactLineage = completedAssignments.length === expectedSteps.length
    && completedAssignments.every((assignment, index) => assignment.stepKey === `bounded-analysis.${expectedSteps[index]}`
      && assignment.status === "completed" && assignment.attempts.length === 1
      && assignment.attempts[0].assignmentId === assignment.assignmentId
      && assignment.attempts[0].outcome === "completed"
      && events.filter(event => event.runId === run.runId
        && event.assignmentId === assignment.assignmentId
        && event.attemptId === assignment.attempts[0].attemptId).length === 1)
    && assignments.every(assignment => ["completed", "interrupted"].includes(assignment.status)
      && assignment.attempts.every(attempt => attempt.outcome !== null))
    && events.length === expectedSteps.length
    && events.every(event => event.runId === run.runId
      && completedAssignments.some(assignment => assignment.assignmentId === event.assignmentId
        && assignment.attempts.some(attempt => attempt.attemptId === event.attemptId)));
  const syntheticTransportOnly = completedAttemptRecords.length === expectedSteps.length
    && completedAttemptRecords.every(({ attempt }) => attempt.telemetryStatus === "incomplete"
      && attempt.observed === null && attempt.usage === null
      && attempt.result?.syntheticTransport?.modelExecuted === false
      && attempt.result?.syntheticTransport?.qualityEvaluated === false)
    && events.every(event => event.payload?.modelExecuted === false && event.payload?.qualityEvaluated === false)
    && traceEvents.length === expectedSteps.length
    && traceEvents.every(event => event.modelExecuted === false && event.qualityEvaluated === false);
  assert.equal(exactLineage, true, "every synthetic step has exact run/assignment/attempt/event lineage");
  assert.equal(syntheticTransportOnly, true,
    "synthetic trace evidence stays separate from incomplete model telemetry and quality assessment");
  return Object.freeze({
    runId: run.runId,
    status: run.status,
    stepIds: Object.freeze(completedAssignments.map(assignment => assignment.stepKey.slice("bounded-analysis.".length))),
    assignmentCount: assignments.length,
    attemptCount: attemptRecords.length,
    traceEventCount: events.length,
    exactLineage,
    fakeTransportOnly: syntheticTransportOnly,
  });
}

async function runPostgresAcceptance({ agentPlatform, contentLedger, postgresReader, pg }) {
  const pgNow = () => new Date().toISOString();
  const suffix = randomUUID().replaceAll("-", "").slice(0, 10);
  const schemas = {
    journal: `archive_journal_${suffix}`,
    content: `archive_content_${suffix}`,
    assessment: `archive_assessment_${suffix}`,
    transfer: `archive_transfer_${suffix}`,
    application: `archive_app_${suffix}`,
    archiveReader: `archive_reader_data_${suffix}`,
    rollback: `archive_rollback_${suffix}`,
  };
  const roles = {
    runtime: `archive_runtime_${suffix}`,
    reader: `archive_reader_${suffix}`,
  };
  let ownerPool;
  let runtimePool;
  let readerPool;
  let runtimeDatabase;
  const createdSchemas = [];
  const createdRoles = [];

  try {
    ownerPool = new pg.Pool({ connectionString: pg.connectionString, max: 8,
      application_name: "standalone-synthetic-archive-owner" });
    for (const schema of Object.values(schemas)) await assertSchemaAbsent(ownerPool, schema);
    for (const role of Object.values(roles)) await assertRoleAbsent(ownerPool, role);
    await pg.createRole(ownerPool, roles.runtime);
    createdRoles.push(roles.runtime);
    await pg.createRole(ownerPool, roles.reader);
    createdRoles.push(roles.reader);
    const ownerDatabase = pg.databaseAdapter(ownerPool);
    const { postgresRelations: journalRelations,
      postgresRunFamilyRelations: familyRelations } = await agentPlatform.bootstrap.bootstrapAgentPlatformPostgresWithRunFamilies({
      database: ownerDatabase,
      schema: schemas.journal,
      runtimeRole: roles.runtime,
      readerRole: roles.reader,
    });
    createdSchemas.push(schemas.journal);
    runtimePool = pg.openRolePool(roles.runtime, "standalone-synthetic-archive-runtime");
    readerPool = pg.openRolePool(roles.reader, "standalone-synthetic-archive-reader");
    runtimeDatabase = pg.databaseAdapter(runtimePool);

    const journalFactory = makeJournalFactory(agentPlatform, journalRelations);
    const familyBudgetJournalFactory = makeJournalFactory(agentPlatform, journalRelations, {}, familyRelations);
    let journal = journalFactory({ database: runtimeDatabase, now: pgNow });
    const { run, callerAssignment, callerAttempt } = await seedJournal(journal);
    const lifecycle = agentPlatform.assignmentLifecycle.createWorkflowAssignmentLifecycle({ journal });
    const input = preparedInput(run.runId, callerAssignment.assignmentId, callerAttempt.attemptId, callerAttempt.threadId);
    const prepared = await lifecycle.prepare(input);
    const replay = await lifecycle.prepare(input);
    assert.equal(prepared.created, true);
    assert.deepEqual(prepared.executionRef, input.executionRef,
      "prepared work retains the exact caller-attempt lineage");
    assert.equal(replay.created, false);
    assert.equal(replay.assignment.assignmentId, prepared.assignment.assignmentId);

    const contentRelations = await contentLedger.capture.bootstrapContentLedgerPostgres({
      database: ownerDatabase,
      schema: schemas.content,
      runtimeRole: roles.runtime,
      readerRole: roles.reader,
      journalRelations: { runs: journalRelations.runs, attempts: journalRelations.attempts },
    });
    createdSchemas.push(schemas.content);
    const assessmentRelations = await contentLedger.assessments.bootstrapContentAssessmentPostgres({
      database: ownerDatabase,
      schema: schemas.assessment,
      runtimeRole: roles.runtime,
      readerRole: roles.reader,
      contentVersions: contentRelations.contentVersions,
    });
    createdSchemas.push(schemas.assessment);
    await createApplicationSchema(ownerPool, schemas.application, contentRelations.contentVersions,
      roles.runtime, () => createdSchemas.push(schemas.application));
    const transferBootstrap = await contentLedger.transfers.bootstrapContentTransferDeliveryPostgres({
      database: ownerDatabase,
      schema: schemas.transfer,
      runtimeRole: roles.runtime,
      readerRole: roles.reader,
      contentVersions: contentRelations.contentVersions,
    });
    createdSchemas.push(schemas.transfer);
    const transferRelations = transferBootstrap.transferRelations;
    const deliveryAttempts = transferBootstrap.deliveryAttempts;
    const captureWriter = contentLedger.capture.createPostgresContentLedgerWriter({
      relations: {
        extensionDefinitions: contentRelations.extensionDefinitions,
        contentVersions: contentRelations.contentVersions,
        observedUsage: contentRelations.observedUsage,
      },
      schemaProfile: contentLedger.capture.CONTENT_LEDGER_POSTGRES_SCHEMA_PROFILE,
    });
    const ledgerHooks = makeExecutionLedgerHooks(contentLedger, captureWriter);
    const ledgerJournalFactory = makeJournalFactory(agentPlatform, journalRelations, ledgerHooks);
    journal = ledgerJournalFactory({ database: runtimeDatabase, now: pgNow });
    const importEvent = await journal.appendEvent({
      runId: run.runId,
      assignmentId: callerAssignment.assignmentId,
      attemptId: callerAttempt.attemptId,
      threadId: callerAttempt.threadId,
      turnId: callerAttempt.turnId,
      sourceKey: "synthetic-archive-import-response:archive-event-0042",
      eventType: "synthetic.archive-import-response",
      payload: { archiveDocument: body, modelExecuted: false, qualityEvaluated: false },
    });

    await assertBootstrapRollback({ ownerPool, ownerDatabase, contentLedger, schemas, roles, contentRelations });
    await assertPrivileges({ ownerPool, runtimePool, readerPool, roles, journalRelations,
      contentRelations, assessmentRelations, transferRelations, deliveryAttempts,
      applicationSchema: schemas.application });

    const tokenBudget = await runPostgresTokenBudgetAcceptance({
      journalFactory: familyBudgetJournalFactory,
      runtimeDatabase,
      ownerPool,
      familyRelations,
      suffix,
      now: pgNow,
    });

    const boundedAnalysis = await runBoundedPostgresAcceptance({ agentPlatform, journal,
      journalFactory: ledgerJournalFactory,
      budgetJournalFactory: familyBudgetJournalFactory,
      runtimeDatabase, runtimePool, ownerPool, journalRelations, contentRelations,
      runtimeRole: roles.runtime, suffix, now: pgNow });

    await createArchiveReaderSchema(ownerPool, schemas.archiveReader, roles.reader,
      () => createdSchemas.push(schemas.archiveReader));
    const postgresReaderAcceptance = await runArchivePostgresReaderAcceptance({
      postgresReader,
      database: pg.databaseAdapter(readerPool),
      schema: schemas.archiveReader,
    });

    // Reconnect with a fresh journal/lifecycle instance before content writes.
    await runtimePool.end();
    runtimePool = pg.openRolePool(roles.runtime, "standalone-synthetic-archive-recovered-runtime");
    runtimeDatabase = pg.databaseAdapter(runtimePool);
    journal = ledgerJournalFactory({ database: runtimeDatabase, now: pgNow });
    const recoveredLifecycle = agentPlatform.assignmentLifecycle.createWorkflowAssignmentLifecycle({ journal });
    const recovered = await recoveredLifecycle.lookupInvocation({
      runId: run.runId,
      idempotencyKey: input.idempotencyKey,
      requestFingerprint: input.requestFingerprint,
    });
    assert.equal(recovered.assignment.assignmentId, prepared.assignment.assignmentId);
    assert.deepEqual(recovered.executionRef, input.executionRef);
    assert.deepEqual(recovered.preparedExecution, input.preparedExecution);
    const recoveredReplay = await recoveredLifecycle.prepare(input);
    assert.equal(recoveredReplay.created, false);
    assert.equal(recoveredReplay.assignment.assignmentId, prepared.assignment.assignmentId);

    const executionEvidence = await inspectNormalExecutionLedger({ ownerPool, relations: contentRelations,
      journal, normalRunId: boundedAnalysis.runId, importRunId: run.runId, importEvent,
      callerAssignment, callerAttempt });
    const outputContent = { versionId: executionEvidence.summarizeOutput.version_id,
      normalized: { usageId: executionEvidence.summarizeOutput.usage_id } };
    const observerRollback = await assertObserverRollback({ agentPlatform, journalRelations,
      contentRelations, runtimeDatabase, ownerPool, ledgerHooks, suffix, now: pgNow });
    await ownerPool.query(`INSERT INTO ${q(schemas.application)}.archive_payloads
      (application_namespace,record_id,content_version_id,body_text)
      VALUES ($1,$2,$3,$4)`, [applicationNamespace, executionEvidence.summarizeAttempt.attemptId,
      outputContent.versionId, executionEvidence.summarizeFinding]);
    await ownerPool.query(`INSERT INTO ${q(schemas.application)}.recipient_permissions
      (permission_id,principal_kind,principal_id,destination_kind,destination_id,destination_action,enabled,authority_version)
      VALUES ('archive-grant-0042',$1,$2,$3,$4,$5,true,7)`,
    [requester.kind, requester.id, destination.kind, destination.id, destination.action]);

    const relationship = contentLedger.relationships.normalizeContentRelationship({
      applicationNamespace,
      relationshipType: "analysis-finding-consumed-by-summary",
      relationshipSchemaVersion: 1,
      fromVersionId: executionEvidence.extractOutput.version_id,
      toVersionId: outputContent.versionId,
      sourceReference: { kind: "workflow-attempt-result", id: executionEvidence.extractAttempt.attemptId },
      extensions: {},
    }, { recordedAt, schemaProfile: contentLedger.capture.CONTENT_LEDGER_POSTGRES_SCHEMA_PROFILE });
    const relationshipWriter = contentLedger.relationships.createPostgresContentRelationshipWriter({
      relations: { relationships: contentRelations.relationships },
      schemaProfile: contentLedger.capture.CONTENT_LEDGER_POSTGRES_SCHEMA_PROFILE,
    });
    await runtimeDatabase.transaction(client => relationshipWriter.insertContentRelationships(client, [relationship]));

    const usageBeforeAssessments = await snapshotRows(ownerPool, [contentRelations.observedUsage]);
    const assessment = await recordContradictoryAssessments({ contentLedger, runtimeDatabase, ownerDatabase,
      relations: assessmentRelations, contentVersionId: outputContent.versionId });
    assert.deepEqual(await snapshotRows(ownerPool, [contentRelations.observedUsage]), usageBeforeAssessments,
      "contradictory assessments do not create or change observed usage");

    const transfer = await seedEligibleTransfer({ contentLedger, runtimeDatabase, ownerDatabase,
      relations: transferRelations, contentVersionId: outputContent.versionId,
      summarizeAttempt: executionEvidence.summarizeAttempt,
      summarizeOutput: executionEvidence.summarizeOutput,
      summarizeFinding: executionEvidence.summarizeFinding });
    const decisionRow = await ownerPool.query(`SELECT outcome,delivery_enabled FROM ${relation(transferRelations.decisions)}
      WHERE id=$1`, [transfer.decision.decisionId]);
    assert.deepEqual(decisionRow.rows[0], { outcome: "eligible", delivery_enabled: false },
      "an eligible transfer decision does not activate delivery");
    const usageAfterTransfer = await snapshotRows(ownerPool, [contentRelations.observedUsage]);
    assert.deepEqual(usageAfterTransfer, usageBeforeAssessments,
      "transfer evidence also remains separate from observed usage");

    const delivery = await exerciseDelivery({ contentLedger, runtimeDatabase, ownerPool, runtimePool,
      relations: transferRelations, deliveryAttempts, request: transfer.request, decision: transfer.decision,
      applicationSchema: schemas.application, runtimeRole: roles.runtime,
      summarizeAttemptId: executionEvidence.summarizeAttempt.attemptId,
      expectedFinding: executionEvidence.summarizeFinding });
    await assertImmutableRows({ ownerPool, contentRelations, assessmentRelations,
      transferRelations, deliveryAttempts, firstContent: outputContent, assessment, delivery });
    const storedPayload = await ownerPool.query(`SELECT body_text FROM ${q(schemas.application)}.archive_payloads
      WHERE application_namespace=$1 AND record_id=$2 AND content_version_id=$3`,
    [applicationNamespace, executionEvidence.summarizeAttempt.attemptId, outputContent.versionId]);
    const deliveredReceipt = await ownerPool.query(`SELECT body_sha256 FROM ${q(schemas.application)}.local_delivery_receipts
      WHERE content_version_id=$1`, [outputContent.versionId]);
    const deliveryByteIdentical = storedPayload.rows[0]?.body_text === executionEvidence.summarizeFinding
      && sha256(storedPayload.rows[0].body_text) === executionEvidence.summarizeOutput.content_sha256
      && Buffer.byteLength(storedPayload.rows[0].body_text, "utf8") === Number(executionEvidence.summarizeOutput.byte_length)
      && deliveredReceipt.rows[0]?.body_sha256 === executionEvidence.summarizeOutput.content_sha256;
    assert.equal(deliveryByteIdentical, true, "delivery stores the selected finding leaf with identical UTF-8 bytes");
    const revisionTransition = await runPostgresRevisionTransition({ agentPlatform, runtimeDatabase,
      ownerPool, journalRelations, familyRelations, applicationSchema: schemas.application,
      runtimeRole: roles.runtime, suffix, now: pgNow });

    return Object.freeze({
      applicationNamespace,
      schemas: Object.freeze(schemas),
      preparedAssignmentId: prepared.assignment.assignmentId,
      callerAttemptId: callerAttempt.attemptId,
      boundedAnalysis,
      postgresReader: postgresReaderAcceptance,
      revisionTransition,
      tokenBudget,
      contentVersionIds: executionEvidence.primaryVersionIds,
      executionLedger: Object.freeze({
        importResponseCount: executionEvidence.importResponseCount,
        transportInputCount: executionEvidence.transportInputCount,
        producedOutputCount: executionEvidence.producedOutputCount,
        exactLineage: executionEvidence.exactLineage,
        eventReplayIdempotent: executionEvidence.eventReplayIdempotent,
        finishReplayIdempotent: executionEvidence.finishReplayIdempotent,
        observerRollback,
        claimLossNoOutputCapture: boundedAnalysis.staleEpoch.producedOutputCount === 0,
        assessmentUsageUnchanged: true,
        summarizeAttemptId: executionEvidence.summarizeAttempt.attemptId,
        delivery: Object.freeze({ sourceKind: executionEvidence.summarizeOutput.source_kind,
          sourceRecordId: executionEvidence.summarizeOutput.source_record_id,
          sourceField: executionEvidence.summarizeOutput.source_field,
          sha256: executionEvidence.summarizeOutput.content_sha256,
          byteLength: Number(executionEvidence.summarizeOutput.byte_length),
          byteIdentical: deliveryByteIdentical }),
      }),
      relationshipId: relationship.relationshipId,
      contradictoryAssessmentIds: Object.freeze(assessment.assessments.map(value => value.assessmentId)),
      decisionId: transfer.decision.decisionId,
      deliveryOutcomeIds: Object.freeze(delivery.outcomeIds),
      deliveredReceiptCount: delivery.receiptCount,
      retriesRecovered: delivery.retriesRecovered,
    });
  } finally {
    const cleanupErrors = [];
    for (const pool of [readerPool, runtimePool]) {
      try { await pool?.end(); } catch (error) { cleanupErrors.push(error); }
    }
    if (ownerPool) {
      for (const schema of createdSchemas.reverse()) {
        try { await ownerPool.query(`DROP SCHEMA ${q(schema)} CASCADE`); }
        catch (error) { cleanupErrors.push(error); }
      }
      for (const role of createdRoles.reverse()) {
        try { await ownerPool.query(`DROP ROLE ${q(role)}`); }
        catch (error) { cleanupErrors.push(error); }
      }
      try { await ownerPool.end(); } catch (error) { cleanupErrors.push(error); }
    }
    if (cleanupErrors.length) throw new AggregateError(cleanupErrors, "Synthetic archive PostgreSQL cleanup failed.");
  }
}

async function runArchivePostgresReaderAcceptance({ postgresReader, database, schema }) {
  const contextFor = principalId => async () => ({ principalId, policyVersion: 3 });
  const createReader = principalId => postgresReader.createAgentPostgresReaderFactory({
    issueReadContext: contextFor(principalId),
    rowOmissionMarker: "$archive",
    async configureTransaction(client, readContext) {
      await client.query(`SET LOCAL search_path=pg_catalog,${q(schema)}`);
      await client.query("SELECT pg_catalog.set_config('archive.reader_principal',$1,true)", [readContext.principalId]);
    },
    validateReadContext(current, initial) {
      if (current?.principalId !== initial?.principalId
        || current?.policyVersion !== initial?.policyVersion) {
        throw new Error("Synthetic archive disclosure policy changed during the read.");
      }
    },
  })({ database, maxRows: 10 });

  const approvedReader = createReader("archive-curator");
  const accepted = await approvedReader.query({
    sql: "SELECT id,payload,classification FROM archive_documents ORDER BY id",
  });
  const omission = accepted.rows.find(row => row?.$archive === "row-omitted");
  assert.ok(omission, "the installed archive reader marks the oversized archive document");
  assert.equal(omission.reason, "row-byte-limit");
  assert.ok(omission.bytes >= 20_000);
  const visibleRecordIds = accepted.rows.filter(row => typeof row?.id === "string").map(row => row.id);
  assert.deepEqual(visibleRecordIds, ["approved"]);
  assert.equal(accepted.rows.some(row => row?.id === "restricted"), false,
    "the archive-owned row policy withholds restricted data");
  assert.equal(accepted.truncated, true);
  assert.deepEqual(accepted.truncationReasons, ["row-byte-limit"]);
  assert.equal(accepted.oversizedRowCount, 1);

  const denied = await createReader("archive-visitor").query({
    sql: "SELECT id FROM archive_documents ORDER BY id",
  });
  assert.deepEqual(denied.rows, [], "an archive visitor without the curator policy sees no records");

  return Object.freeze({
    contextPrincipal: "archive-curator",
    policyVersion: 3,
    visibleRecordIds: Object.freeze(visibleRecordIds),
    restrictedRecordExcluded: true,
    omissionMarker: omission.$archive,
    omissionReason: omission.reason,
    omittedBytes: omission.bytes,
    returnedRowCount: accepted.returnedRowCount,
    truncated: accepted.truncated,
    truncationReasons: accepted.truncationReasons,
    oversizedRowCount: accepted.oversizedRowCount,
    deniedPrincipalRowCount: denied.returnedRowCount,
  });
}

async function runPostgresTokenBudgetAcceptance({ journalFactory, runtimeDatabase, ownerPool,
  familyRelations, suffix, now }) {
  const journal = journalFactory({ database: runtimeDatabase, now });
  const workflowType = `synthetic-archive-token-budget-${suffix}`;
  const version = await journal.registerVersion({
    id: `synthetic-archive-token-budget.v3.${suffix}`,
    workflowType,
    definition: {
      kind: "archive-retention-review",
      evidencePolicy: { requiredFields: ["recordCode", "retentionEnd"], quoteRequired: true },
      resourcePolicy: { familyTokenLimit: 1_000, unresolvedTelemetryKeepsReservation: true },
    },
    codeRevision: "standalone-synthetic-archive-token-budget-v3",
    changeReason: "Exercise v3 family token accounting through the installed generic journal export.",
    improvementHypothesis: "Measured archive review usage releases slack while unknown usage remains reserved.",
    reviewedBy: "synthetic-archive-token-budget-reviewer",
  });
  const run = await journal.createRun({
    runId: `synthetic-archive-token-budget-${suffix}`,
    workflowVersionId: version.id,
    owner: { kind: "system", principalId: `synthetic-archive-curator-${suffix}` },
    input: {
      recordCode: "SYNTH-RET-317",
      jurisdiction: "Example District",
      retentionEnd: "2068-06-30",
      requestedEvidence: ["recordCode", "retentionEnd"],
    },
    authorization: {
      policy: "archive-preservation-review.v3",
      permittedEvidence: ["recordCode", "retentionEnd"],
      decisionAuthority: "synthetic-human-curator",
    },
    runFamilyBudget: {
      version: 3,
      maxDescendantRuns: 0,
      maxTotalAttempts: 2,
      maxTotalTokens: 1_000,
    },
  });
  const workerId = `synthetic-archive-budget-worker-${suffix}`;
  const claimed = await journal.claimRun({ workerId, workflowType });
  assert.equal(claimed?.runId, run.runId);
  const expectedClaim = { runId: run.runId, workerId, claimedAt: claimed.claim.claimedAt };
  const measuredAssignment = await journal.createAssignment({
    assignmentId: `synthetic-archive-budget-measured-assignment-${suffix}`,
    runId: run.runId,
    stepKey: "archive-review.verify-retention",
    assignedTo: `synthetic-archive-curator-${suffix}`,
    execution: "current-agent",
    targets: [{ type: "archive-record", id: "SYNTH-RET-317#retention", expectedVersion: 4 }],
    expectedClaim,
  });
  const measuredAttempt = await journal.startAttempt({
    attemptId: `synthetic-archive-budget-measured-attempt-${suffix}`,
    assignmentId: measuredAssignment.assignmentId,
    prompt: "Quote the retention end date from the synthetic archive record.",
    requested: { model: "synthetic-archive-model", reasoningEffort: "low", serviceTier: "standard" },
    capabilityProfile: {
      allowedTools: [],
      evidencePolicy: "quoted-source-fields-only",
      workspaceAccess: "none",
      networkAccess: "none",
    },
    expectedClaim,
    resourceReservation: { totalTokens: 640 },
  });
  const measuredFinish = {
    attemptId: measuredAttempt.attemptId,
    outcome: "completed",
    telemetryStatus: "complete",
    resourceUsageBasis: "attempt",
    usage: {
      inputTokens: 260,
      cachedInputTokens: 20,
      cacheWriteInputTokens: 0,
      outputTokens: 130,
      reasoningOutputTokens: 10,
      totalTokens: 390,
    },
    result: {
      evidence: { recordCode: "SYNTH-RET-317", retentionEnd: "2068-06-30" },
      disposition: "ready-for-curator-review",
    },
    expectedClaim,
  };
  await journal.finishAttempt(measuredFinish);
  const afterMeasuredFinish = await journal.inspectRunFamilyBudget({ runId: run.runId });
  assert.equal(afterMeasuredFinish.measuredTokens, 390);
  assert.equal(afterMeasuredFinish.heldTokens, 0);
  assert.equal(afterMeasuredFinish.remainingTokens, 610);
  assert.equal(afterMeasuredFinish.unresolvedAttempts, 0);
  await journal.finishAttempt(measuredFinish);
  const afterExactReplay = await journal.inspectRunFamilyBudget({ runId: run.runId });
  assert.deepEqual(afterExactReplay, afterMeasuredFinish,
    "the packaged generic journal's exact completion replay does not charge archive usage twice");

  const unknownAssignment = await journal.createAssignment({
    assignmentId: `synthetic-archive-budget-unknown-assignment-${suffix}`,
    runId: run.runId,
    stepKey: "archive-review.reconcile-lost-usage",
    assignedTo: `synthetic-archive-curator-${suffix}`,
    execution: "current-agent",
    targets: [{ type: "archive-record", id: "SYNTH-RET-317#access", expectedVersion: 4 }],
    expectedClaim,
  });
  const unknownAttempt = await journal.startAttempt({
    attemptId: `synthetic-archive-budget-unknown-attempt-${suffix}`,
    assignmentId: unknownAssignment.assignmentId,
    prompt: "Reconcile an archive response whose final usage record was lost.",
    requested: { model: "synthetic-archive-model", reasoningEffort: "low", serviceTier: "standard" },
    capabilityProfile: {
      allowedTools: [],
      evidencePolicy: "quoted-source-fields-only",
      workspaceAccess: "none",
      networkAccess: "none",
    },
    expectedClaim,
    resourceReservation: { totalTokens: 300 },
  });
  await journal.finishAttempt({
    attemptId: unknownAttempt.attemptId,
    outcome: "interrupted",
    telemetryStatus: "incomplete",
    resourceUsageBasis: "unknown",
    usage: { inputTokens: 3 },
    result: { transport: "synthetic-archive-connection-lost-after-response" },
    expectedClaim,
  });
  const familyBudget = await journal.inspectRunFamilyBudget({ runId: run.runId });
  assert.equal(familyBudget.measuredTokens, 390);
  assert.equal(familyBudget.heldTokens, 300);
  assert.equal(familyBudget.remainingTokens, 310);
  assert.equal(familyBudget.unresolvedAttempts, 1);
  assert.equal(familyBudget.totalAttempts, 2);

  const familyRow = (await ownerPool.query(`
    SELECT budget_version::integer AS budget_version,
           max_descendant_runs::integer AS max_descendant_runs,
           max_total_attempts::integer AS max_total_attempts,
           max_total_tokens::integer AS max_total_tokens
    FROM ${relation(familyRelations.families)} WHERE root_run_id=$1
  `, [run.runId])).rows[0];
  assert.deepEqual(familyRow, {
    budget_version: 3,
    max_descendant_runs: 0,
    max_total_attempts: 2,
    max_total_tokens: 1_000,
  }, "the installed generic journal persists the synthetic archive v3 policy in its configured family relation");
  return Object.freeze({
    budgetVersion: familyBudget.budgetVersion,
    maxTotalTokens: familyBudget.maxTotalTokens,
    measuredTokens: familyBudget.measuredTokens,
    heldTokens: familyBudget.heldTokens,
    remainingTokens: familyBudget.remainingTokens,
    unresolvedAttempts: familyBudget.unresolvedAttempts,
    totalAttempts: familyBudget.totalAttempts,
    exactFinishReplay: true,
  });
}

async function runBoundedTokenAccountingAcceptance({ agentPlatform, journal,
  workflowVersionId, suffix, now }) {
  const createWorkerRun = async (label, maxTotalTokens, input = {}, maxTotalAttempts = 4) => {
    const run = await createBoundedRun(journal, workflowVersionId, `${label}-${suffix}`, input, {
      version: 3,
      maxDescendantRuns: 0,
      maxTotalAttempts,
      maxTotalTokens,
    });
    return run;
  };
  const execute = async (run, label, tokenAccounting, workerOptions = {}) => {
    const execution = createBoundedAnalysisWorker({ agentPlatform, journal,
      workerId: `synthetic-budgeted-${label}-${suffix}`, now, ...workerOptions, tokenAccounting });
    const result = await execution.worker.runOnce();
    const persistedRun = await journal.inspectRun({ runId: run.runId });
    const budget = await journal.inspectRunFamilyBudget({ runId: run.runId });
    return { execution, result, persistedRun, budget };
  };

  const completedRun = await createWorkerRun("budget-complete", 1_000,
    { requestedReservationTokens: 900_000 });
  const completed = await execute(completedRun, "complete", {
    reservationTokens: syntheticTestReservationTokens,
    finishReplayStep: "extract",
  });
  const completedAttempts = completed.persistedRun.assignments.flatMap(assignment => assignment.attempts);
  assert.equal(completed.result.status, "completed", JSON.stringify(completed.result.failure ?? null));
  assert.deepEqual(completedAttempts.map(attempt => attempt.resourceUsageBasis), ["attempt", "attempt"]);
  assert.deepEqual(completedAttempts.map(attempt => attempt.usage.totalTokens), [100, 80]);
  assert.equal(completed.budget.measuredTokens, 180);
  assert.equal(completed.budget.heldTokens, 0, "measured attempts release unused reservation slack");
  assert.equal(completed.budget.remainingTokens, 820);
  assert.equal(completed.budget.totalAttempts, 2);
  assert.deepEqual(completed.execution.resourceObservations.filter(event => event.type === "admitted")
    .map(event => event.reservedTokens), [syntheticTestReservationTokens, syntheticTestReservationTokens],
  "the adapter's fixed reservation ignores the untrusted requestedReservationTokens run input");
  assert.deepEqual(completed.execution.resourceObservations.find(event => event.type === "replay-verified"), {
    type: "replay-verified", runId: completedRun.runId, stepId: "extract",
    exactReplay: true, conflictingReplayRejected: true,
  });

  const unknownRun = await createWorkerRun("budget-unknown", 600);
  const unknown = await execute(unknownRun, "unknown", {
    reservationTokens: syntheticTestReservationTokens,
    usageByStep: {
      extract: { inputTokens: 40, totalTokens: 40 },
      summarize: syntheticTestUsageByStep.summarize,
    },
    usageScopeByStep: { summarize: "workflow" },
  });
  const unknownAttempts = unknown.persistedRun.assignments.flatMap(assignment => assignment.attempts);
  assert.equal(unknown.result.status, "completed");
  assert.deepEqual(unknownAttempts.map(attempt => attempt.resourceUsageBasis), ["unknown", "unknown"]);
  assert.equal(unknownAttempts[0].usage.inputTokens, 40);
  assert.equal(unknownAttempts[0].usage.outputTokens, null,
    "partial deterministic telemetry remains explicitly unknown at omitted counters");
  assert.equal(unknown.budget.measuredTokens, 0);
  assert.equal(unknown.budget.heldTokens, 600, "partial or non-attempt telemetry keeps each reservation held");
  assert.equal(unknown.budget.unresolvedAttempts, 2);

  const providerFailureRun = await createWorkerRun("budget-provider-failure", 500, {}, 2);
  const providerFailure = await execute(providerFailureRun, "provider-failure", {
    reservationTokens: syntheticTestReservationTokens,
    providerFailureStep: "extract",
  });
  const failedAttempt = providerFailure.persistedRun.assignments.flatMap(assignment => assignment.attempts)[0];
  assert.equal(providerFailure.result.status, "failed");
  assert.equal(failedAttempt.outcome, "failed");
  assert.equal(failedAttempt.resourceUsageBasis, "unknown");
  assert.equal(providerFailure.budget.heldTokens, syntheticTestReservationTokens);
  assert.equal(providerFailure.budget.measuredTokens, 0);
  assert.equal(providerFailure.execution.providerCalls.length, 1,
    "the deterministic provider was called only after its attempt reservation succeeded");

  const unsafeTelemetryRun = await createWorkerRun("budget-unsafe-telemetry", 1_000);
  const unsafeTelemetry = await execute(unsafeTelemetryRun, "unsafe-telemetry", {
    reservationTokens: syntheticTestReservationTokens,
    usageByStep: {
      extract: { inputTokens: -1, outputTokens: 7, totalTokens: Number.MAX_SAFE_INTEGER + 1 },
      summarize: syntheticTestUsageByStep.summarize,
    },
  });
  const unsafeAttempts = unsafeTelemetry.persistedRun.assignments.flatMap(assignment => assignment.attempts);
  assert.equal(unsafeTelemetry.result.status, "completed");
  assert.equal(unsafeAttempts[0].resourceUsageBasis, "unknown");
  assert.equal(unsafeAttempts[0].usage.inputTokens, null);
  assert.equal(unsafeAttempts[0].usage.outputTokens, 7,
    "valid partial counters survive while unsafe deterministic counters are omitted");
  assert.equal(unsafeAttempts[0].usage.totalTokens, null);
  assert.equal(unsafeTelemetry.budget.measuredTokens, 80);
  assert.equal(unsafeTelemetry.budget.heldTokens, syntheticTestReservationTokens);
  assert.equal(unsafeTelemetry.budget.unresolvedAttempts, 1);

  const inconsistentTelemetryRun = await createWorkerRun("budget-inconsistent-telemetry", 1_000);
  const inconsistentTelemetry = await execute(inconsistentTelemetryRun, "inconsistent-telemetry", {
    reservationTokens: syntheticTestReservationTokens,
    usageByStep: {
      extract: { inputTokens: 1, cachedInputTokens: 1, cacheWriteInputTokens: 1,
        outputTokens: 1, reasoningOutputTokens: 2, totalTokens: 1 },
      summarize: syntheticTestUsageByStep.summarize,
    },
  });
  const inconsistentAttempts = inconsistentTelemetry.persistedRun.assignments.flatMap(assignment => assignment.attempts);
  assert.equal(inconsistentTelemetry.result.status, "completed");
  assert.equal(inconsistentAttempts[0].resourceUsageBasis, "unknown");
  assert.equal(inconsistentAttempts[0].usage.inputTokens, 1);
  assert.equal(inconsistentAttempts[0].usage.cachedInputTokens, null);
  assert.equal(inconsistentAttempts[0].usage.cacheWriteInputTokens, null);
  assert.equal(inconsistentAttempts[0].usage.outputTokens, 1);
  assert.equal(inconsistentAttempts[0].usage.reasoningOutputTokens, null);
  assert.equal(inconsistentAttempts[0].usage.totalTokens, null);
  assert.equal(inconsistentTelemetry.budget.measuredTokens, 80);
  assert.equal(inconsistentTelemetry.budget.heldTokens, syntheticTestReservationTokens);
  assert.equal(inconsistentTelemetry.budget.unresolvedAttempts, 1,
    "contradictory safe counters are reduced to a valid unknown subset");

  const postAdmissionFailureRun = await createWorkerRun("budget-post-admission-failure", 500, {}, 2);
  const postAdmissionFailure = await execute(postAdmissionFailureRun, "post-admission-failure", {
    reservationTokens: syntheticTestReservationTokens,
  }, {
    onTransportResult: async () => {
      throw Object.assign(new Error("Deterministic synthetic result-hook failure."), {
        code: "SYNTHETIC_RESULT_HOOK_FAILURE",
      });
    },
  });
  const postAdmissionFailureAttempt = postAdmissionFailure.persistedRun.assignments
    .flatMap(assignment => assignment.attempts)[0];
  assert.equal(postAdmissionFailure.result.status, "failed");
  assert.equal(postAdmissionFailureAttempt.outcome, "failed");
  assert.equal(postAdmissionFailureAttempt.resourceUsageBasis, "unknown");
  assert.equal(postAdmissionFailure.budget.heldTokens, syntheticTestReservationTokens);
  assert.equal(postAdmissionFailure.budget.measuredTokens, 0);
  assert.equal(postAdmissionFailure.budget.unresolvedAttempts, 1,
    "a failure after admission settles unknown instead of stranding an active reserved attempt");

  const raceRoot = await journal.createRun({
    runId: `synthetic-bounded-race-root-${suffix}`,
    workflowVersionId,
    owner: { kind: "system", principalId: "synthetic-archive-analysis-worker" },
    input: { documentId: `synthetic-archive-race-root-${suffix}` },
    runFamilyBudget: { version: 3, maxDescendantRuns: 2, maxTotalAttempts: 8, maxTotalTokens: 300 },
  });
  await journal.transitionRun({ runId: raceRoot.runId, status: "failed",
    failure: { code: "SYNTHETIC_ARCHIVE_FAMILY_HANDOFF" } });
  const children = await Promise.all(["left", "right"].map(section => journal.createRun({
    runId: `synthetic-bounded-race-${section}-${suffix}`,
    workflowVersionId,
    owner: { kind: "system", principalId: "synthetic-archive-analysis-worker" },
    input: { documentId: `synthetic-archive-race-${section}-${suffix}`, section },
    resumedFromRunId: raceRoot.runId,
  })));
  const competingProviderCalls = [];
  const competingExecutions = children.map((child, index) => createBoundedAnalysisWorker({
    agentPlatform,
    journal,
    workerId: `synthetic-budget-race-${index}-${suffix}`,
    now,
    tokenAccounting: { reservationTokens: syntheticTestReservationTokens,
      providerCalls: competingProviderCalls },
  }));
  const competingRuns = await Promise.all(competingExecutions.map(execution => execution.worker.runOnce()));
  const competingStates = await Promise.all(children.map(child => journal.inspectRun({ runId: child.runId })));
  const competingBudget = await journal.inspectRunFamilyBudget({ runId: raceRoot.runId });
  const noAttemptChild = competingStates.find(child => child.assignments.every(assignment => assignment.attempts.length === 0));
  assert.ok(noAttemptChild, "the losing sibling has no admitted attempt");
  assert.equal(competingRuns.length, 2);
  assert.ok(competingRuns.every(run => run.status === "failed"),
    "both sibling workflows stop after their next reservation is rejected");
  assert.equal(competingProviderCalls.length, 1,
    "concurrent siblings share one family ceiling and only the admitted sibling reaches the provider");
  assert.equal(competingProviderCalls.some(call => call.runId === noAttemptChild.runId), false,
    "a rejected sibling admission makes zero provider calls");
  assert.equal(competingBudget.totalAttempts, 1);
  assert.equal(competingBudget.measuredTokens, 100);
  assert.equal(competingBudget.heldTokens, 0);
  assert.equal(competingBudget.maxTotalTokens, 300);

  const overrunRun = await createWorkerRun("budget-overrun", 300, {}, 2);
  const overrun = await execute(overrunRun, "overrun", {
    reservationTokens: 200,
    usageByStep: {
      extract: { inputTokens: 250, cachedInputTokens: 0, cacheWriteInputTokens: 0,
        outputTokens: 100, reasoningOutputTokens: 10, totalTokens: 350 },
      summarize: syntheticTestUsageByStep.summarize,
    },
  });
  assert.equal(overrun.result.status, "failed",
    "the later bounded-analysis step cannot reserve after the first measured provider overrun");
  assert.equal(overrun.budget.measuredTokens, 350);
  assert.equal(overrun.budget.overrunTokens, 150);
  assert.equal(overrun.budget.tokenAdmissionBlocked, true);
  assert.equal(overrun.execution.providerCalls.length, 1,
    "overrun rejection happens before the next deterministic provider call");
  assert.equal(overrun.execution.resourceObservations.some(event => event.type === "admission-rejected"
    && event.stepId === "summarize"), true);

  return Object.freeze({
    telemetrySource: "deterministic-test-telemetry",
    modelExecuted: false,
    completed: Object.freeze({ status: completed.result.status,
      measuredTokens: completed.budget.measuredTokens, heldTokens: completed.budget.heldTokens,
      remainingTokens: completed.budget.remainingTokens, attempts: completedAttempts.length,
      exactReplay: true, conflictingReplayRejected: true, trustedFixedReservation: true }),
    unknown: Object.freeze({ status: unknown.result.status,
      measuredTokens: unknown.budget.measuredTokens, heldTokens: unknown.budget.heldTokens,
      unresolvedAttempts: unknown.budget.unresolvedAttempts, partialAndWrongScope: true }),
    providerFailure: Object.freeze({ status: providerFailure.result.status,
      heldTokens: providerFailure.budget.heldTokens, resourceUsageBasis: failedAttempt.resourceUsageBasis,
      providerCalls: providerFailure.execution.providerCalls.length }),
    unsafeTelemetry: Object.freeze({ status: unsafeTelemetry.result.status,
      measuredTokens: unsafeTelemetry.budget.measuredTokens, heldTokens: unsafeTelemetry.budget.heldTokens,
      unresolvedAttempts: unsafeTelemetry.budget.unresolvedAttempts,
      safeOutputTokens: unsafeAttempts[0].usage.outputTokens, unsafeCountersOmitted: true }),
    inconsistentTelemetry: Object.freeze({ status: inconsistentTelemetry.result.status,
      measuredTokens: inconsistentTelemetry.budget.measuredTokens,
      heldTokens: inconsistentTelemetry.budget.heldTokens,
      unresolvedAttempts: inconsistentTelemetry.budget.unresolvedAttempts,
      contradictoryCountersOmitted: true }),
    postAdmissionFailure: Object.freeze({ status: postAdmissionFailure.result.status,
      heldTokens: postAdmissionFailure.budget.heldTokens,
      resourceUsageBasis: postAdmissionFailureAttempt.resourceUsageBasis,
      unresolvedAttempts: postAdmissionFailure.budget.unresolvedAttempts }),
    siblingRace: Object.freeze({ children: competingStates.length,
      providerCalls: competingProviderCalls.length, totalAttempts: competingBudget.totalAttempts,
      rejectedSiblingNoProviderCall: true }),
    overrun: Object.freeze({ status: overrun.result.status,
      measuredTokens: overrun.budget.measuredTokens, overrunTokens: overrun.budget.overrunTokens,
      laterAdmissionBlocked: overrun.budget.tokenAdmissionBlocked,
      providerCalls: overrun.execution.providerCalls.length }),
  });
}

async function runBoundedPostgresAcceptance({ agentPlatform, journal, journalFactory, budgetJournalFactory,
  runtimeDatabase, runtimePool, ownerPool, journalRelations, contentRelations, runtimeRole, suffix, now }) {
  const runtimeUser = (await runtimePool.query("SELECT current_user AS role_name")).rows[0].role_name;
  assert.equal(runtimeUser, runtimeRole, "the synthetic bounded worker uses the least-privilege runtime role");
  const workflowVersion = await registerBoundedAnalysisVersion(journal);
  const budgetJournal = budgetJournalFactory({ database: runtimeDatabase, now });
  const normalRun = await createBoundedRun(journal, workflowVersion.id, `normal-${suffix}`);
  const normalExecution = createBoundedAnalysisWorker({ agentPlatform, journal,
    workerId: `synthetic-bounded-normal-${suffix}`, now });
  const completed = await normalExecution.worker.runOnce();
  assert.equal(completed.runId, normalRun.runId);
  assert.equal(completed.status, "completed");
  const normalSummary = summarizeBoundedAnalysis(completed, normalExecution.traceEvents);
  const normalAttempts = completed.assignments.flatMap(assignment => assignment.attempts);
  assert.ok(normalAttempts.every(attempt => attempt.resourceReservation === undefined
    && !Object.hasOwn(attempt, "resourceUsageBasis") && attempt.usage === null),
  "legacy unbudgeted bounded execution retains its original resource-accounting shape");

  const recoveryRun = await createBoundedRun(journal, workflowVersion.id, `recovery-${suffix}`);
  const recoverySeed = await seedOpenBoundedAttempt({ journal, run: recoveryRun,
    documentId: `synthetic-archive-document-recovery-${suffix}`,
    workerId: `synthetic-bounded-recovery-old-${suffix}`, assignmentId: `bounded-recovery-assignment-${suffix}`,
    attemptId: `bounded-recovery-attempt-${suffix}`, threadId: `bounded-recovery-thread-${suffix}`,
    turnId: `bounded-recovery-turn-${suffix}`, sourceKey: `bounded-recovery-open-event-${suffix}` });
  await ownerPool.query(`UPDATE ${relation(journalRelations.runs)}
    SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, [recoveryRun.runId]);
  const recoveryPages = [];
  const recoveryInterrupts = [];
  const recoveryJournal = journalFactory({ database: runtimeDatabase, now });
  const recoveryExecution = createBoundedAnalysisWorker({ agentPlatform, journal: recoveryJournal,
    workerId: `synthetic-bounded-recovery-new-${suffix}`,
    now,
    listThreads: async input => {
      assert.equal(input.limit, 100);
      assert.deepEqual(input.sourceKinds, ["appServer", "subAgent", "subAgentReview", "subAgentThreadSpawn", "subAgentOther"]);
      assert.equal(input.useStateDbOnly, false);
      recoveryPages.push(input);
      if (input.cursor === undefined) {
        return { data: Array.from({ length: 100 }, (_, index) => ({ id: `bounded-unrelated-${index}`, status: "idle" })),
          nextCursor: "bounded-recovery-page-two" };
      }
      assert.equal(input.cursor, "bounded-recovery-page-two");
      return { data: [{ id: recoverySeed.attempt.threadId, status: { type: "active" } }], nextCursor: null };
    },
    interruptTurn: async input => { recoveryInterrupts.push(input); },
  });
  const recovered = await recoveryExecution.worker.runOnce();
  assert.equal(recovered.runId, recoveryRun.runId);
  assert.equal(recovered.status, "completed");
  assert.equal(recoveryPages.length, 2, "the new worker reads through page one to find the required thread");
  assert.equal(recoveryInterrupts.length, 1);
  assert.deepEqual(recoveryInterrupts[0], { threadId: recoverySeed.attempt.threadId, turnId: recoverySeed.attempt.turnId });
  const recoveredOldAttempt = recovered.assignments.find(value => value.assignmentId === recoverySeed.assignment.assignmentId)
    ?.attempts.find(value => value.attemptId === recoverySeed.attempt.attemptId);
  assert.equal(recoveredOldAttempt?.outcome, "interrupted");
  assert.equal(recoveredOldAttempt?.result?.reconciliation?.observedStatus, "active");
  assert.equal(recoveredOldAttempt?.resourceReservation, undefined);
  assert.equal(Object.hasOwn(recoveredOldAttempt ?? {}, "resourceUsageBasis"), false,
    "legacy unbudgeted recovery omits the v3-only usage basis");
  summarizeBoundedAnalysis(recovered, recoveryExecution.traceEvents);

  const reservedRecoveryRun = await createBoundedRun(budgetJournal, workflowVersion.id,
    `reserved-recovery-${suffix}`, {}, {
      version: 3, maxDescendantRuns: 0, maxTotalAttempts: 4, maxTotalTokens: 1_000,
    });
  const reservedRecoverySeed = await seedOpenBoundedAttempt({ journal: budgetJournal, run: reservedRecoveryRun,
    documentId: `synthetic-archive-document-reserved-recovery-${suffix}`,
    workerId: `synthetic-bounded-reserved-recovery-old-${suffix}`,
    assignmentId: `bounded-reserved-recovery-assignment-${suffix}`,
    attemptId: `bounded-reserved-recovery-attempt-${suffix}`,
    threadId: `bounded-reserved-recovery-thread-${suffix}`,
    turnId: `bounded-reserved-recovery-turn-${suffix}`,
    sourceKey: `bounded-reserved-recovery-open-event-${suffix}`,
    resourceReservationTokens: 120,
  });
  await ownerPool.query(`UPDATE ${relation(journalRelations.runs)}
    SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, [reservedRecoveryRun.runId]);
  const reservedRecoveryExecution = createBoundedAnalysisWorker({ agentPlatform, journal: budgetJournal,
    workerId: `synthetic-bounded-reserved-recovery-new-${suffix}`,
    tokenAccounting: { reservationTokens: syntheticTestReservationTokens },
    now,
    listThreads: async input => {
      assert.equal(input.useStateDbOnly, false);
      return { data: [{ id: reservedRecoverySeed.attempt.threadId, status: { type: "active" } }], nextCursor: null };
    },
    interruptTurn: async input => assert.deepEqual(input, {
      threadId: reservedRecoverySeed.attempt.threadId, turnId: reservedRecoverySeed.attempt.turnId,
    }),
  });
  const reservedRecovered = await reservedRecoveryExecution.worker.runOnce();
  const reservedRecoveredOldAttempt = reservedRecovered.assignments
    .find(assignment => assignment.assignmentId === reservedRecoverySeed.assignment.assignmentId)
    ?.attempts.find(attempt => attempt.attemptId === reservedRecoverySeed.attempt.attemptId);
  const reservedRecoveryBudget = await budgetJournal.inspectRunFamilyBudget({ runId: reservedRecoveryRun.runId });
  assert.equal(reservedRecovered.status, "completed");
  assert.equal(reservedRecoveredOldAttempt?.outcome, "interrupted");
  assert.equal(reservedRecoveredOldAttempt?.resourceUsageBasis, "unknown",
    "expired reserved attempts recover as unknown rather than releasing their token hold");
  assert.equal(reservedRecoveryBudget.measuredTokens, 180);
  assert.equal(reservedRecoveryBudget.heldTokens, 120);
  assert.equal(reservedRecoveryBudget.unresolvedAttempts, 1);
  assert.equal(reservedRecoveryBudget.totalAttempts, 3);

  const unknownRun = await createBoundedRun(journal, workflowVersion.id, `unknown-${suffix}`);
  const unknownSeed = await seedOpenBoundedAttempt({ journal, run: unknownRun,
    documentId: `synthetic-archive-document-unknown-${suffix}`,
    workerId: `synthetic-bounded-unknown-old-${suffix}`, assignmentId: `bounded-unknown-assignment-${suffix}`,
    attemptId: `bounded-unknown-attempt-${suffix}`, threadId: `bounded-unknown-thread-${suffix}`,
    turnId: `bounded-unknown-turn-${suffix}`, sourceKey: `bounded-unknown-open-event-${suffix}` });
  await ownerPool.query(`UPDATE ${relation(journalRelations.runs)}
    SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, [unknownRun.runId]);
  const unknownJournal = journalFactory({ database: runtimeDatabase, now });
  const unknownExecution = createBoundedAnalysisWorker({ agentPlatform, journal: unknownJournal,
    workerId: `synthetic-bounded-unknown-new-${suffix}`,
    listThreads: async input => {
      assert.equal(input.useStateDbOnly, false);
      return input.cursor === undefined
        ? { data: Array.from({ length: 100 }, (_, index) => ({ id: `unknown-unrelated-${index}`, status: "idle" })),
          nextCursor: "bounded-unknown-page-two" }
        : { data: [{ id: unknownSeed.attempt.threadId, status: { type: "future-status" } }], nextCursor: null };
    },
  });
  const unknownBeforeClaim = await unknownJournal.inspectRun({ runId: unknownRun.runId });
  const unknownError = await unknownExecution.worker.runOnce().then(() => null, error => error);
  assert.equal(unknownError?.code, "RECOVERY_UNAVAILABLE");
  const unknownAfterClaim = await unknownJournal.inspectRun({ runId: unknownRun.runId });
  assert.deepEqual(boundedProtectedSnapshot(unknownAfterClaim), boundedProtectedSnapshot(unknownBeforeClaim),
    "unknown thread status defers recovery without changing assignment, attempt, event, or terminal state");
  assert.equal(unknownAfterClaim.status, "running");
  await ownerPool.query(`UPDATE ${relation(journalRelations.runs)}
    SET lease_expires_at=clock_timestamp()+interval '10 minutes' WHERE id=$1`, [unknownRun.runId]);

  const staleRun = await createBoundedRun(budgetJournal, workflowVersion.id, `stale-${suffix}`, {}, {
    version: 3, maxDescendantRuns: 0, maxTotalAttempts: 4, maxTotalTokens: 1_000,
  });
  let transportResultResolve;
  const transportResultReady = new Promise(resolve => { transportResultResolve = resolve; });
  let releaseTransportResolve;
  const releaseTransport = new Promise(resolve => { releaseTransportResolve = resolve; });
  const staleWorkerId = `synthetic-bounded-same-worker-${suffix}`;
  const staleJournal = budgetJournal;
  const staleExecution = createBoundedAnalysisWorker({ agentPlatform, journal: staleJournal,
    workerId: staleWorkerId,
    tokenAccounting: { reservationTokens: syntheticTestReservationTokens },
    onTransportResult: async context => {
      assert.equal(Object.isFrozen(context.journal), true);
      transportResultResolve(context);
      await releaseTransport;
    },
  });
  const staleRunTask = staleExecution.worker.runOnce().then(() => null, error => error);
  const delayedTransport = await transportResultReady;
  const oldClaimEpoch = delayedTransport.run.claim.claimedAt;
  await ownerPool.query(`UPDATE ${relation(journalRelations.runs)}
    SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, [staleRun.runId]);
  const replacementClaim = await staleJournal.claimRun({ workerId: staleWorkerId,
    leaseSeconds: 30, workflowType: "bounded-analysis" });
  assert.equal(replacementClaim.runId, staleRun.runId);
  assert.notEqual(replacementClaim.claim.claimedAt, oldClaimEpoch,
    "the same worker identifier receives a distinct claim epoch after expiry");
  const staleBeforeRelease = await staleJournal.inspectRun({ runId: staleRun.runId });
  const staleBudgetBeforeRelease = await staleJournal.inspectRunFamilyBudget({ runId: staleRun.runId });
  const staleWriteResults = await Promise.all([
    delayedTransport.journal.finishAttempt({
      attemptId: delayedTransport.attempt.attemptId,
      outcome: "completed",
      telemetryStatus: "complete",
      resourceUsageBasis: "attempt",
      usage: delayedTransport.transport.testTelemetry.usage,
      result: { stale: true },
    }).then(() => null, error => error),
    delayedTransport.journal.finishAssignment({
      assignmentId: delayedTransport.assignmentId,
      status: "completed",
    }).then(() => null, error => error),
    delayedTransport.journal.appendEvent({
      runId: staleRun.runId,
      assignmentId: delayedTransport.assignmentId,
      attemptId: delayedTransport.attempt.attemptId,
      sourceKey: `stale-analysis-trace:${delayedTransport.attempt.attemptId}`,
      eventType: "synthetic.archive-analysis-trace",
      payload: { stale: true },
    }).then(() => null, error => error),
    delayedTransport.journal.transitionRun({
      runId: staleRun.runId,
      status: "failed",
      failure: { code: "STALE_CLAIM_TEST" },
    }).then(() => null, error => error),
  ]);
  assert.ok(staleWriteResults.every(error => error?.code === "WORKFLOW_CLAIM_LOST"),
    "delayed model-result telemetry, attempt, assignment, and terminal writes all reject the stale epoch");
  releaseTransportResolve();
  const staleError = await staleRunTask;
  assert.equal(staleError?.code, "WORKFLOW_CLAIM_LOST");
  const staleAfterRelease = await staleJournal.inspectRun({ runId: staleRun.runId });
  const staleBudgetAfterRelease = await staleJournal.inspectRunFamilyBudget({ runId: staleRun.runId });
  assert.deepEqual(boundedProtectedSnapshot(staleAfterRelease), boundedProtectedSnapshot(staleBeforeRelease),
    "a per-run facade retained across a same-worker epoch replacement cannot write");
  assert.deepEqual(staleBudgetAfterRelease, staleBudgetBeforeRelease,
    "the replaced claim cannot settle or release a reserved token hold");
  assert.equal(staleBudgetAfterRelease.heldTokens, syntheticTestReservationTokens);
  assert.equal(staleBudgetAfterRelease.measuredTokens, 0);
  const staleOpenAttempt = staleAfterRelease.assignments.flatMap(assignment => assignment.attempts)
    .find(attempt => attempt.attemptId === delayedTransport.attempt.attemptId);
  assert.equal(staleOpenAttempt?.outcome, null);
  assert.equal(staleOpenAttempt?.resourceReservation?.totalTokens, syntheticTestReservationTokens);
  assert.equal(staleOpenAttempt?.resourceUsageBasis, null,
    "the stale claim leaves its reserved attempt unresolved for a valid replacement owner");
  assert.equal(staleAfterRelease.status, "running");
  const staleOutputCount = await ownerPool.query(`SELECT count(*)::integer AS count
    FROM ${relation(contentRelations.observedUsage)}
    WHERE attempt_id=$1 AND direction='produced-output'`, [delayedTransport.attempt.attemptId]);
  assert.equal(staleOutputCount.rows[0].count, 0,
    "lost-claim finish cannot create produced-output capture; earlier input remains valid");
  await ownerPool.query(`UPDATE ${relation(journalRelations.runs)}
    SET lease_expires_at=clock_timestamp()+interval '10 minutes' WHERE id=$1`, [staleRun.runId]);

  const deadlineRun = await createBoundedRun(journal, workflowVersion.id, `deadline-${suffix}`, {
    executionDeadlineAt: "2000-01-01T00:00:00.000Z",
  });
  const deadlineJournal = journalFactory({ database: runtimeDatabase, now });
  const deadlineExecution = createBoundedAnalysisWorker({ agentPlatform, journal: deadlineJournal,
    workerId: `synthetic-bounded-deadline-${suffix}` });
  const deadlineResult = await deadlineExecution.worker.runOnce();
  assert.equal(deadlineResult.runId, deadlineRun.runId);
  assert.equal(deadlineResult.status, "failed");
  assert.equal(deadlineResult.failure.code, "EXECUTION_DEADLINE_EXCEEDED");
  assert.equal(deadlineResult.assignments.filter(value => value.stepKey.startsWith("bounded-analysis.")).length, 0);

  const heartbeatRun = await createBoundedRun(journal, workflowVersion.id, `heartbeat-${suffix}`);
  const heartbeatJournal = journalFactory({ database: runtimeDatabase, now });
  const heartbeatExecution = createBoundedAnalysisWorker({ agentPlatform, journal: heartbeatJournal,
    workerId: `synthetic-bounded-heartbeat-${suffix}`, heartbeatMs: 50,
    onAttemptStarted: async ({ run, signal }) => {
      await ownerPool.query(`UPDATE ${relation(journalRelations.runs)}
        SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, [run.runId]);
      const deadline = Date.now() + 2_000;
      while (!signal.aborted && Date.now() < deadline) await new Promise(resolve => setTimeout(resolve, 10));
      assert.equal(signal.aborted, true, "server-side expiry is observed by the exact-epoch heartbeat");
      signal.throwIfAborted();
    },
  });
  const heartbeatError = await heartbeatExecution.worker.runOnce().then(() => null, error => error);
  assert.equal(heartbeatError?.code, "WORKFLOW_CLAIM_LOST");
  const heartbeatAfter = await heartbeatJournal.inspectRun({ runId: heartbeatRun.runId });
  const heartbeatAssignments = heartbeatAfter.assignments.filter(value => value.stepKey.startsWith("bounded-analysis."));
  assert.equal(heartbeatAfter.status, "running");
  assert.equal(heartbeatAssignments.length, 1);
  assert.equal(heartbeatAssignments[0].status, "running");
  assert.equal(heartbeatAssignments[0].attempts.length, 1);
  assert.equal(heartbeatAssignments[0].attempts[0].outcome, null);
  assert.equal(heartbeatAfter.events.some(event => event.eventType === "synthetic.archive-analysis-trace"), false);
  assert.equal(heartbeatExecution.traceEvents.length, 0);
  await ownerPool.query(`UPDATE ${relation(journalRelations.runs)}
    SET lease_expires_at=clock_timestamp()+interval '10 minutes' WHERE id=$1`, [heartbeatRun.runId]);

  const recoveredStatus = await runtimePool.query(`SELECT status FROM ${relation(journalRelations.runs)} WHERE id=$1`,
    [recoveryRun.runId]);
  assert.equal(recoveredStatus.rows[0].status, "completed");
  const tokenAccounting = await runBoundedTokenAccountingAcceptance({
    agentPlatform,
    journal: budgetJournal,
    workflowVersionId: workflowVersion.id,
    suffix,
    now,
  });
  return Object.freeze({
    ...normalSummary,
    tokenAccounting,
    reservedRecovery: Object.freeze({ resourceUsageBasis: reservedRecoveredOldAttempt?.resourceUsageBasis,
      heldTokens: reservedRecoveryBudget.heldTokens,
      measuredTokens: reservedRecoveryBudget.measuredTokens,
      totalAttempts: reservedRecoveryBudget.totalAttempts,
      staleEpochCannotSettle: staleBudgetAfterRelease.heldTokens === syntheticTestReservationTokens
        && staleBudgetAfterRelease.measuredTokens === 0 }),
    legacyUnbudgetedCompatibility: true,
    runtimeRoleVerified: true,
    recovery: Object.freeze({ activePageTwoReconciled: recoveryPages.length === 2
      && recoveryInterrupts.length === 1 && recoveredOldAttempt?.outcome === "interrupted",
    pageCount: recoveryPages.length, observedStatus: recoveredOldAttempt?.result?.reconciliation?.observedStatus }),
    unknownRecovery: Object.freeze({ deferredWithoutMutation: unknownError?.code === "RECOVERY_UNAVAILABLE"
      && stableJson(boundedProtectedSnapshot(unknownAfterClaim)) === stableJson(boundedProtectedSnapshot(unknownBeforeClaim)),
    errorCode: unknownError?.code }),
    staleEpoch: Object.freeze({ rejectedWithoutMutation: staleError?.code === "WORKFLOW_CLAIM_LOST"
      && stableJson(boundedProtectedSnapshot(staleAfterRelease)) === stableJson(boundedProtectedSnapshot(staleBeforeRelease)),
    oldClaimEpoch, replacementClaimEpoch: replacementClaim.claim.claimedAt,
    producedOutputCount: staleOutputCount.rows[0].count }),
    deadline: Object.freeze({ failureCode: deadlineResult.failure.code, status: deadlineResult.status }),
    heartbeat: Object.freeze({ failureCode: heartbeatError?.code, status: heartbeatAfter.status,
      assignmentStatus: heartbeatAssignments[0].status, attemptOutcome: heartbeatAssignments[0].attempts[0].outcome }),
  });
}

async function runPostgresRevisionTransition({ agentPlatform, runtimeDatabase, ownerPool,
  journalRelations, familyRelations, applicationSchema, runtimeRole, suffix, now }) {
  const factory = makeJournalFactory(agentPlatform, journalRelations, {}, familyRelations);
  const journal = factory({ database: runtimeDatabase, now });
  const version = await journal.registerVersion(syntheticRevisionVersion());
  const owner = { kind: "system", principalId: "synthetic-archive-revision-worker" };
  const makeParent = async label => {
    const parent = await journal.createRun({
      runId: `synthetic-revision-${label}-${suffix}`, workflowVersionId: version.id,
      owner, queuePriority: 7,
      input: { archiveId: `synthetic-archive-${label}`, threadId: `synthetic-revision-thread-${label}` },
      runFamilyBudget: { version: 1, maxDescendantRuns: 1 },
    });
    await journal.transitionRun({ runId: parent.runId, status: "running" });
    await journal.transitionRun({ runId: parent.runId, status: "blocked",
      failure: { code: "ARCHIVE_METADATA_MISSING" } });
    return journal.inspectRun({ runId: parent.runId });
  };
  const membersFor = async parentRunId => (await ownerPool.query(`
    SELECT run_id,root_run_id,parent_run_id FROM ${relation(familyRelations.members)}
    WHERE root_run_id=$1 ORDER BY run_id
  `, [parentRunId])).rows;
  const assertAbsentChild = async (parent, transition, expectedEventCount) => {
    const childRows = await ownerPool.query(`SELECT id FROM ${relation(journalRelations.runs)} WHERE id=$1`,
      [transition.run.runId]);
    const memberRows = await ownerPool.query(`SELECT run_id FROM ${relation(familyRelations.members)} WHERE run_id=$1`,
      [transition.run.runId]);
    const current = await journal.inspectRun({ runId: parent.runId });
    assert.equal(childRows.rows.length, 0, "failed combined transitions leave no child");
    assert.equal(memberRows.rows.length, 0, "failed combined transitions reserve no family slot");
    assert.equal(current.events.length, expectedEventCount, "failed combined transitions leave parent events unchanged");
    assert.equal(current.updatedAt, parent.updatedAt, "failed combined transitions leave the parent timestamp unchanged");
  };
  const captureRequest = async (parent, outcome) => {
    let transition = null;
    const intercepted = { ...journal,
      createRunAndAppendEvent(value) {
        transition = value;
        throw new Error("Synthetic revision request captured before persistence.");
      } };
    await assert.rejects(makeSyntheticRevisionController(agentPlatform, intercepted).advance({
      runId: parent.runId, outcome,
    }), /captured before persistence/);
    assert.ok(transition?.run && transition?.event);
    return transition;
  };

  const parent = await makeParent("exact");
  const controller = makeSyntheticRevisionController(agentPlatform, journal);
  const outcome = syntheticRevisionOutcome("exact");
  const [first, replay] = await Promise.all([
    controller.advance({ runId: parent.runId, outcome }),
    controller.advance({ runId: parent.runId, outcome }),
  ]);
  assert.equal(first.childRunId, replay.childRunId, "concurrent exact retries select one child");
  const firstParent = await journal.inspectRun({ runId: parent.runId });
  const firstChild = await journal.inspectRun({ runId: first.childRunId });
  assert.equal(firstParent.events.length, 1, "concurrent exact retries persist one parent event");
  assert.deepEqual(await membersFor(parent.runId), [{ run_id: first.childRunId,
    root_run_id: parent.runId, parent_run_id: parent.runId }]);
  assert.equal(firstChild.resumedFromRunId, parent.runId);
  assert.equal(firstChild.owner.principalId, owner.principalId);
  assert.equal(firstChild.workflowVersionId, version.id);
  assert.equal(firstParent.events[0].payload.childRunId, firstChild.runId);
  assert.equal(firstParent.events[0].payload.parentRunId, parent.runId);

  await journal.transitionRun({ runId: first.childRunId, status: "running" });
  await journal.transitionRun({ runId: first.childRunId, status: "blocked",
    failure: { code: "SYNTHETIC_REVISION_PAUSED" } });
  const progressed = await controller.advance({ runId: parent.runId, outcome });
  assert.equal(progressed.childRunId, first.childRunId);
  assert.equal((await journal.inspectRun({ runId: first.childRunId })).status, "blocked",
    "an exact replay does not reopen a progressed child");
  const freshJournal = factory({ database: runtimeDatabase, now });
  const freshReplay = await makeSyntheticRevisionController(agentPlatform, freshJournal).advance({
    runId: parent.runId, outcome,
  });
  assert.equal(freshReplay.childRunId, first.childRunId);
  assert.equal((await freshJournal.inspectRun({ runId: first.childRunId })).status, "blocked");
  assert.equal((await journal.inspectRun({ runId: parent.runId })).events.length, 1);

  const slotParent = await makeParent("last-slot");
  const contenders = await Promise.allSettled(["a", "b"].map(label =>
    makeSyntheticRevisionController(agentPlatform, journal).advance({
      runId: slotParent.runId, outcome: syntheticRevisionOutcome(`slot-${label}`),
    })));
  const accepted = contenders.filter(value => value.status === "fulfilled");
  const rejected = contenders.filter(value => value.status === "rejected");
  assert.equal(accepted.length, 1, "the last family slot has one winner");
  assert.equal(rejected.length, 1, "the other distinct child is rejected");
  assert.ok(["23514", "WORKFLOW_CONFLICT"].includes(rejected[0].reason?.code));
  assert.equal((await membersFor(slotParent.runId)).length, 1);
  assert.equal((await journal.inspectRun({ runId: slotParent.runId })).events.length, 1);

  const missingParent = await makeParent("missing-event");
  const missingOutcome = syntheticRevisionOutcome("missing-event");
  const missingTransition = await captureRequest(missingParent, missingOutcome);
  const stagedChild = await journal.createRun(missingTransition.run);
  assert.equal(stagedChild.status, "queued");
  assert.equal((await journal.inspectRun({ runId: missingParent.runId })).events.length, 0);
  const repaired = await controller.advance({ runId: missingParent.runId, outcome: missingOutcome });
  assert.equal(repaired.childRunId, stagedChild.runId);
  assert.equal((await journal.inspectRun({ runId: stagedChild.runId })).status, "queued");
  assert.equal((await journal.inspectRun({ runId: missingParent.runId })).events.length, 1);
  assert.deepEqual(await membersFor(missingParent.runId), [{ run_id: stagedChild.runId,
    root_run_id: missingParent.runId, parent_run_id: missingParent.runId }]);

  const conflictRoot = await makeParent("event-conflict");
  const conflictOutcome = syntheticRevisionOutcome("event-conflict");
  const conflictTransition = await captureRequest(conflictRoot, conflictOutcome);
  await journal.appendEvent({ ...conflictTransition.event,
    payload: { ...conflictTransition.event.payload, fabricatedConflict: true } });
  const conflictBefore = await journal.inspectRun({ runId: conflictRoot.runId });
  await assert.rejects(controller.advance({ runId: conflictRoot.runId, outcome: conflictOutcome }),
    error => error.code === "WORKFLOW_CONFLICT");
  await assertAbsentChild(conflictBefore, conflictTransition, 1);
  assert.deepEqual(await membersFor(conflictRoot.runId), []);

  const identityRoot = await makeParent("child-identity-conflict");
  const identityOutcome = syntheticRevisionOutcome("child-identity-conflict");
  const identityTransition = await captureRequest(identityRoot, identityOutcome);
  await journal.createRun({ ...identityTransition.run,
    input: { ...identityTransition.run.input, alteredChildIdentity: true } });
  const identityParentBefore = await journal.inspectRun({ runId: identityRoot.runId });
  const identityChildBefore = await journal.inspectRun({ runId: identityTransition.run.runId });
  const identityMembersBefore = await membersFor(identityRoot.runId);
  assert.deepEqual(identityMembersBefore, [{ run_id: identityTransition.run.runId,
    root_run_id: identityRoot.runId, parent_run_id: identityRoot.runId }]);
  await assert.rejects(controller.advance({ runId: identityRoot.runId, outcome: identityOutcome }),
    error => error.code === "WORKFLOW_CONFLICT");
  const identityParentAfter = await journal.inspectRun({ runId: identityRoot.runId });
  assert.equal(identityParentAfter.events.length, 0, "child identity conflict appends no parent event");
  assert.equal(identityParentAfter.updatedAt, identityParentBefore.updatedAt,
    "child identity conflict leaves the parent timestamp unchanged");
  assert.deepEqual(await journal.inspectRun({ runId: identityTransition.run.runId }), identityChildBefore,
    "child identity conflict preserves the existing child's immutable record and status");
  assert.deepEqual(await membersFor(identityRoot.runId), identityMembersBefore,
    "child identity conflict leaves exact existing family membership unchanged");

  const persistenceRoot = await makeParent("event-write-failure");
  const persistenceOutcome = syntheticRevisionOutcome("event-write-failure");
  const persistenceTransition = await captureRequest(persistenceRoot, persistenceOutcome);
  const failingDatabase = {
    query: (...args) => runtimeDatabase.query(...args),
    transaction: callback => runtimeDatabase.transaction(client => callback({
      query(sql, params) {
        if (typeof sql === "string" && sql.includes(`INSERT INTO ${relation(journalRelations.events)}`)) {
          throw new Error("Synthetic parent event persistence failure.");
        }
        return client.query(sql, params);
      },
    })),
  };
  const failingJournal = factory({ database: failingDatabase, now });
  await assert.rejects(makeSyntheticRevisionController(agentPlatform, failingJournal).advance({
    runId: persistenceRoot.runId, outcome: persistenceOutcome,
  }), /parent event persistence failure/);
  await assertAbsentChild(persistenceRoot, persistenceTransition, 0);
  assert.deepEqual(await membersFor(persistenceRoot.runId), []);

  const observerRoot = await makeParent("observer-failure");
  const observerOutcome = syntheticRevisionOutcome("observer-failure");
  const observerTransition = await captureRequest(observerRoot, observerOutcome);
  const observerTable = `${q(applicationSchema)}.${q("revision_observer_markers")}`;
  const observerMarker = `synthetic-revision-observer-${suffix}`;
  await ownerPool.query(`CREATE TABLE ${observerTable} (marker text PRIMARY KEY)`);
  await ownerPool.query(`REVOKE ALL ON TABLE ${observerTable} FROM PUBLIC`);
  await ownerPool.query(`GRANT INSERT ON TABLE ${observerTable} TO ${q(runtimeRole)}`);
  let observerCalls = 0;
  const observerFactory = makeJournalFactory(agentPlatform, journalRelations, {
    observeJournalEvent: async (client, event) => {
      if (event.event_type === "workflow.semantic-revision-created") {
        await client.query(`INSERT INTO ${observerTable} (marker) VALUES ($1)`, [observerMarker]);
        observerCalls += 1;
        throw new Error("Synthetic revision event observer failure.");
      }
    },
  }, familyRelations);
  const observerJournal = observerFactory({ database: runtimeDatabase, now });
  await assert.rejects(makeSyntheticRevisionController(agentPlatform, observerJournal).advance({
    runId: observerRoot.runId, outcome: observerOutcome,
  }), /revision event observer failure/);
  assert.equal(observerCalls, 1, "the event observer runs once within the failed transaction");
  const rolledBackMarker = await ownerPool.query(`SELECT count(*)::integer AS count
    FROM ${observerTable} WHERE marker=$1`, [observerMarker]);
  assert.equal(rolledBackMarker.rows[0].count, 0,
    "the observer's database write rolls back with the child, member, and event");
  await assertAbsentChild(observerRoot, observerTransition, 0);
  assert.deepEqual(await membersFor(observerRoot.runId), []);

  return Object.freeze({ concurrentExactRetry: true, lastFamilySlotSingleWinner: true,
    freshInstanceRetry: true, progressedChildPreserved: true,
    missingEventRepairedWithoutReopen: true, eventIdentityConflictRollback: true,
    childIdentityConflictRollback: true,
    eventPersistenceFailureRollback: true, observerFailureRollback: true,
    parentTimestampUnchangedOnRollback: true, familyMembershipExact: true,
    exactLineage: true });
}

async function createBoundedRun(journal, workflowVersionId, label, input = {}, runFamilyBudget = undefined) {
  return journal.createRun({
    runId: `synthetic-bounded-${label}`,
    workflowVersionId,
    owner: { kind: "system", principalId: "synthetic-archive-analysis-worker" },
    input: { documentId: `synthetic-archive-document-${label}`, archiveText: "Preserve the source record and review restrictions.", ...input },
    ...(runFamilyBudget ? { runFamilyBudget } : {}),
  });
}

async function seedOpenBoundedAttempt({ journal, run, documentId, workerId, assignmentId, attemptId, threadId, turnId, sourceKey,
  resourceReservationTokens = null }) {
  const claimed = await journal.claimRun({ workerId, leaseSeconds: 30, workflowType: "bounded-analysis" });
  assert.equal(claimed.runId, run.runId);
  const expectedClaim = { runId: run.runId, workerId, claimedAt: claimed.claim.claimedAt };
  const assignment = await journal.createAssignment({
    assignmentId,
    runId: run.runId,
    stepKey: "bounded-analysis.extract",
    assignedTo: "synthetic-archive-recovery-agent",
    execution: "current-agent",
    targets: [{ type: "synthetic-archive-document", id: `${documentId}:extract`, expectedVersion: 1 }],
    expectedClaim,
  });
  const attempt = await journal.startAttempt({
    attemptId,
    assignmentId,
    prompt: "Extract the synthetic archive retention evidence.",
    requested: { model: "synthetic-codex-transport", reasoningEffort: "low", serviceTier: "standard" },
    capabilityProfile: { promptVersionId: "synthetic-archive-analysis.extract.v1",
      executionProfile: "bounded-analysis.v1", allowedTools: [] },
    threadId,
    turnId,
    expectedClaim,
    ...(resourceReservationTokens == null ? {} : { resourceReservation: { totalTokens: resourceReservationTokens } }),
  });
  await journal.appendEvent({ runId: run.runId, assignmentId, attemptId, sourceKey,
    eventType: "synthetic.archive-analysis-started", payload: { threadId }, expectedClaim });
  return Object.freeze({ assignment, attempt, expectedClaim });
}

function boundedProtectedSnapshot(run) {
  return {
    status: run.status,
    failure: run.failure,
    completedAt: run.completedAt,
    finalSummary: run.finalSummary,
    assignments: run.assignments.map(assignment => ({
      assignmentId: assignment.assignmentId,
      stepKey: assignment.stepKey,
      status: assignment.status,
      failure: assignment.failure,
      attempts: assignment.attempts.map(attempt => ({
        attemptId: attempt.attemptId,
        outcome: attempt.outcome,
        telemetryStatus: attempt.telemetryStatus,
        result: attempt.result,
        error: attempt.error,
      })),
    })),
    events: run.events,
  };
}

function assertAgentPlatform(value) {
  for (const [key, names] of Object.entries({
    journal: ["createAgentWorkflowJournalFactory"],
    bootstrap: ["bootstrapAgentPlatformPostgres", "bootstrapAgentPlatformPostgresWithRunFamilies"],
    assignmentLifecycle: ["createWorkflowAssignmentLifecycle"],
    boundedAnalysisWorker: ["createBoundedAnalysisWorkerFactory"],
    revisionController: ["createWorkflowRevisionControllerFactory"],
  })) {
    assert.ok(value?.[key], `installed agent-platform namespace ${key} is required`);
    for (const name of names) assert.equal(typeof value[key][name], "function", `agent-platform ${key} exports ${name}`);
  }
}

function assertAgentPostgresReader(value) {
  assert.deepEqual(Object.keys(value ?? {}).sort(), [
    "AgentPostgresQueryError",
    "createAgentPostgresReaderFactory",
    "normalizeAgentPostgresQuery",
  ], "the independent archive uses the exact installed generic PostgreSQL reader namespace");
  assert.equal(typeof value.createAgentPostgresReaderFactory, "function");
  assert.equal(typeof value.AgentPostgresQueryError, "function");
  assert.equal(typeof value.normalizeAgentPostgresQuery, "function");
}

function assertContentLedger(value) {
  for (const [key, names] of Object.entries({
    capture: ["bootstrapContentLedgerPostgres", "createPostgresContentLedgerWriter", "normalizeObservedContent"],
    relationships: ["createPostgresContentRelationshipWriter", "normalizeContentRelationship"],
    assessments: ["bootstrapContentAssessmentPostgres", "createPostgresContentAssessmentWriter",
      "normalizeAssessmentDefinition", "normalizeContentAssessment"],
    transfers: ["bootstrapContentTransferPostgres", "createPostgresContentTransferPersistence",
      "bootstrapContentTransferDeliveryPostgres", "normalizeTransferAssessment", "normalizeTransferAssessmentDefinition", "normalizeTransferDecision",
      "normalizeTransferPolicyDefinition", "normalizeTransferRequest"],
    delivery: ["createContentTransferDeliveryCoordinator", "createPostgresContentTransferDeliveryPersistence"],
  })) {
    assert.ok(value?.[key], `installed content-ledger namespace ${key} is required`);
    for (const name of names) assert.equal(typeof value[key][name], "function", `content-ledger ${key} exports ${name}`);
  }
}

function makeJournalFactory(agentPlatform, relations, {
  observeJournalEvent = async () => {}, observeAttemptOutput = async () => {},
} = {}, familyRelations = undefined) {
  return agentPlatform.journal.createAgentWorkflowJournalFactory({
    postgresRelations: relations ?? undefined,
    postgresRunFamilyRelations: familyRelations,
    async assertCompletion(run, version, summary) {
      if (run.workflowType === "synthetic-archive-review") {
        assert.equal(version.workflowType, "synthetic-archive-review");
        if (summary && summary.disposition !== "preserved") throw new Error("Archive completion policy rejected the synthetic summary.");
        return;
      }
      assert.equal(run.workflowType, "bounded-analysis");
      assert.equal(version.workflowType, "bounded-analysis");
      assert.equal(summary?.kind, "bounded-analysis-completion");
      assert.equal(summary?.definitionHash, version.definition?.definitionHash);
      assert.deepEqual(summary?.results?.map(({ stepId }) => stepId), ["extract", "summarize"]);
      const boundedAssignments = run.assignments.filter(value => value.stepKey.startsWith("bounded-analysis."));
      assert.equal(boundedAssignments.filter(value => value.status === "completed").length, 2);
      assert.ok(boundedAssignments.every(value => ["completed", "interrupted"].includes(value.status)
        && value.attempts.every(attempt => attempt.outcome !== null)));
    },
    projectMetrics(run) {
      return { assignmentCount: run.assignments.length,
        attemptCount: run.assignments.reduce((count, assignment) => count + assignment.attempts.length, 0) };
    },
    observeJournalEvent,
    observeAttemptOutput,
    assignmentMarker: id => `SYNTHETIC_ARCHIVE_ASSIGNMENT:${id}`,
  });
}

function makeExecutionLedgerHooks(contentLedger, writer) {
  const schemaProfile = contentLedger.capture.CONTENT_LEDGER_POSTGRES_SCHEMA_PROFILE;
  const record = (client, { contentId, contentType, source, text, lineage, direction,
    usageRole, observationKey, observedAt }) => {
    assert.equal(typeof text, "string", "observed content must be the exact stored text leaf");
    const normalized = contentLedger.capture.normalizeObservedContent({
      applicationNamespace,
      contentId,
      contentType,
      contentSchemaVersion: 1,
      source,
      capture: { status: "complete", source: "synthetic archive journal observer",
        sha256: sha256(text), hashEncoding: "utf8-v1", byteLength: Buffer.byteLength(text, "utf8") },
      ...lineage,
      direction,
      usageRole,
      observationKey,
      lineageStatus: "exact",
      queryTruncated: false,
    }, { recordedAt: observedAt, schemaProfile });
    return writer.insertObservedContent(client, normalized);
  };
  return Object.freeze({
    async observeJournalEvent(client, event, receivedAt) {
      const imported = event.event_type === "synthetic.archive-import-response";
      const transportInput = event.event_type === "synthetic.archive-transport-input";
      if (!imported && !transportInput) return undefined;
      const lineage = { runId: event.run_id, assignmentId: event.assignment_id,
        attemptId: event.attempt_id, threadId: event.thread_id, turnId: event.turn_id };
      await record(client, {
        contentId: imported ? "archive-record-0042" : `transport-input:${event.attempt_id}`,
        contentType: imported ? "archive-document" : "synthetic-transport-input",
        source: { kind: "workflow-event-response", recordId: String(event.id),
          field: imported ? "payload.archiveDocument" : "payload.transportInput" },
        text: imported ? event.payload.archiveDocument : event.payload.transportInput,
        lineage,
        direction: "supplied-input",
        usageRole: imported ? "archive-import-response" : "synthetic-transport-input",
        observationKey: `journal-event:${event.id}`,
        observedAt: receivedAt,
      });
      return undefined;
    },
    observeAttemptOutput(client, attempt, resultPayload, timestamp) {
      if (typeof resultPayload?.output?.finding !== "string") return undefined;
      // This version records the selected finding string, not the complete result object.
      return record(client, {
        contentId: `analysis-finding:${attempt.id}`,
        contentType: "synthetic-analysis-finding",
        source: { kind: "attempt-final-output", recordId: attempt.id,
          field: "result.output.finding" },
        text: resultPayload.output.finding,
        lineage: { runId: attempt.run_id, assignmentId: attempt.assignment_id,
          attemptId: attempt.id, threadId: attempt.thread_id, turnId: attempt.turn_id },
        direction: "produced-output",
        usageRole: "synthetic-produced-finding",
        observationKey: `attempt-finding:${attempt.id}`,
        observedAt: timestamp,
      });
    },
  });
}

async function seedJournal(journal) {
  const version = await journal.registerVersion({
    id: "synthetic-archive-review.v1",
    workflowType: "synthetic-archive-review",
    definition: { kind: "archive-review", policy: "preservation-check.v1" },
    codeRevision: "standalone-synthetic-app",
    changeReason: "Exercise PostgreSQL workflow lineage with an independent archive application.",
    improvementHypothesis: "Prepared assignments recover from exact caller-attempt lineage.",
    reviewedBy: "archive-fixture-reviewer",
  });
  const run = await journal.createRun({
    runId: "synthetic-archive-run-0042",
    workflowVersionId: version.id,
    owner: { kind: "system", principalId: "archive-import-service" },
    input: { recordId: "archive-event-0042", requestedField: "preservationSummary" },
  });
  await journal.claimRun({ workerId: "archive-workflow-worker", workflowType: version.workflowType });
  const callerAssignment = await journal.createAssignment({
    assignmentId: "synthetic-archive-caller-assignment-0042",
    runId: run.runId,
    stepKey: "archive-review.extract-summary",
    assignedTo: requester.id,
    execution: "current-agent",
    threadId: "archive-thread-0042",
    targets: [{ type: "archive-record", id: "archive-event-0042", expectedVersion: 1 }],
  });
  const callerAttempt = await journal.startAttempt({
    attemptId: "synthetic-archive-caller-attempt-0042",
    assignmentId: callerAssignment.assignmentId,
    prompt: "Review this synthetic preservation record.",
    requested: { model: "synthetic-model", reasoningEffort: "low", serviceTier: "standard" },
    capabilityProfile: { allowedTools: [] },
    threadId: "archive-thread-0042",
    turnId: "archive-turn-0042",
  });
  return { run, callerAssignment, callerAttempt };
}

function preparedInput(runId, callerAssignmentId, callerAttemptId, threadId) {
  return {
    runId,
    idempotencyKey: "archive-preservation-review-0042",
    parentAssignmentId: callerAssignmentId,
    stepKey: "archive-review.check-restrictions",
    assignedTo: "archive-review-agent-02",
    execution: "subagent",
    targets: [{ type: "archive-record", id: "archive-event-0042#restrictions", expectedVersion: 1 }],
    requestFingerprint: "synthetic-archive-review-request-v1",
    executionRef: { kind: "caller-attempt", assignmentId: callerAssignmentId,
      attemptId: callerAttemptId, threadId },
    preparedExecution: {
      stage: "preservation-review",
      recordId: "archive-event-0042",
      assignmentBrief: "Review the synthetic preservation restrictions.",
      assignmentPrompt: "Check whether the synthetic archive record lists explicit preservation limits.",
      assignmentSettings: { model: "synthetic-model", reasoningEffort: "low" },
    },
  };
}

async function readObservedContentRows(pool, relations, runId) {
  const result = await pool.query(`
    SELECT version.id AS version_id,version.source_kind,version.source_record_id,
      version.source_field,version.content_sha256,version.byte_length,
      usage.id AS usage_id,usage.run_id,usage.assignment_id,usage.attempt_id,
      usage.thread_id,usage.turn_id,usage.direction,usage.usage_role,usage.observation_key
    FROM ${relation(relations.contentVersions)} AS version
    JOIN ${relation(relations.observedUsage)} AS usage
      ON usage.run_id=version.run_id AND usage.content_version_id=version.id
    WHERE usage.run_id=$1 ORDER BY usage.observation_key
  `, [runId]);
  return result.rows;
}

async function inspectNormalExecutionLedger({ ownerPool, relations, journal, normalRunId,
  importRunId, importEvent, callerAssignment, callerAttempt }) {
  const importRows = await readObservedContentRows(ownerPool, relations, importRunId);
  const importedResponses = importRows.filter(row => row.usage_role === "archive-import-response");
  assert.equal(importedResponses.length, 1, "one archive response is captured from its durable event");
  const imported = importRows.find(row => row.source_record_id === importEvent.eventId
    && row.usage_role === "archive-import-response");
  assert.ok(imported, "the archive import is captured from its durable response event");
  assert.deepEqual({ sourceKind: imported.source_kind, sourceField: imported.source_field,
    direction: imported.direction, assignmentId: imported.assignment_id,
    attemptId: imported.attempt_id, threadId: imported.thread_id, turnId: imported.turn_id,
    hash: imported.content_sha256, byteLength: Number(imported.byte_length) }, {
    sourceKind: "workflow-event-response", sourceField: "payload.archiveDocument",
    direction: "supplied-input", assignmentId: callerAssignment.assignmentId,
    attemptId: callerAttempt.attemptId, threadId: callerAttempt.threadId, turnId: callerAttempt.turnId,
    hash: sha256(importEvent.payload.archiveDocument), byteLength: Buffer.byteLength(importEvent.payload.archiveDocument, "utf8"),
  });
  const run = await journal.inspectRun({ runId: normalRunId });
  const rows = await readObservedContentRows(ownerPool, relations, normalRunId);
  const inputs = rows.filter(row => row.usage_role === "synthetic-transport-input");
  const outputs = rows.filter(row => row.usage_role === "synthetic-produced-finding");
  assert.equal(inputs.length, 2, "two normal rendered prompts are captured");
  assert.equal(outputs.length, 2, "two normal persisted finding leaves are captured");
  const primaryVersionIds = [imported.version_id];
  let extractAttempt;
  let extractOutput;
  let summarizeAttempt;
  let summarizeOutput;
  for (const stepId of ["extract", "summarize"]) {
    const assignment = run.assignments.find(value => value.stepKey === `bounded-analysis.${stepId}`);
    assert.equal(assignment?.attempts.length, 1);
    const attempt = assignment.attempts[0];
    const event = run.events.find(value => value.eventType === "synthetic.archive-transport-input"
      && value.attemptId === attempt.attemptId);
    assert.ok(event, `the ${stepId} prompt has a durable input event`);
    const input = inputs.find(value => value.source_record_id === event.eventId);
    const output = outputs.find(value => value.source_record_id === attempt.attemptId);
    assert.ok(input && output, `the ${stepId} input and finding have observed usage`);
    assert.equal(event.payload.transportInput, attempt.prompt,
      "the transport input event stores the exact rendered prompt sent to the attempt");
    for (const [capture, text, sourceKind, sourceField, direction] of [
      [input, event.payload.transportInput, "workflow-event-response", "payload.transportInput", "supplied-input"],
      [output, attempt.result.output.finding, "attempt-final-output", "result.output.finding", "produced-output"],
    ]) {
      assert.deepEqual({ sourceKind: capture.source_kind, sourceField: capture.source_field,
        direction: capture.direction, runId: capture.run_id, assignmentId: capture.assignment_id,
        attemptId: capture.attempt_id, threadId: capture.thread_id, turnId: capture.turn_id,
        hash: capture.content_sha256, byteLength: Number(capture.byte_length) }, {
        sourceKind, sourceField, direction, runId: normalRunId,
        assignmentId: assignment.assignmentId, attemptId: attempt.attemptId,
        threadId: attempt.threadId, turnId: attempt.turnId,
        hash: sha256(text), byteLength: Buffer.byteLength(text, "utf8"),
      });
    }
    assert.match(attempt.result.output.finding, /Café/,
      "the captured finding exercises non-ASCII UTF-8 byte accounting");
    primaryVersionIds.push(input.version_id, output.version_id);
    if (stepId === "extract") {
      extractAttempt = attempt;
      extractOutput = output;
    } else {
      summarizeAttempt = attempt;
      summarizeOutput = output;
    }
  }
  const beforeReplay = await snapshotRows(ownerPool, [relations.contentVersions, relations.observedUsage]);
  const replayEvent = run.events.find(value => value.eventType === "synthetic.archive-transport-input");
  await journal.appendEvent({ runId: replayEvent.runId, assignmentId: replayEvent.assignmentId,
    attemptId: replayEvent.attemptId, threadId: replayEvent.threadId, turnId: replayEvent.turnId,
    sourceKey: replayEvent.sourceKey, eventType: replayEvent.eventType, payload: replayEvent.payload });
  const eventReplayIdempotent = stableJson(await snapshotRows(ownerPool,
    [relations.contentVersions, relations.observedUsage])) === stableJson(beforeReplay);
  assert.equal(eventReplayIdempotent, true, "event replay adds no duplicate observed usage");
  await journal.finishAttempt({ attemptId: summarizeAttempt.attemptId, outcome: "completed",
    telemetryStatus: "incomplete", result: summarizeAttempt.result });
  const finishReplayIdempotent = stableJson(await snapshotRows(ownerPool,
    [relations.contentVersions, relations.observedUsage])) === stableJson(beforeReplay);
  assert.equal(finishReplayIdempotent, true, "finish replay adds no duplicate observed usage");
  return Object.freeze({ imported, extractAttempt, extractOutput, summarizeAttempt, summarizeOutput,
    summarizeFinding: summarizeAttempt.result.output.finding,
    primaryVersionIds: Object.freeze(primaryVersionIds),
    importResponseCount: importedResponses.length,
    transportInputCount: inputs.length, producedOutputCount: outputs.length,
    exactLineage: true, eventReplayIdempotent, finishReplayIdempotent });
}

async function assertObserverRollback({ agentPlatform, journalRelations, contentRelations,
  runtimeDatabase, ownerPool, ledgerHooks, suffix, now }) {
  const attemptId = `synthetic-observer-rollback-attempt-${suffix}`;
  const journalFactory = makeJournalFactory(agentPlatform, journalRelations, {
    observeJournalEvent: ledgerHooks.observeJournalEvent,
    async observeAttemptOutput(client, attempt, resultPayload, timestamp) {
      await ledgerHooks.observeAttemptOutput(client, attempt, resultPayload, timestamp);
      if (attempt.id === attemptId) throw new Error("Synthetic observer failure after ledger write.");
    },
  });
  const journal = journalFactory({ database: runtimeDatabase, now });
  const run = await journal.createRun({ runId: `synthetic-observer-rollback-run-${suffix}`,
    workflowVersionId: "synthetic-archive-review.v1",
    owner: { kind: "system", principalId: "archive-import-service" },
    input: { recordId: `synthetic-observer-rollback-${suffix}` }, queuePriority: 1_000_000 });
  const claimed = await journal.claimRun({ workerId: `synthetic-observer-rollback-worker-${suffix}`,
    workflowType: "synthetic-archive-review" });
  assert.equal(claimed.runId, run.runId);
  const assignment = await journal.createAssignment({
    assignmentId: `synthetic-observer-rollback-assignment-${suffix}`,
    runId: run.runId, stepKey: "archive-review.observer-rollback",
    assignedTo: "synthetic-archive-observer", execution: "current-agent",
    targets: [{ type: "archive-record", id: `observer-rollback-${suffix}`, expectedVersion: 1 }],
  });
  await journal.startAttempt({ attemptId, assignmentId: assignment.assignmentId,
    prompt: "Check observer transaction rollback.",
    requested: { model: "synthetic-model", reasoningEffort: "low", serviceTier: "standard" },
    capabilityProfile: { allowedTools: [] },
    threadId: `synthetic-observer-thread-${suffix}`, turnId: `synthetic-observer-turn-${suffix}` });
  const before = await snapshotRows(ownerPool, [contentRelations.contentVersions, contentRelations.observedUsage]);
  await assert.rejects(journal.finishAttempt({ attemptId, outcome: "completed",
    telemetryStatus: "incomplete", result: { output: { finding: "Café rollback must be atomic." } } }),
  /Synthetic observer failure after ledger write/);
  const after = await snapshotRows(ownerPool, [contentRelations.contentVersions, contentRelations.observedUsage]);
  assert.deepEqual(after, before, "the observer and attempt update roll back together");
  const persisted = await journal.inspectRun({ runId: run.runId });
  assert.equal(persisted.assignments[0].attempts[0].outcome, null);
  return true;
}

async function recordContradictoryAssessments({ contentLedger, runtimeDatabase, ownerDatabase, relations, contentVersionId }) {
  const definition = contentLedger.assessments.normalizeAssessmentDefinition({
    applicationNamespace,
    definitionId: "archive-curator-judgment",
    version: 1,
    schemaProfile: contentLedger.capture.CONTENT_LEDGER_POSTGRES_SCHEMA_PROFILE,
    resultSchema: { type: "object", additionalProperties: false, properties: {
      verdict: { type: "string", enum: ["retain", "discard"] },
      confidence: { type: "number", minimum: 0, maximum: 1 },
      rationale: { type: "string", minLength: 1 },
    }, required: ["verdict", "confidence", "rationale"] },
  }, { recordedAt });
  const writer = contentLedger.assessments.createPostgresContentAssessmentWriter({ relations });
  await ownerDatabase.transaction(client => writer.insertDefinition(client, definition));
  const inputs = [
    { assessmentKey: "archive-record-0042-review-a", evaluator: "curator-a", verdict: "retain",
      rationale: "The selected synthetic analysis finding is useful for archive review." },
    { assessmentKey: "archive-record-0042-review-b", evaluator: "curator-b", verdict: "discard",
      rationale: "The selected synthetic analysis finding is too narrow to justify retention alone." },
  ];
  const assessments = inputs.map(({ assessmentKey, evaluator, verdict, rationale }) =>
    contentLedger.assessments.normalizeContentAssessment({
      applicationNamespace,
      assessmentKey,
      contentVersionId,
      evaluatorIdentity: { kind: "human-curator", id: evaluator },
      sourceReference: { kind: "review-event", id: `event:${evaluator}:${assessmentKey}` },
      assessedAt: recordedAt,
      result: { verdict, confidence: verdict === "retain" ? 0.93 : 0.81, rationale },
    }, { definition, recordedAt }));
  await runtimeDatabase.transaction(async client => {
    for (const assessment of assessments) await writer.insertAssessment(client, assessment);
  });
  const rows = await runtimeDatabase.query(`SELECT result FROM ${relation(relations.assessments)}
    WHERE application_namespace=$1 AND content_version_id=$2 ORDER BY assessment_key`,
  [applicationNamespace, contentVersionId]);
  assert.deepEqual(rows.rows.map(row => row.result.verdict), ["retain", "discard"]);
  return Object.freeze({ definition, assessments: Object.freeze(assessments) });
}

async function seedEligibleTransfer({ contentLedger, runtimeDatabase, ownerDatabase, relations, contentVersionId,
  summarizeAttempt, summarizeOutput, summarizeFinding }) {
  assert.equal(summarizeAttempt.result.output.finding, summarizeFinding,
    "the transfer assessment checks the persisted selected finding leaf");
  assert.deepEqual({ versionId: summarizeOutput.version_id, sourceKind: summarizeOutput.source_kind,
    sourceRecordId: summarizeOutput.source_record_id, sourceField: summarizeOutput.source_field,
    assignmentId: summarizeOutput.assignment_id, attemptId: summarizeOutput.attempt_id,
    threadId: summarizeOutput.thread_id, turnId: summarizeOutput.turn_id }, {
    versionId: contentVersionId, sourceKind: "attempt-final-output",
    sourceRecordId: summarizeAttempt.attemptId, sourceField: "result.output.finding",
    assignmentId: summarizeAttempt.assignmentId, attemptId: summarizeAttempt.attemptId,
    threadId: summarizeAttempt.threadId, turnId: summarizeAttempt.turnId,
  }, "the selected finding has exact source and attempt lineage");
  assert.equal(summarizeOutput.content_sha256, sha256(summarizeFinding),
    "the selected finding capture matches the persisted UTF-8 bytes");
  assert.equal(Number(summarizeOutput.byte_length), Buffer.byteLength(summarizeFinding, "utf8"),
    "the selected finding capture has the exact UTF-8 byte length");
  const transferPersistence = contentLedger.transfers.createPostgresContentTransferPersistence({ relations });
  const policy = contentLedger.transfers.normalizeTransferPolicyDefinition({
    applicationNamespace,
    policyId: "archive-preservation-transfer-policy",
    version: 1,
    configSchema: { type: "object", additionalProperties: false, properties: {
      retentionClass: { type: "string", enum: ["permanent", "long-term"] },
      recipientTier: { type: "string", enum: ["regional", "national"] },
    }, required: ["retentionClass", "recipientTier"] },
    config: { retentionClass: "permanent", recipientTier: "regional" },
    recordedAt,
  });
  const assessmentDefinition = contentLedger.transfers.normalizeTransferAssessmentDefinition({
    applicationNamespace,
    definitionId: "archive-transfer-finding-capture-review",
    version: 1,
    resultSchema: { type: "object", additionalProperties: false, properties: {
      findingSha256: { type: "string", minLength: 64, maxLength: 64 },
      findingByteLength: { type: "integer", minimum: 0 },
      sourceKind: { type: "string", enum: ["attempt-final-output"] },
      sourceRecordId: { type: "string", minLength: 1 },
      sourceField: { type: "string", enum: ["result.output.finding"] },
    }, required: ["findingSha256", "findingByteLength", "sourceKind", "sourceRecordId", "sourceField"] },
    recordedAt,
  });
  await ownerDatabase.transaction(async client => {
    await transferPersistence.insertTransferPolicyDefinition(client, policy);
    await transferPersistence.insertTransferAssessmentDefinition(client, assessmentDefinition);
  });
  const request = contentLedger.transfers.normalizeTransferRequest({
    applicationNamespace,
    contentVersionId,
    destination,
    policyId: policy.policyId,
    policyVersion: policy.policyVersion,
    requesterIdentity: requester,
    retryKey: "archive-transfer-request-0042",
    recordedAt,
  }, { policyDefinition: policy, recordedAt });
  const assessment = contentLedger.transfers.normalizeTransferAssessment({
    applicationNamespace,
    assessmentDefinitionId: assessmentDefinition.assessmentDefinitionId,
    assessmentDefinitionVersion: assessmentDefinition.assessmentDefinitionVersion,
    contentVersionId,
    requestId: request.requestId,
    destination,
    evaluatorIdentity: { kind: "service", id: "archive-finding-capture-service" },
    outcome: "passed",
    result: { findingSha256: summarizeOutput.content_sha256,
      findingByteLength: Number(summarizeOutput.byte_length), sourceKind: summarizeOutput.source_kind,
      sourceRecordId: summarizeOutput.source_record_id, sourceField: summarizeOutput.source_field },
    executionReference: summarizeAttempt.attemptId,
    recordedAt,
  }, { assessmentDefinition, recordedAt });
  const authorization = { status: "authorized", checkedAt: recordedAt,
    authorityVersion: "archive-permission-v7", evidence: { permissionId: "archive-grant-0042" } };
  const decision = contentLedger.transfers.normalizeTransferDecision({
    applicationNamespace,
    requestId: request.requestId,
    contentVersionId,
    destination,
    policyId: policy.policyId,
    policyVersion: policy.policyVersion,
    assessments: [assessment],
    authorization,
    outcome: "eligible",
    recordedAt,
  }, { request, assessments: [assessment], authorization, recordedAt });
  assert.equal(decision.deliveryEnabled, false);
  await runtimeDatabase.transaction(async client => {
    await transferPersistence.insertTransferRequest(client, request);
    await transferPersistence.insertTransferAssessment(client, assessment);
    await transferPersistence.insertTransferDecision(client, decision);
  });
  return Object.freeze({ request, assessment, decision, transferPersistence });
}

async function exerciseDelivery({ contentLedger, runtimeDatabase, ownerPool, runtimePool,
  relations, deliveryAttempts, request, decision, applicationSchema, runtimeRole,
  summarizeAttemptId, expectedFinding }) {
  const bodyLoader = async (client, metadata, exactRequest) => {
    if (metadata.source.kind !== "attempt-final-output"
      || metadata.source.recordId !== summarizeAttemptId
      || metadata.source.field !== "result.output.finding"
      || exactRequest.contentVersionId !== metadata.versionId) return null;
    const result = await client.query(`SELECT body_text FROM ${q(applicationSchema)}.archive_payloads
      WHERE application_namespace=$1 AND record_id=$2 AND content_version_id=$3`,
    [metadata.applicationNamespace, metadata.source.recordId, metadata.versionId]);
    return result.rows[0]?.body_text ?? null;
  };
  const store = contentLedger.delivery.createPostgresContentTransferDeliveryPersistence({
    database: runtimeDatabase,
    transferRelations: relations,
    deliveryAttempts,
    loadContentBody: bodyLoader,
  });
  const adapterCalls = [];
  const destinationReceipts = [];
  const adapterOutcomes = new Map([
    ["archive-attempt-lock-owner", "failed"],
    ["archive-attempt-retry", "delivered"],
  ]);
  let heldEffectKey = null;
  let effectEntered;
  let releaseEffect;
  let effectGate = Promise.resolve();
  const adapter = {
    applicationNamespace,
    destinationKind: destination.kind,
    destinationAction: destination.action,
    adapterId: "synthetic-local-archive",
    adapterVersion: 1,
    async deliver(input) {
      const visible = await ownerPool.query(`SELECT id,application_namespace,decision_id,request_id,
        content_version_id,destination_kind,destination_id,destination_action,adapter_id,adapter_version,
        attempt_key,identity_sha256 FROM ${relation(deliveryAttempts)} WHERE id=$1`, [input.idempotencyKey]);
      assert.equal(visible.rows.length, 1, "durable delivery claim is visible before the local adapter effect");
      const claim = visible.rows[0];
      assert.deepEqual({ id: claim.id, applicationNamespace: claim.application_namespace, decisionId: claim.decision_id,
        requestId: claim.request_id, contentVersionId: claim.content_version_id,
        destinationKind: claim.destination_kind, destinationId: claim.destination_id,
        destinationAction: claim.destination_action, adapterId: claim.adapter_id,
        adapterVersion: claim.adapter_version, attemptKey: claim.attempt_key,
        identityHash: claim.identity_sha256 }, {
        id: input.idempotencyKey,
        applicationNamespace, decisionId: decision.decisionId, requestId: request.requestId,
        contentVersionId: request.contentVersionId, destinationKind: destination.kind,
        destinationId: destination.id, destinationAction: destination.action,
        adapterId: "synthetic-local-archive", adapterVersion: 1, attemptKey: expectedAttemptKey,
        identityHash: expectedClaimIdentity(request, decision, expectedAttemptKey),
      });
      assert.equal(input.content, expectedFinding,
        "the local adapter receives the exact persisted selected finding leaf");
      assert.equal(sha256(input.content), input.contentVersion.capture.sha256,
        "the installed delivery path receives bytes matching the captured SHA-256");
      adapterCalls.push({ attemptId: input.idempotencyKey, attemptKey: claim.attempt_key });
      if (claim.attempt_key === heldEffectKey) {
        effectEntered?.();
        await effectGate;
      }
      const next = adapterOutcomes.get(claim.attempt_key) ?? "delivered";
      if (next === "failed") return { outcome: "failed", noEffect: true, failureCode: "local-store-temporary-failure",
        details: { noEffect: true } };
      const receiptId = `local-archive-receipt-${input.idempotencyKey.slice(-16)}`;
      await ownerPool.query(`INSERT INTO ${q(applicationSchema)}.local_delivery_receipts
        (receipt_id,attempt_id,request_id,content_version_id,destination_id,body_sha256,recorded_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7)`, [receiptId, input.idempotencyKey, input.request.requestId,
        input.contentVersion.versionId, input.destination.id, sha256(input.content), recordedAt]);
      destinationReceipts.push(receiptId);
      return { outcome: "delivered", details: { localReceiptId: receiptId } };
    },
  };
  const createCoordinator = currentStore => contentLedger.delivery.createContentTransferDeliveryCoordinator({
    store: currentStore,
    accessControl: { assertContext(context) {
      if (context?.session !== trustedContext.session) throw new TypeError("Synthetic archive session is invalid.");
      return { kind: requester.kind, principalId: requester.id };
    } },
    registrations: [adapter],
    async authorizeDestination({ context, request: transferRequest }) {
      if (context?.session !== trustedContext.session) {
        return { status: "unauthorized", authorityVersion: null, evidence: { reason: "invalid-session" } };
      }
      const result = await runtimePool.query(`SELECT enabled,authority_version FROM ${q(applicationSchema)}.recipient_permissions
        WHERE principal_kind=$1 AND principal_id=$2 AND destination_kind=$3 AND destination_id=$4
          AND destination_action=$5`, [requester.kind, requester.id, transferRequest.destination.kind,
        transferRequest.destination.id, transferRequest.destination.action]);
      const row = result.rows[0];
      return { status: row?.enabled ? "authorized" : "unauthorized",
        authorityVersion: row ? `archive-permission-v${row.authority_version}` : "no-current-permission",
        evidence: { permissionPresent: Boolean(row), enabled: row?.enabled ?? false } };
    },
    clock: () => new Date(recordedAt),
  });
  const coordinator = createCoordinator(store);
  const command = attemptKey => ({ context: trustedContext, applicationNamespace,
    requestId: request.requestId, attemptKey });
  let expectedAttemptKey = "archive-attempt-lock-owner";
  let releaseClaim;
  let claimInserted;
  let signalEffectEntered;
  const claimGate = new Promise(resolve => { releaseClaim = resolve; });
  const claimReady = new Promise(resolve => { claimInserted = resolve; });
  const effectReady = new Promise(resolve => { signalEffectEntered = resolve; });
  effectEntered = signalEffectEntered;
  effectGate = new Promise(resolve => { releaseEffect = resolve; });
  heldEffectKey = expectedAttemptKey;
  let holdClaim = true;
  const gatedStore = Object.freeze({
    ...store,
    async insertAttemptClaim(client, claim) {
      const inserted = await store.insertAttemptClaim(client, claim);
      if (holdClaim && claim.attemptKey === "archive-attempt-lock-owner") {
        holdClaim = false;
        claimInserted(claim);
        await claimGate;
      }
      return inserted;
    },
  });
  const firstCoordinator = createCoordinator(gatedStore);
  let firstRun;
  let contenderBlocked;
  try {
    firstRun = firstCoordinator.deliver(command(expectedAttemptKey));
    const visibleClaim = await claimReady;
    assert.equal(visibleClaim.attemptKey, expectedAttemptKey);
    const adapterCallsBeforeContender = adapterCalls.length;
    const contenderRun = coordinator.deliver(command("archive-attempt-lock-contender"));
    contenderBlocked = assert.rejects(contenderRun, error => error.code === "TRANSFER_BLOCKED");
    const lockWait = await waitForRequestLock(ownerPool, runtimeRole, relations.requests);
    assert.ok(lockWait, "the second coordinator waits on the request row lock in PostgreSQL");
    releaseClaim();
    await effectReady;
    await contenderBlocked;
    assert.equal(adapterCalls.length, adapterCallsBeforeContender + 1,
      "the unresolved competing claim is rejected before a second adapter call");
  } finally {
    releaseClaim();
    releaseEffect();
    await firstRun?.catch(() => undefined);
    await contenderBlocked?.catch(() => undefined);
  }
  const failed = await firstRun;
  assert.equal(failed.receipt.outcome, "failed");
  assert.equal(failed.receipt.failureCode, "local-store-temporary-failure");
  assert.equal(failed.receipt.details.noEffect, true);
  const failedReplay = await coordinator.deliver(command(expectedAttemptKey));
  assert.equal(failedReplay.replayed, true);
  assert.equal(adapterCalls.length, 1, "exact receipt replay does not repeat the local effect");
  expectedAttemptKey = "archive-attempt-retry";
  const recovered = await coordinator.deliver(command(expectedAttemptKey));
  assert.equal(recovered.receipt.outcome, "delivered");
  assert.equal(destinationReceipts.length, 1, "a new permitted attempt recovers from the no-effect failure");
  const deliveredReplay = await coordinator.deliver(command(expectedAttemptKey));
  assert.equal(deliveredReplay.replayed, true);
  assert.equal(deliveredReplay.receipt.deliveryOutcomeId, recovered.receipt.deliveryOutcomeId);
  assert.equal(adapterCalls.length, 2);
  await ownerPool.query(`UPDATE ${q(applicationSchema)}.recipient_permissions SET enabled=false
    WHERE permission_id='archive-grant-0042'`);
  const authorizationRevokedReplay = await coordinator.deliver(command(expectedAttemptKey));
  assert.equal(authorizationRevokedReplay.replayed, true);
  assert.equal(authorizationRevokedReplay.authorizationFresh, true);
  assert.equal(authorizationRevokedReplay.detailsWithheld, true,
    "replay rechecks current application authorization and withholds the old receipt details");
  assert.equal(adapterCalls.length, 2, "revoked receipt replay does not resend");
  const stored = await ownerPool.query(`SELECT outcome,attempt_key FROM ${relation(relations.deliveryOutcomes)}
    WHERE request_id=$1 ORDER BY attempt_key`, [request.requestId]);
  assert.deepEqual(stored.rows.map(row => [row.attempt_key, row.outcome]), [
    ["archive-attempt-lock-owner", "failed"], ["archive-attempt-retry", "delivered"],
  ]);
  const claimIds = await ownerPool.query(`SELECT id,attempt_key FROM ${relation(deliveryAttempts)}
    WHERE request_id=$1 ORDER BY attempt_key`, [request.requestId]);
  assert.deepEqual(claimIds.rows.map(row => row.attempt_key), ["archive-attempt-lock-owner", "archive-attempt-retry"]);
  return Object.freeze({
    outcomeIds: Object.freeze([failed.receipt.deliveryOutcomeId, recovered.receipt.deliveryOutcomeId]),
    attemptIds: Object.freeze(claimIds.rows.map(row => row.id)),
    receiptCount: destinationReceipts.length,
    retriesRecovered: true,
  });
}

async function waitForRequestLock(pool, role, descriptor) {
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline) {
    const result = await pool.query(`SELECT wait_event_type,query FROM pg_catalog.pg_stat_activity
      WHERE usename=$1 AND wait_event_type='Lock'`, [role]);
    const waiting = result.rows.find(row => typeof row.query === "string"
      && row.query.includes(descriptor.name) && /FOR UPDATE/i.test(row.query));
    if (waiting) return waiting;
    await new Promise(resolve => setTimeout(resolve, 25));
  }
  return null;
}

function expectedClaimIdentity(request, decision, attemptKey) {
  const identity = {
    applicationNamespace,
    decisionId: decision.decisionId,
    requestId: request.requestId,
    contentVersionId: request.contentVersionId,
    destination,
    adapterId: "synthetic-local-archive",
    adapterVersion: 1,
    attemptKey,
  };
  return sha256(stableJson(identity));
}

async function assertBootstrapRollback({ ownerPool, ownerDatabase, contentLedger, schemas, roles, contentRelations }) {
  const failingDatabase = {
    query: ownerPool.query.bind(ownerPool),
    async transaction(work) {
      return ownerDatabase.transaction(async client => {
        await work(client);
        throw new Error("synthetic archive bootstrap failure after DDL and grants");
      });
    },
  };
  await assert.rejects(contentLedger.transfers.bootstrapContentTransferDeliveryPostgres({
    database: failingDatabase,
    schema: schemas.rollback,
    runtimeRole: roles.runtime,
    readerRole: roles.reader,
    contentVersions: contentRelations.contentVersions,
  }), /synthetic archive bootstrap failure after DDL and grants/);
  await assertSchemaAbsent(ownerPool, schemas.rollback);
}

async function assertPrivileges({ ownerPool, runtimePool, readerPool, roles, journalRelations,
  contentRelations, assessmentRelations, transferRelations, deliveryAttempts, applicationSchema }) {
  const journalUpdates = new Set(["agent_workflow_runs", "agent_workflow_assignments",
    "agent_workflow_attempts", "agent_workflow_staged_commands"]);
  for (const descriptor of Object.values(journalRelations)) {
    await assertReadBoundaries(ownerPool, descriptor, roles, {
      runtimeInsert: true,
      runtimeUpdate: journalUpdates.has(descriptor.name),
    });
  }
  for (const descriptor of [contentRelations.extensionDefinitions, contentRelations.contentVersions,
    contentRelations.observedUsage, contentRelations.relationships]) {
    await assertReadBoundaries(ownerPool, descriptor, roles, {
      runtimeInsert: [contentRelations.contentVersions.name, contentRelations.observedUsage.name,
        contentRelations.relationships.name].includes(descriptor.name),
    });
  }
  for (const descriptor of Object.values(assessmentRelations)) {
    await assertReadBoundaries(ownerPool, descriptor, roles, {
      runtimeInsert: descriptor.name === assessmentRelations.assessments.name,
    });
  }
  const transferTables = [...Object.values(transferRelations).filter(descriptor => descriptor.schema === transferRelations.requests.schema),
    deliveryAttempts];
  const transferInsertTables = new Set([transferRelations.assessments.name, transferRelations.requests.name,
    transferRelations.decisions.name, transferRelations.deliveryOutcomes.name, deliveryAttempts.name]);
  for (const descriptor of transferTables) {
    await assertReadBoundaries(ownerPool, descriptor, roles, {
      runtimeInsert: transferInsertTables.has(descriptor.name),
      runtimeUpdateColumns: [transferRelations.requests.name, deliveryAttempts.name].includes(descriptor.name)
        ? ["id"] : [],
    });
  }
  for (const descriptor of [transferRelations.requests, deliveryAttempts]) {
    const table = relation(descriptor);
    assert.equal((await ownerPool.query("SELECT has_table_privilege($1,$2,'UPDATE') AS allowed",
      [roles.runtime, table])).rows[0].allowed, false,
    "runtime row locking has no table-wide UPDATE grant");
    const updates = await ownerPool.query(`SELECT attname::text AS name,
      has_column_privilege($1,$2,attname::text,'UPDATE') AS allowed
      FROM pg_catalog.pg_attribute WHERE attrelid=$2::regclass AND attnum>0 AND NOT attisdropped ORDER BY attnum`,
    [roles.runtime, table]);
    assert.deepEqual(updates.rows.filter(row => row.allowed).map(row => row.name), ["id"],
      "row locking is limited to UPDATE(id)");
  }
  const external = await ownerPool.query(`SELECT
    has_table_privilege($1,$3,'SELECT') AS runtime_select,
    has_table_privilege($2,$3,'SELECT') AS reader_select,
    has_table_privilege($1,$3,'INSERT') AS runtime_insert,
    has_table_privilege($2,$3,'INSERT') AS reader_insert
    FROM (SELECT 1) AS unused`, [roles.runtime, roles.reader, relation(contentRelations.contentVersions)]);
  assert.deepEqual(external.rows[0], { runtime_select: true, reader_select: true,
    runtime_insert: true, reader_insert: false });
  assert.equal((await runtimePool.query("SELECT has_parameter_privilege(current_user,'session_replication_role','SET') AS allowed")).rows[0].allowed,
    false, "runtime cannot disable PostgreSQL checks");
  assert.equal((await readerPool.query("SELECT has_parameter_privilege(current_user,'session_replication_role','SET') AS allowed")).rows[0].allowed,
    false, "reader cannot disable PostgreSQL checks");
  const owner = (await ownerPool.query("SELECT current_user AS role_name")).rows[0].role_name;
  const ownedRelations = [...Object.values(journalRelations), contentRelations.extensionDefinitions,
    contentRelations.contentVersions, contentRelations.observedUsage, contentRelations.relationships,
    ...Object.values(assessmentRelations), ...transferTables];
  for (const descriptor of ownedRelations) {
    const result = await ownerPool.query(`SELECT pg_catalog.pg_get_userbyid(c.relowner) AS owner
      FROM pg_catalog.pg_class c WHERE c.oid=pg_catalog.to_regclass($1)`, [relation(descriptor)]);
    assert.equal(result.rows[0]?.owner, owner, `${relation(descriptor)} remains owned by the bootstrap owner`);
  }
  const appOwner = await ownerPool.query(`SELECT pg_catalog.pg_get_userbyid(n.nspowner) AS owner
    FROM pg_catalog.pg_namespace n WHERE n.nspname=$1`, [applicationSchema]);
  assert.equal(appOwner.rows[0]?.owner, owner, "the synthetic application schema remains application-owned");
}

async function assertReadBoundaries(pool, descriptor, roles, {
  runtimeInsert = false, runtimeUpdate = false, runtimeUpdateColumns = [],
} = {}) {
  const target = relation(descriptor);
  const result = await pool.query(`SELECT
    has_schema_privilege($1,$3,'USAGE') AS runtime_usage,
    has_schema_privilege($2,$3,'USAGE') AS reader_usage,
    has_table_privilege($1,$4,'SELECT') AS runtime_select,
    has_table_privilege($2,$4,'SELECT') AS reader_select,
    has_table_privilege($1,$4,'INSERT') AS runtime_insert,
    has_table_privilege($2,$4,'INSERT') AS reader_insert,
    has_table_privilege($1,$4,'UPDATE') AS runtime_update,
    has_table_privilege($2,$4,'UPDATE') AS reader_update,
    has_table_privilege($1,$4,'DELETE') AS runtime_delete,
    has_table_privilege($2,$4,'DELETE') AS reader_delete,
    has_table_privilege($1,$4,'TRUNCATE') AS runtime_truncate,
    has_table_privilege($2,$4,'TRUNCATE') AS reader_truncate`, [roles.runtime, roles.reader, descriptor.schema, target]);
  const row = result.rows[0];
  assert.equal(row.runtime_usage, true, `${target}: runtime schema usage`);
  assert.equal(row.reader_usage, true, `${target}: reader schema usage`);
  assert.equal(row.runtime_select, true, `${target}: runtime read`);
  assert.equal(row.reader_select, true, `${target}: reader read`);
  assert.equal(row.runtime_insert, runtimeInsert, `${target}: runtime INSERT grant matches the operation contract`);
  assert.equal(row.reader_insert, false, `${target}: reader cannot insert`);
  assert.equal(row.runtime_update, runtimeUpdate, `${target}: runtime table UPDATE grant matches the operation contract`);
  assert.equal(row.reader_update, false, `${target}: reader cannot update`);
  assert.equal(row.runtime_delete, false, `${target}: runtime cannot delete`);
  assert.equal(row.reader_delete, false, `${target}: reader cannot delete`);
  assert.equal(row.runtime_truncate, false, `${target}: runtime cannot truncate`);
  assert.equal(row.reader_truncate, false, `${target}: reader cannot truncate`);
  const columnRights = await pool.query(`SELECT a.attname::text AS name,
    has_column_privilege($1,$3,a.attname::text,'INSERT') AS runtime_insert,
    has_column_privilege($2,$3,a.attname::text,'INSERT') AS reader_insert,
    has_column_privilege($1,$3,a.attname::text,'UPDATE') AS runtime_update,
    has_column_privilege($2,$3,a.attname::text,'UPDATE') AS reader_update
    FROM pg_catalog.pg_attribute a WHERE a.attrelid=$3::regclass
      AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum`, [roles.runtime, roles.reader, target]);
  assert.deepEqual(columnRights.rows.filter(value => value.reader_insert || value.reader_update), [],
    `${target}: reader has no effective column-level writes`);
  if (runtimeInsert) {
    assert.ok(columnRights.rows.every(value => value.runtime_insert),
      `${target}: granted runtime INSERT applies to the application columns`);
  } else {
    assert.deepEqual(columnRights.rows.filter(value => value.runtime_insert), [],
      `${target}: runtime has no column-level INSERT grant`);
  }
  if (!runtimeUpdate) {
    assert.deepEqual(columnRights.rows.filter(value => value.runtime_update).map(value => value.name), runtimeUpdateColumns,
      `${target}: runtime UPDATE is restricted to the named columns`);
  }
}

async function assertImmutableRows({ ownerPool, contentRelations, assessmentRelations, transferRelations,
  deliveryAttempts, firstContent, assessment, delivery }) {
  const probes = [
    [contentRelations.contentVersions, "id", firstContent.versionId],
    [contentRelations.observedUsage, "id", firstContent.normalized.usageId],
    [contentRelations.relationships, "id", null],
    [assessmentRelations.assessments, "id", assessment.assessments[0].assessmentId],
    [transferRelations.decisions, "id", null],
    [deliveryAttempts, "id", delivery.attemptIds[0]],
    [transferRelations.deliveryOutcomes, "id", delivery.outcomeIds[0]],
  ];
  for (const [descriptor, key, value] of probes) {
    const condition = value == null ? "true" : `${q(key)}=$1`;
    const params = value == null ? [] : [value];
    const before = await snapshotRows(ownerPool, [descriptor]);
    await assert.rejects(ownerPool.query(`UPDATE ${relation(descriptor)} SET ${q(key)}=${q(key)} WHERE ${condition}`, params),
      error => error.code === "23514", `${descriptor.name} rows reject direct mutation`);
    assert.deepEqual(await snapshotRows(ownerPool, [descriptor]), before,
      `${descriptor.name} keeps its exact row snapshot after blocked mutation`);
  }
}

async function createArchiveReaderSchema(pool, schema, readerRole, onCreated) {
  const archive = q(schema);
  await pool.query(`CREATE SCHEMA ${archive}`);
  onCreated();
  await pool.query(`CREATE TABLE ${archive}.archive_documents (
    id text PRIMARY KEY,
    payload text NOT NULL,
    classification text NOT NULL CHECK (classification IN ('public','restricted'))
  )`);
  await pool.query(`INSERT INTO ${archive}.archive_documents (id,payload,classification)
    VALUES ('approved','Synthetic preservation summary','public'),
      ('oversized',$1,'public'),
      ('restricted','Synthetic restricted note','restricted')`, ["x".repeat(20_000)]);
  await pool.query(`ALTER TABLE ${archive}.archive_documents ENABLE ROW LEVEL SECURITY`);
  await pool.query(`ALTER TABLE ${archive}.archive_documents FORCE ROW LEVEL SECURITY`);
  await pool.query(`CREATE POLICY archive_document_read_policy ON ${archive}.archive_documents
    TO ${q(readerRole)}
    USING (pg_catalog.current_setting('archive.reader_principal',true)='archive-curator'
      AND classification='public')`);
  await pool.query(`GRANT USAGE ON SCHEMA ${archive} TO ${q(readerRole)}`);
  await pool.query(`GRANT SELECT ON TABLE ${archive}.archive_documents TO ${q(readerRole)}`);
}

async function createApplicationSchema(pool, schema, contentVersions, runtimeRole, onCreated) {
  const app = q(schema);
  await pool.query(`CREATE SCHEMA ${app}`);
  onCreated();
  await pool.query(`CREATE TABLE ${app}.archive_payloads (
    application_namespace text NOT NULL,
    record_id text NOT NULL,
    content_version_id text NOT NULL,
    body_text text NOT NULL,
    PRIMARY KEY (application_namespace,record_id,content_version_id),
    FOREIGN KEY (application_namespace,content_version_id)
      REFERENCES ${relation(contentVersions)}(application_namespace,id)
  )`);
  await pool.query(`CREATE TABLE ${app}.recipient_permissions (
    permission_id text PRIMARY KEY,
    principal_kind text NOT NULL,
    principal_id text NOT NULL,
    destination_kind text NOT NULL,
    destination_id text NOT NULL,
    destination_action text NOT NULL,
    enabled boolean NOT NULL,
    authority_version integer NOT NULL,
    UNIQUE (principal_kind,principal_id,destination_kind,destination_id,destination_action)
  )`);
  await pool.query(`CREATE TABLE ${app}.local_delivery_receipts (
    receipt_id text PRIMARY KEY,
    attempt_id text NOT NULL UNIQUE,
    request_id text NOT NULL,
    content_version_id text NOT NULL,
    destination_id text NOT NULL,
    body_sha256 text NOT NULL,
    recorded_at timestamptz NOT NULL
  )`);
  await pool.query(`GRANT USAGE ON SCHEMA ${app} TO ${q(runtimeRole)}`);
  await pool.query(`GRANT SELECT ON TABLE ${app}.archive_payloads, ${app}.recipient_permissions TO ${q(runtimeRole)}`);
}

async function assertSchemaAbsent(pool, schema) {
  assert.equal((await pool.query("SELECT to_regnamespace($1) IS NULL AS absent", [schema])).rows[0].absent, true,
    `${schema} was not partially created`);
}

async function assertRoleAbsent(pool, role) {
  assert.equal((await pool.query("SELECT NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=$1) AS absent",
    [role])).rows[0].absent, true, `${role} already exists`);
}

async function snapshotRows(pool, descriptors) {
  const entries = await Promise.all(descriptors.map(async descriptor => {
    const result = await pool.query(`SELECT COALESCE(jsonb_agg(to_jsonb(row_value) ORDER BY to_jsonb(row_value)::text),
      '[]'::jsonb) AS rows FROM ${relation(descriptor)} AS row_value`);
    return [descriptor.name, result.rows[0].rows];
  }));
  return Object.fromEntries(entries.sort(([left], [right]) => left.localeCompare(right)));
}

function tickingClock() {
  let time = Date.parse(recordedAt);
  return () => new Date(time += 1000).toISOString();
}

function relation({ schema, name }) { return `${q(schema)}.${q(name)}`; }

function q(value) {
  assert.equal(typeof value, "string");
  assert.match(value, /^[A-Za-z_][A-Za-z0-9_]*$/);
  assert.ok(Buffer.byteLength(value, "utf8") <= 63);
  return `"${value}"`;
}

function sha256(value) { return createHash("sha256").update(value).digest("hex"); }

function stableJson(value) {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${stableJson(value[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}
