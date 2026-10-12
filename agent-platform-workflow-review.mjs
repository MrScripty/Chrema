import { createHash, randomUUID } from "node:crypto";
import { normalizeExpectedClaim } from "./agent-platform-claim-bound-journal.mjs";

/**
 * Create a workflow review runtime with its platform dependencies captured at
 * factory creation time. The platform callbacks are intentionally the only
 * configurable seams in this module; the review protocol remains fixed here.
 */
export function createWorkflowReviewFactory({ hashPayload, renderReviewPrompt, validateVerdict } = {}) {
  if (typeof hashPayload !== "function") throw new TypeError("hashPayload is required.");
  if (typeof renderReviewPrompt !== "function") throw new TypeError("renderReviewPrompt is required.");
  if (typeof validateVerdict !== "function") throw new TypeError("validateVerdict is required.");

  return function createWorkflowReview(options = {}) {
    if (!options || typeof options !== "object" || Array.isArray(options)) {
      throw new TypeError("Workflow review options must be an object.");
    }
    if ("database" in options) throw new TypeError("Workflow review database must be supplied through a store.");
    const {
      journal,
      catalog,
      structuredAgent,
      staging,
      store,
      expectedClaim,
      createId = randomUUID,
      now = () => new Date().toISOString(),
    } = options;
    requireMethod(journal, "inspectRun", "AgentWorkflowJournal");
    requireMethod(journal, "createAssignment", "AgentWorkflowJournal");
    requireMethod(journal, "appendEvent", "AgentWorkflowJournal");
    requireMethod(catalog, "get", "WorkflowCatalog");
    requireMethod(catalog, "render", "WorkflowCatalog");
    requireMethod(structuredAgent, "run", "StructuredWorkflowAgent");
    requireMethod(staging, "list", "ResearchStaging");
    const capturedClaim = normalizeExpectedClaim(expectedClaim);
    const reviewStore = store === undefined ? createMemoryReviewStore() : requireReviewStore(store);

    async function review({ runId, manifest, deterministicReport, signal } = {}) {
      signal?.throwIfAborted();
      const id = requireText(runId, "Workflow run id");
      const hash = requireHash(manifest?.manifestHash, "Review manifest hash");
      if (manifest?.runId !== id) throw new TypeError("Review manifest run id does not match the workflow run.");
      if (deterministicReport?.passed !== true || deterministicReport?.manifestHash !== hash) {
        throw new TypeError("Independent review requires a passing deterministic report for the exact manifest.");
      }
      const run = await journal.inspectRun({ runId: id });
      if (run.status !== "ready-for-admission" || run.validatedManifestHash !== hash) {
        throw conflict(`Workflow run ${id} is not ready for independent review of manifest ${hash}.`);
      }
      const workflow = catalog.get(run.workflowVersionId);
      if (workflow.admissionPolicy?.llmReviewRequired !== true) {
        throw new TypeError(`Workflow ${workflow.id} does not declare an independent LLM review gate.`);
      }
      const existing = await readReview(id, hash);
      if (existing) {
        if (existing.workflowVersionId !== workflow.id) throw conflict("A different LLM review already exists for this manifest.");
        await recordReviewEvent(existing);
        return existing;
      }
      const reviewPacket = buildReviewPacket(manifest, await staging.list({ runId: id }));

      const priorThreads = [...new Set(run.assignments
        .flatMap(({ attempts }) => attempts ?? [])
        .map(({ threadId }) => threadId)
        .filter(Boolean))];
      const assignment = await journal.createAssignment({
        runId: id,
        stepKey: `${workflow.workflowType}.llm-review`,
        assignedTo: "independent-reviewer",
        execution: "current-agent",
        targets: [{ type: "staged-manifest", id: hash, expectedVersion: null }],
      });
      const rendered = renderReviewPrompt({ catalog, workflow, run, reviewPacket, deterministicReport });
      let execution;
      try {
        execution = await structuredAgent.run({
          signal,
          runId: id,
          assignmentId: assignment.assignmentId,
          prompt: rendered,
          forbiddenThreadIds: priorThreads,
          validateOutput: (output) => validateVerdict(output, { manifestHash: hash }),
        });
      } catch (error) {
        const current = await journal.inspectRun({ runId: id });
        if (["running", "ready-for-admission"].includes(current.status)) {
          await journal.transitionRun({
            runId: id,
            status: signal?.aborted ? "failed" : "blocked",
            failure: {
              code: signal?.aborted ? signal.reason?.code ?? "EXECUTION_STOPPED" : "LLM_REVIEW_FAILED",
              message: error instanceof Error ? error.message : String(error),
              manifestHash: hash,
            },
          });
        }
        throw error;
      }
      const record = {
        reviewId: createId(),
        runId: id,
        workflowVersionId: workflow.id,
        manifestHash: hash,
        assignmentId: assignment.assignmentId,
        attemptId: execution.attempt.attemptId,
        reviewerThreadId: execution.threadId,
        promptVersionId: rendered.promptVersionId,
        verdict: execution.output.verdict,
        summary: execution.output.summary,
        issues: execution.output.issues,
        outputHash: hashValue(execution.output),
        completedAt: isoTimestamp(now(), "Workflow review completion time"),
      };
      const persisted = await writeReview(record);
      await recordReviewEvent(persisted);
      if (persisted.verdict !== "approved") {
        const block = persisted.verdict === "revision-required" && capturedClaim && journal.blockRunForRevision
          ? input => journal.blockRunForRevision({ ...input, expectedClaim: capturedClaim, outcome: {
            gate: "llm-review", verdict: persisted.verdict, manifestHash: hash, issues: persisted.issues,
          } })
          : input => journal.transitionRun(input);
        await block({
          runId: id,
          status: "blocked",
          failure: {
            code: persisted.verdict === "revision-required" ? "LLM_REVIEW_REVISION_REQUIRED" : "LLM_REVIEW_REJECTED",
            message: persisted.summary,
            manifestHash: hash,
            issues: persisted.issues,
          },
          finalSummary: { reviewVerdict: persisted.verdict, reviewIssueCount: persisted.issues.length },
        });
      }
      return persisted;
    }

    async function recordReviewEvent(record) {
      await journal.appendEvent({
        runId: record.runId,
        assignmentId: record.assignmentId,
        attemptId: record.attemptId,
        sourceKey: `llm-review:${record.reviewId}`,
        eventType: "workflow.llm-review-completed",
        threadId: record.reviewerThreadId,
        occurredAt: record.completedAt,
        payload: {
          reviewId: record.reviewId,
          manifestHash: record.manifestHash,
          promptVersionId: record.promptVersionId,
          verdict: record.verdict,
          issueCodes: record.issues.map(({ code }) => code),
        },
      });
    }

    async function requireApproval({ runId, manifestHash } = {}) {
      const id = requireText(runId, "Workflow run id");
      const hash = requireHash(manifestHash, "Approval manifest hash");
      const record = await readReview(id, hash);
      if (!record || record.verdict !== "approved") throw approvalRequired(id, hash);
      const run = await journal.inspectRun({ runId: id });
      if (record.workflowVersionId !== run.workflowVersionId) throw approvalRequired(id, hash);
      const assignment = run.assignments.find(({ assignmentId }) => assignmentId === record.assignmentId);
      const attempt = assignment?.attempts.find(({ attemptId }) => attemptId === record.attemptId);
      const otherThreads = new Set(run.assignments
        .filter(({ assignmentId }) => assignmentId !== record.assignmentId)
        .flatMap(({ attempts }) => attempts ?? [])
        .map(({ threadId }) => threadId)
        .filter(Boolean));
      const workflow = catalog.get(run.workflowVersionId);
      if (!assignment || assignment.parentAssignmentId !== null
        || assignment.stepKey !== `${workflow.workflowType}.llm-review`
        || assignment.execution !== "current-agent" || assignment.status !== "completed"
        || !attempt || attempt.outcome !== "completed" || attempt.telemetryStatus !== "complete"
        || attempt.threadId !== record.reviewerThreadId || otherThreads.has(record.reviewerThreadId)) {
        throw approvalRequired(id, hash);
      }
      return true;
    }

    async function readReview(runId, manifestHash) {
      return requireReviewRecord(await reviewStore.read({ runId, manifestHash }), runId, manifestHash);
    }

    async function writeReview(record) {
      const inserted = await reviewStore.insertIfAbsent(record);
      if (inserted !== null) {
        const persisted = requireReviewRecord(inserted, record.runId, record.manifestHash);
        if (!sameReviewRecord(persisted, record)) {
          throw conflict("A different LLM review already exists for this manifest.");
        }
        return persisted;
      }
      const existing = await readReview(record.runId, record.manifestHash);
      if (!existing || existing.outputHash !== record.outputHash) throw conflict("A different LLM review already exists for this manifest.");
      return existing;
    }

    function buildReviewPacket(manifest, stagedCommands) {
      if (!Array.isArray(manifest.commands) || !Array.isArray(stagedCommands)) {
        throw new TypeError("Independent review requires manifest and staged command arrays.");
      }
      if (manifest.commands.length !== stagedCommands.length) {
        throw conflict("The staged command set does not match the deterministically validated manifest.");
      }
      const byId = new Map(stagedCommands.map((command) => [command.commandId, command]));
      if (byId.size !== stagedCommands.length) throw conflict("The staged command set contains duplicate command ids.");
      const commands = manifest.commands.map((identity) => {
        const staged = byId.get(identity.commandId);
        if (!staged || staged.payloadHash !== identity.payloadHash
          || hashPayload(staged.payload) !== identity.payloadHash) {
          throw conflict(`Staged command ${identity.commandId} does not match the deterministically validated manifest.`);
        }
        return {
          ...clone(identity),
          payload: clone(staged.payload),
        };
      });
      return { ...clone(manifest), stagedCommands: commands };
    }

    return { review, requireApproval };
  };
}

function createMemoryReviewStore() {
  const records = new Map();
  const key = ({ runId, manifestHash }) => `${runId}:${manifestHash}`;
  return {
    async read(identity) { return clone(records.get(key(identity)) ?? null); },
    async insertIfAbsent(record) {
      const identity = key(record);
      if (records.has(identity)) return null;
      records.set(identity, clone(record));
      return clone(record);
    },
  };
}

function requireReviewStore(store) {
  requireMethod(store, "read", "WorkflowReviewStore");
  requireMethod(store, "insertIfAbsent", "WorkflowReviewStore");
  return store;
}

function requireReviewRecord(record, runId, manifestHash) {
  if (record === null) return null;
  if (!record || typeof record !== "object" || Array.isArray(record)
    || record.runId !== runId || record.manifestHash !== manifestHash
    || !/^[0-9a-f]{64}$/.test(record.outputHash)
    || !["approved", "revision-required", "rejected"].includes(record.verdict)
    || !Array.isArray(record.issues)
    || ["reviewId", "workflowVersionId", "assignmentId", "attemptId", "reviewerThreadId",
      "promptVersionId", "summary", "completedAt"].some((field) =>
      typeof record[field] !== "string" || !record[field].trim())
    || !Number.isFinite(new Date(record.completedAt).getTime())) {
    throw new TypeError("Workflow review store returned an invalid review record.");
  }
  return record;
}

function sameReviewRecord(left, right) {
  const fields = ["reviewId", "runId", "workflowVersionId", "manifestHash", "assignmentId", "attemptId",
    "reviewerThreadId", "promptVersionId", "verdict", "summary", "issues", "outputHash", "completedAt"];
  return fields.every((field) => field === "issues"
    ? stableJson(left[field]) === stableJson(right[field])
    : left[field] === right[field]);
}

function approvalRequired(runId, manifestHash) {
  const error = new Error(`Workflow run ${runId} does not have an approved independent review for manifest ${manifestHash}.`);
  error.code = "WORKFLOW_REVIEW_REQUIRED";
  error.statusCode = 409;
  return error;
}

function conflict(message) {
  const error = new Error(message);
  error.code = "WORKFLOW_CONFLICT";
  error.statusCode = 409;
  return error;
}

function requireMethod(value, method, label) {
  if (!value || typeof value[method] !== "function") throw new TypeError(`${label}.${method} is required.`);
}

function requireText(value, label) {
  if (typeof value !== "string" || !value.trim()) throw new TypeError(`${label} is required.`);
  return value.trim();
}

function requireHash(value, label) {
  const hash = requireText(value, label);
  if (!/^[0-9a-f]{64}$/.test(hash)) throw new TypeError(`${label} must be a lowercase SHA-256 value.`);
  return hash;
}

function isoTimestamp(value, label) {
  const text = requireText(value, label);
  const date = new Date(text);
  if (!Number.isFinite(date.getTime())) throw new TypeError(`${label} must be an ISO timestamp.`);
  return date.toISOString();
}

function hashValue(value) {
  return createHash("sha256").update(stableJson(value)).digest("hex");
}

function stableJson(value) {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  if (value && typeof value === "object") return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stableJson(value[key])}`).join(",")}}`;
  return JSON.stringify(value);
}

function clone(value) {
  return value == null ? value : structuredClone(value);
}
