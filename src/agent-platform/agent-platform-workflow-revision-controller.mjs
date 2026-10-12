import { createHash } from "node:crypto";
import { normalizeExpectedClaim } from "./agent-platform-claim-bound-journal.mjs";

/** Trusted runtime transition; caller outcomes and authorization snapshots prove nothing.
 * Application adapters own carry eligibility/identity and thread continuation.
 * The journal persists child enqueue and parent event as one transition.
 */
export function createWorkflowRevisionControllerFactory(options = {}) {
  const { gates, selectCarryForwardCommands, carriedCommandId, continuationThread } = options;
  for (const [name, dependency] of Object.entries({ selectCarryForwardCommands, carriedCommandId, continuationThread })) {
    if (typeof dependency !== 'function') throw new TypeError(`Revision controller requires ${name}.`);
  }
  if (!Array.isArray(gates) || gates.length === 0 || gates.length > 20) throw new TypeError('Bounded revision gates are required.');
  const gatePolicies = new Map();
  for (const gate of gates) {
    if (!gate || typeof gate !== 'object' || Array.isArray(gate)
      || Object.keys(gate).length !== 3 || !Object.hasOwn(gate, 'id') || !Object.hasOwn(gate, 'defaultVerdict') || !Object.hasOwn(gate, 'requiresRevisionVerdict')
      || typeof gate.id !== 'string' || !/^[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,199}$/.test(gate.id)
      || gatePolicies.has(gate.id) || ![null, 'revision-required'].includes(gate.defaultVerdict)
      || typeof gate.requiresRevisionVerdict !== 'boolean') throw new TypeError('Invalid revision gate policy.');
    gatePolicies.set(gate.id, structuredClone(gate));
  }
  /**
   * Owns the bounded semantic-repair transition. It creates a new immutable run;
   * it never reopens or rewrites the run that produced the failed gate result.
   */
  return function createWorkflowRevisionController({
    journal,
    workflowCatalog,
    staging = null,
    authorizationRecheck,
    expectedClaim,
  } = {}) {
    const capturedClaim = normalizeExpectedClaim(expectedClaim);
    if (typeof authorizationRecheck !== "function") throw new TypeError("Revision authorizationRecheck is required.");
    requireMethod(journal, "inspectRun", "AgentWorkflowJournal");
    requireMethod(journal, "createRunAndAppendEvent", "AgentWorkflowJournal");
    requireMethod(workflowCatalog, "get", "WorkflowCatalog");

    async function advance({ runId, outcome } = {}) {
      const id = requireText(runId, "Workflow run id");
      const normalized = normalizeOutcome(outcome, gatePolicies);
      const run = await journal.inspectRun({ runId: id });
      if (run.status !== "blocked") {
        throw conflict(`Workflow run ${id} must be blocked before semantic revision can be considered.`);
      }
      const policy = normalizePolicy(workflowCatalog.get(run.workflowVersionId)?.revisionPolicy);
      if (!policy) return terminal("revision-policy-unavailable");
      if (normalized.verdict === "rejected") return terminal("review-rejected");
      if (normalized.verdict !== "revision-required" && gatePolicies.get(normalized.gate).requiresRevisionVerdict) {
        return terminal("review-not-revisable");
      }
      const issueCodes = [...new Set(normalized.issues.map(({ code }) => code))];
      if (!issueCodes.length || issueCodes.some((code) => !policy.correctableIssueCodes.has(code))) {
        return terminal("human-review-required", { issueCodes });
      }

      const lineage = await readLineage(journal, run);
      const ordinal = lineage.filter(({ input }) => input?.revision).length + 1;
      if (ordinal > policy.maximumSemanticRevisions) {
        return terminal("revision-budget-exhausted", { maximumSemanticRevisions: policy.maximumSemanticRevisions });
      }
      const issueFingerprint = hashValue(normalized.issues.map(({ code, path, severity }) => ({ code, path, severity })));
      if (lineage.some(({ input }) => input?.revision?.issueFingerprint === issueFingerprint)) {
        return terminal("repeated-issue-set", { issueFingerprint });
      }
      if (await authorizationRecheck({ phase: "revision", run: clone(run), outcome: clone(normalized) }) !== true) {
        return terminal("authorization-revoked");
      }

      const sourceHash = hashValue({ parentRunId: id, ...normalized, issueFingerprint });
      const childRunId = `revision-${sourceHash.slice(0, 40)}`;
      const carryForwardCommandIds = await selectCarryForwardCommands({ staging, runId: id, issues: clone(normalized.issues) });
      if (!Array.isArray(carryForwardCommandIds) || new Set(carryForwardCommandIds).size !== carryForwardCommandIds.length) {
        throw new TypeError("Carry-forward selection must return unique command IDs.");
      }
      const carriedCommands = carryForwardCommandIds.map((parentCommandId) => ({
        parentCommandId: requireText(parentCommandId, "Parent command id"),
        commandId: requireText(carriedCommandId(childRunId, parentCommandId), "Carried command id"),
      }));
      const continuationThreadId = continuationThread(clone(run)) ?? null;
      const revision = {
        schemaVersion: 1,
        ordinal,
        parentRunId: id,
        gate: normalized.gate,
        verdict: normalized.verdict,
        manifestHash: normalized.manifestHash,
        issueFingerprint,
        sourceHash,
        issues: clone(normalized.issues),
        carriedCommands,
      };
      const createRevision = capturedClaim && journal.createRevisionRunAndAppendEvent
        ? input => journal.createRevisionRunAndAppendEvent({ ...input, outcome: normalized, expectedClaim: capturedClaim })
        : input => journal.createRunAndAppendEvent(input);
      await createRevision({ run: {
        runId: childRunId,
        workflowVersionId: run.workflowVersionId,
        owner: clone(run.owner),
        authorization: clone(run.authorization),
        queuePriority: run.queuePriority,
        resumedFromRunId: id,
        input: {
          ...clone(run.input),
          threadId: continuationThreadId,
          revision,
        },
      }, event: {
        runId: id,
        sourceKey: `semantic-revision:${sourceHash}`,
        eventType: "workflow.semantic-revision-created",
        threadId: continuationThreadId ?? undefined,
        payload: { childRunId, ...revision },
      } });
      return Object.freeze({
        kind: "revision-created",
        parentRunId: id,
        childRunId,
        ordinal,
        issueFingerprint,
      });
    }

    return Object.freeze({ advance });
  };
}

async function readLineage(journal, leaf) {
  const result = [];
  const seen = new Set();
  let current = leaf;
  while (current) {
    if (seen.has(current.runId)) throw new Error("Workflow revision lineage contains a cycle.");
    seen.add(current.runId);
    result.push(current);
    current = current.resumedFromRunId
      ? await (journal.inspectAncestorRun ?? journal.inspectRun)({ runId: current.resumedFromRunId })
      : null;
  }
  return result;
}

function normalizeOutcome(value, gatePolicies) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError("Workflow gate outcome is required.");
  }
  const gate = requireText(value.gate, "Workflow gate");
  if (!gatePolicies.has(gate)) {
    throw new TypeError(`Unsupported workflow revision gate: ${gate}.`);
  }
  const verdict = value.verdict == null
    ? gatePolicies.get(gate).defaultVerdict
    : requireText(value.verdict, "Workflow gate verdict");
  const manifestHash = requireHash(value.manifestHash, "Workflow gate manifest hash");
  if (!Array.isArray(value.issues) || value.issues.length > 200) {
    throw new TypeError("Workflow gate issues must be an array with at most 200 entries.");
  }
  const issues = value.issues.map((candidate, index) => {
    const code = requireText(candidate?.code, `Workflow gate issue ${index} code`);
    return {
      ...clone(candidate),
      code,
      path: typeof candidate.path === "string" ? candidate.path : "",
      severity: typeof candidate.severity === "string" ? candidate.severity : "blocking",
    };
  }).sort((left, right) => left.code.localeCompare(right.code)
    || left.path.localeCompare(right.path)
    || left.severity.localeCompare(right.severity));
  return { gate, verdict, manifestHash, issues };
}

function normalizePolicy(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const maximumSemanticRevisions = value.maximumSemanticRevisions;
  if (!Number.isInteger(maximumSemanticRevisions) || maximumSemanticRevisions < 0 || maximumSemanticRevisions > 10) {
    throw new TypeError("Workflow maximumSemanticRevisions must be from 0 through 10.");
  }
  if (!Array.isArray(value.correctableIssueCodes) || value.correctableIssueCodes.length === 0) {
    throw new TypeError("Workflow correctableIssueCodes must contain at least one code.");
  }
  return {
    maximumSemanticRevisions,
    correctableIssueCodes: new Set(value.correctableIssueCodes.map((code, index) => (
      requireText(code, `Workflow correctable issue code ${index}`)
    ))),
  };
}

function terminal(reason, details = {}) {
  return Object.freeze({ kind: "terminal", reason, ...details });
}

function requireMethod(value, name, label) {
  if (!value || typeof value[name] !== "function") throw new TypeError(`${label}.${name} is required.`);
}

function requireText(value, label) {
  if (typeof value !== "string" || !value.trim()) throw new TypeError(`${label} must be a non-empty string.`);
  return value;
}

function requireHash(value, label) {
  if (typeof value !== "string" || !/^[0-9a-f]{64}$/.test(value)) {
    throw new TypeError(`${label} must be a lowercase SHA-256 hash.`);
  }
  return value;
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

function clone(value) {
  return value == null ? value : structuredClone(value);
}

function conflict(message) {
  return Object.assign(new Error(message), { code: "CONFLICT", statusCode: 409 });
}
