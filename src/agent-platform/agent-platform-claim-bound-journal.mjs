const runMutations = [
  "assertExpectedClaim", "createAssignment", "createPreparedAssignment", "observeAssignment", "startAttempt",
  "bindAttempt", "appendEvent", "finishAttempt", "finishAssignment", "transitionRun", "blockRunForRevision",
  "inspectRunCancellation", "confirmRunCancellation",
];

export function claimLostError() {
  return Object.assign(new Error("Workflow claim is no longer held by this worker."), {
    code: "WORKFLOW_CLAIM_LOST",
    statusCode: 409,
  });
}

export function normalizeExpectedClaim(value) {
  if (value === undefined || value === null) return null;
  if (!value || typeof value !== "object" || Array.isArray(value)
    || Object.keys(value).sort().join("|") !== "claimedAt|runId|workerId") {
    throw new TypeError("Expected workflow claim requires runId, workerId, and claimedAt.");
  }
  const runId = requiredText(value.runId, "Claim run id");
  const workerId = requiredText(value.workerId, "Claim worker id");
  const time = Date.parse(value.claimedAt);
  if (!Number.isFinite(time)) throw new TypeError("Claim epoch must be an ISO timestamp.");
  return Object.freeze({ runId, workerId, claimedAt: new Date(time).toISOString() });
}

/** Caller owns the memory serialization boundary; absent claims preserve unbound callers. */
export function assertMemoryExpectedClaim(run, value, clock) {
  const expected = normalizeExpectedClaim(value);
  if (!expected) return;
  assertExactLiveClaim({ runId: run?.runId, workerId: run?.claim?.workerId,
    claimedAt: run?.claim?.claimedAt, expiresAt: run?.claim?.expiresAt }, expected, clock());
}

/**
 * Caller owns the transaction and its established lock order. Relations must be
 * resolved by resolvePostgresRelations. Read database time only AFTER the run
 * lock, including when this transaction already holds that lock.
 */
export async function assertPostgresExpectedClaim(client, relations, runId, value) {
  const expected = normalizeExpectedClaim(value);
  if (!expected) return;
  if (runId !== expected.runId) throw claimLostError();
  const result = await client.query(`
    SELECT claimed_by,claimed_at,lease_expires_at
    FROM ${relations.runs} WHERE id=$1 FOR UPDATE
  `, [runId]);
  const row = result.rows[0];
  if (!row) throw claimLostError();
  const checkedAt = (await client.query("SELECT clock_timestamp() AS checked_at")).rows[0].checked_at;
  assertExactLiveClaim({ runId, workerId: row.claimed_by, claimedAt: row.claimed_at,
    expiresAt: row.lease_expires_at }, expected, checkedAt);
}

function assertExactLiveClaim(actual, expected, checkedAt) {
  const claimedAt = actual.claimedAt == null ? NaN : timestampMillis(actual.claimedAt);
  const expiresAt = actual.expiresAt == null ? NaN : timestampMillis(actual.expiresAt);
  const time = timestampMillis(checkedAt);
  if (actual.runId !== expected.runId || actual.workerId !== expected.workerId
    || claimedAt !== Date.parse(expected.claimedAt) || !Number.isFinite(expiresAt)
    || !Number.isFinite(time) || expiresAt <= time) throw claimLostError();
}

function timestampMillis(value) {
  return value instanceof Date ? value.getTime() : Date.parse(value);
}

/** A per-claim, narrow journal view for worker execution and telemetry. */
export function createClaimBoundJournal(journal, claim, { withRevisionHandoff = operation => operation() } = {}) {
  const expectedClaim = normalizeExpectedClaim(claim);
  if (!expectedClaim || !journal || typeof journal.inspectRun !== "function") {
    throw new TypeError("Claim-bound journal requires a journal and exact claim.");
  }
  function bind(input) {
    assertInput(input);
    if (input.runId !== undefined && input.runId !== expectedClaim.runId) throw claimLostError();
    if (input.expectedClaim !== undefined) {
      const supplied = normalizeExpectedClaim(input.expectedClaim);
      if (!supplied || supplied.runId !== expectedClaim.runId
        || supplied.workerId !== expectedClaim.workerId || supplied.claimedAt !== expectedClaim.claimedAt) throw claimLostError();
    }
    return { ...input, expectedClaim };
  }
  function ownRead(method, input) {
    assertInput(input);
    if (input.runId !== expectedClaim.runId) throw claimLostError();
    if (typeof journal[method] !== "function") throw new TypeError(`Journal ${method} is required.`);
    return journal[method](input);
  }
  const facade = {
    // Enter before opening a transaction: an in-flight heartbeat may need its run lock.
    withRevisionHandoff,
    inspectRun: input => ownRead("inspectRun", input),
    inspectRunFamilyBudget: input => ownRead("inspectRunFamilyBudget", input),
    async inspectAncestorRun(input) {
      assertInput(input);
      const target = requiredText(input.runId, "Ancestor run id");
      const seen = new Set();
      let runId = expectedClaim.runId;
      while (runId) {
        if (seen.has(runId)) throw new Error("Workflow revision lineage contains a cycle.");
        seen.add(runId);
        const run = await journal.inspectRun({ runId });
        if (runId === target) return input.owner ? journal.inspectRun(input) : run;
        runId = run.resumedFromRunId;
      }
      throw claimLostError();
    },
    registerPriceVersion(input) {
      if (typeof journal.registerPriceVersion !== "function") throw new TypeError("Journal registerPriceVersion is required.");
      return journal.registerPriceVersion(bind(input));
    },
  };
  if (typeof journal.inspectAssignment === "function") facade.inspectAssignment = input => ownRead("inspectAssignment", input);
  if (typeof journal.inspectVersion === 'function') facade.inspectVersion = async input => {
    assertInput(input);
    const run = await journal.inspectRun({ runId: expectedClaim.runId });
    if (input.workflowVersionId !== run.workflowVersionId) throw claimLostError();
    return journal.inspectVersion({ workflowVersionId: run.workflowVersionId });
  };
  for (const method of ["appendEventInTransaction", "blockRunForRevisionInTransaction"]) {
    if (typeof journal[method] === "function") facade[method] = (client, input) => journal[method](client, bind(input));
  }
  for (const method of runMutations) {
    if (typeof journal[method] === "function") facade[method] = input => {
      const bound = bind(input);
      const operation = () => journal[method](bound);
      return method === "blockRunForRevision" ? withRevisionHandoff(operation) : operation();
    };
  }
  for (const method of ["createRunAndAppendEvent", "createRevisionRunAndAppendEvent"]) {
    if (typeof journal[method] !== "function") continue;
    facade[method] = input => {
      const bound = bind(input);
      assertInput(input.run);
      assertInput(input.event);
      if (input.run.resumedFromRunId !== expectedClaim.runId || input.event.runId !== expectedClaim.runId) throw claimLostError();
      return journal[method]({ ...bound, event: bind(input.event) });
    };
  }
  return Object.freeze(facade);
}

function assertInput(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new TypeError("Claim-bound journal input must be an object.");
}

function requiredText(value, label) {
  if (typeof value !== "string" || !value.trim()) throw new TypeError(`${label} is required.`);
  return value.trim();
}
