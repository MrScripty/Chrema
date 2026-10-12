import { createHash } from "node:crypto";
import { normalizeExpectedClaim, assertMemoryExpectedClaim, assertPostgresExpectedClaim } from "./agent-platform-claim-bound-journal.mjs";
import { resolvePostgresRelations } from "./postgres-relations.mjs";

const postgresRelationKeys = ["runs", "assignments", "attempts", "stagedCommands"];

/** Inactive proposals only. Payload semantics and expected-version sources belong
 * to the application. Persistence checks active lineage and immutable identity;
 * staging is neither canonical admission nor disclosure approval.
 */
export function createWorkflowStagingFactory({ commandTypes: types, validatePayload,
  expectedVersions: deriveExpectedVersions, commandLabel = 'command', postgresRelations } = {}) {
  if (!Array.isArray(types) || types.length === 0
    || types.some(type => typeof type !== 'string' || !type.trim() || type !== type.trim())
    || new Set(types).size !== types.length) throw new TypeError('Unique staging command types are required.');
  if (typeof validatePayload !== 'function' || typeof deriveExpectedVersions !== 'function') {
    throw new TypeError('Staging requires payload validation and expected-version derivation.');
  }
  if (typeof commandLabel !== 'string' || !commandLabel.trim()) throw new TypeError('Staging command label is required.');
  const commandTypes = new Set(types);
  const resolvedRelations = postgresRelations === undefined ? null
    : resolvePostgresRelations(postgresRelations, postgresRelationKeys, "Workflow staging");
return function createWorkflowStaging({ database, journal, expectedClaim, now = () => new Date().toISOString() } = {}) {
  const claim = normalizeExpectedClaim(expectedClaim);
  if (database) return createPostgresStaging({ database, now, relations: resolvedRelations, expectedClaim: claim });
  if (!journal?.inspectRun) throw new TypeError("Memory staging requires an AgentWorkflowJournal.");
  return createMemoryStaging({ journal, now, expectedClaim: claim });
};
function createMemoryStaging({ journal, now, expectedClaim }) {
  const commandsByRun = new Map();
  const commandsById = new Map();
  let queue = Promise.resolve();
  const transact = (operation) => {
    const task = queue.then(operation, operation);
    queue = task.catch(() => undefined);
    return task;
  };
  const readJournal = journal;
  function view(journal, now, expectedClaim) {
    return Object.freeze({
      forClaim({ expectedClaim: claim, journal: boundJournal = journal, now: clock = now } = {}) {
        return view(boundJournal, clock, requireClaim(claim));
      },
      stage(input) {
        return transact(async () => {
          const normalized = normalizeStagedCommand(input, now());
          const run = await journal.inspectRun({ runId: normalized.runId });
          if (expectedClaim && typeof journal.assertExpectedClaim === "function") {
            await journal.assertExpectedClaim({ runId: normalized.runId, expectedClaim });
          } else {
            assertMemoryExpectedClaim(run, expectedClaim, now);
          }
          assertAttemptLineage(run, normalized);
          const commands = commandsByRun.get(normalized.runId) ?? [];
          const existing = commandsById.get(normalized.commandId);
          if (existing) {
            if (existing.payloadHash !== normalized.payloadHash) {
              throw conflict(`Staged command ${normalized.commandId} already has a different payload.`);
            }
            assertSameIdentity(existing, normalized);
            return clone(existing);
          }
          normalized.ordinal = commands.length + 1;
          commands.push(normalized);
          commandsById.set(normalized.commandId, normalized);
          commandsByRun.set(normalized.runId, commands);
          return clone(normalized);
        });
      },
      list(input) {
        return transact(async () => {
          const runId = requireText(input?.runId, "Workflow run id");
          await readJournal.inspectRun({ runId });
          return clone(commandsByRun.get(runId) ?? []);
        });
      },
    });
  }
  return view(journal, now, expectedClaim);
}

function createPostgresStaging({ database, now, relations, expectedClaim }) {
  if (typeof database.query !== "function" || typeof database.transaction !== "function") {
    throw new TypeError("PostgreSQL staging requires a transaction-capable database.");
  }
  relations ??= resolvePostgresRelations(undefined, postgresRelationKeys, "Workflow staging");
  return Object.freeze({
    forClaim({ expectedClaim: claim, now: clock = now } = {}) {
      return createPostgresStaging({ database, now: clock, relations, expectedClaim: requireClaim(claim) });
    },
    async stage(input) {
      const normalized = normalizeStagedCommand(input, now());
      return database.transaction(async (client) => {
        // Lock command identity before lineage rows: duplicate IDs can target distinct runs.
        await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))", [`workflow-staged-command:${normalized.commandId}`]);
        const lineage = await client.query(`
          SELECT r.status AS run_status,a.status AS assignment_status,t.outcome AS attempt_outcome
          FROM ${relations.runs} r
          JOIN ${relations.assignments} a ON a.run_id=r.id AND a.id=$2
          JOIN ${relations.attempts} t
            ON t.run_id=r.id AND t.assignment_id=a.id AND t.id=$3
          WHERE r.id=$1
          FOR UPDATE OF r,a,t
        `, [normalized.runId, normalized.assignmentId, normalized.attemptId]);
        await assertPostgresExpectedClaim(client, relations, normalized.runId, expectedClaim);
        if (!lineage.rows[0]) throw new TypeError("Staged command attempt does not match its run and assignment.");
        if (lineage.rows[0].run_status !== "running") {
          throw conflict(`Workflow run ${normalized.runId} is not accepting staged commands.`);
        }
        if (lineage.rows[0].assignment_status !== "running" || lineage.rows[0].attempt_outcome !== null) {
          throw conflict(`Workflow attempt ${normalized.attemptId} is not active.`);
        }
        const existing = await client.query(`
          SELECT * FROM ${relations.stagedCommands} WHERE id=$1
        `, [normalized.commandId]);
        if (existing.rows[0]) {
          if (existing.rows[0].payload_sha256 !== normalized.payloadHash) {
            throw conflict(`Staged command ${normalized.commandId} already has a different payload.`);
          }
          const stored = mapStagedRow(existing.rows[0]);
          assertSameIdentity(stored, normalized);
          return stored;
        }
        const ordinal = await client.query(`
          SELECT coalesce(max(command_ordinal),0)+1 AS ordinal
          FROM ${relations.stagedCommands} WHERE run_id=$1
        `, [normalized.runId]);
        const result = await client.query(`
          INSERT INTO ${relations.stagedCommands} (
            id,run_id,assignment_id,attempt_id,command_ordinal,command_type,
            schema_version,payload,payload_sha256,expected_versions,dependency_ids,
            status,staged_at
          ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10::jsonb,$11,'staged',$12)
          RETURNING *
        `, [
          normalized.commandId, normalized.runId, normalized.assignmentId,
          normalized.attemptId, Number(ordinal.rows[0].ordinal), normalized.commandType,
          normalized.schemaVersion, JSON.stringify(normalized.payload), normalized.payloadHash,
          JSON.stringify(normalized.expectedVersions), normalized.dependencyIds,
          normalized.stagedAt,
        ]);
        await client.query(
          `UPDATE ${relations.runs} SET updated_at=$2 WHERE id=$1`,
          [normalized.runId, normalized.stagedAt],
        );
        return mapStagedRow(result.rows[0]);
      });
    },
    async list(input) {
      const runId = requireText(input?.runId, "Workflow run id");
      const result = await database.query(`
        SELECT * FROM ${relations.stagedCommands}
        WHERE run_id=$1 ORDER BY command_ordinal
      `, [runId]);
      return result.rows.map(mapStagedRow);
    },
  });
}

function normalizeStagedCommand(input, stagedAt) {
  if (!input || typeof input !== "object" || Array.isArray(input)) {
    throw new TypeError(`Staged ${commandLabel} is required.`);
  }
  const commandType = requireText(input.commandType, `Staged ${commandLabel} type`);
  if (!commandTypes.has(commandType)) {
    throw new TypeError(`Unsupported staged ${commandLabel} type: ${commandType}.`);
  }
  const commandId = requireText(input.commandId, `Staged ${commandLabel} id`);
  const payload = cloneJson(input.payload);
  const payloadBytes = Buffer.byteLength(canonicalStagedJson(payload), "utf8");
  if (payloadBytes > 4_194_304) {
    throw new TypeError(`Staged ${commandLabel} payload exceeds 4194304 bytes (${payloadBytes}).`);
  }
  if (payload.commandId !== commandId) {
    throw new TypeError("Staged command id must equal its domain payload commandId.");
  }
  validatePayload(commandType, clone(payload));
  const dependencyIds = uniqueText(input.dependencyIds ?? [], "Staged command dependency id");
  if (dependencyIds.includes(commandId)) throw new TypeError("A staged command cannot depend on itself.");
  return {
    commandId,
    runId: requireText(input.runId, "Workflow run id"),
    assignmentId: requireText(input.assignmentId, "Assignment id"),
    attemptId: requireText(input.attemptId, "Attempt id"),
    ordinal: null,
    commandType,
    schemaVersion: 1,
    payload,
    payloadHash: stagedValueHash(payload),
    expectedVersions: cloneJson(deriveExpectedVersions(commandType, clone(payload))),
    dependencyIds,
    status: "staged",
    manifestHash: null,
    stagedAt: isoTimestamp(stagedAt, "Staging time"),
    validatedAt: null,
    admittedAt: null,
  };
}

}

function assertSameIdentity(stored, candidate) {
  const identity = value => Object.fromEntries(['commandId', 'runId', 'assignmentId', 'attemptId',
    'commandType', 'schemaVersion', 'payloadHash', 'expectedVersions', 'dependencyIds'].map(key => [key, value[key]]));
  if (canonicalStagedJson(identity(stored)) !== canonicalStagedJson(identity(candidate))) {
    throw conflict(`Staged command ${candidate.commandId} already has a different immutable identity.`);
  }
}

function assertAttemptLineage(run, command) {
  if (run.status !== "running") throw conflict(`Workflow run ${run.runId} is not accepting staged commands.`);
  const assignment = run.assignments.find(({ assignmentId }) => assignmentId === command.assignmentId);
  const attempt = assignment?.attempts.find(({ attemptId }) => attemptId === command.attemptId);
  if (!assignment || !attempt) throw new TypeError("Staged command attempt does not match its run and assignment.");
  if (assignment.status !== "running" || attempt.outcome !== null) {
    throw conflict(`Workflow attempt ${command.attemptId} is not active.`);
  }
}

function mapStagedRow(row) {
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

export function stagedValueHash(value) {
  return createHash("sha256").update(canonicalStagedJson(value)).digest("hex");
}

export function canonicalStagedJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalStagedJson).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${canonicalStagedJson(value[key])}`).join(",")}}`;
  }
  const encoded = JSON.stringify(value);
  if (encoded === undefined) throw new TypeError("Staged JSON values cannot contain undefined.");
  return encoded;
}

function cloneJson(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError("Staged command payload must be an object.");
  }
  try {
    const copy = JSON.parse(JSON.stringify(value));
    if (!copy || typeof copy !== 'object' || Array.isArray(copy)) {
      throw new TypeError('Staged JSON must retain an object envelope after serialization.');
    }
    return copy;
  } catch (error) {
    throw new TypeError("Staged command payload must be JSON serializable.", { cause: error });
  }
}

function uniqueText(values, label) {
  if (!Array.isArray(values)) throw new TypeError(`${label}s must be an array.`);
  const normalized = values.map((value) => requireText(value, label));
  if (new Set(normalized).size !== normalized.length) throw new TypeError(`${label}s must be unique.`);
  return normalized;
}

function requireText(value, label) {
  if (typeof value !== "string" || !value.trim()) throw new TypeError(`${label} is required.`);
  return value.trim();
}

function isoTimestamp(value, label) {
  const milliseconds = Date.parse(value);
  if (!Number.isFinite(milliseconds)) throw new TypeError(`${label} must be an ISO timestamp.`);
  return new Date(milliseconds).toISOString();
}

function asIso(value) {
  if (value == null) return null;
  return value instanceof Date ? value.toISOString() : new Date(value).toISOString();
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

function requireClaim(value) {
  const claim = normalizeExpectedClaim(value);
  if (!claim) throw new TypeError("An exact workflow claim is required.");
  return claim;
}
