import { normalizeExpectedClaim, assertPostgresExpectedClaim } from "./agent-platform-claim-bound-journal.mjs";
import { orderManifestCommands } from './agent-platform-workflow-manifest.mjs';
import { canonicalStagedJson, stagedValueHash } from './agent-platform-workflow-staging.mjs';
import { validateWorkflowAdmissionReceipt } from './workflow-admission-contract.mjs';
import { resolvePostgresRelations } from "../shared/postgres-relations.mjs";

const postgresRelationKeys = ["runs", "stagedCommands", "admissions"];

/** Commit one exact validated manifest through the existing PostgreSQL journal. */
export function createWorkflowAdmissionCommitter({
  database, transaction = database?.transaction?.bind(database),
  authorizeCommit, readVersions, assertPreconditions, applyCommand,
  summarize, createId, now, maxAttempts = 3, postgresRelations, expectedClaim,
} = {}) {
  if (typeof database?.transaction !== 'function') throw new TypeError('Admission committer requires a database transaction.');
  if (typeof transaction !== 'function') throw new TypeError('Admission committer requires a transaction adapter.');
  for (const [name, operation] of Object.entries({ authorizeCommit, readVersions, assertPreconditions, applyCommand, summarize, createId, now })) {
    if (typeof operation !== 'function') throw new TypeError(`Admission committer requires ${name}.`);
  }
  if (!Number.isInteger(maxAttempts) || maxAttempts < 1 || maxAttempts > 10) {
    throw new TypeError('Serialization retry maxAttempts must be an integer from 1 through 10.');
  }
  const claim = normalizeExpectedClaim(expectedClaim);
  const relations = resolvePostgresRelations(postgresRelations, postgresRelationKeys, "Workflow admission committer");

  async function admit({ runId, manifestHash, manifest, actor, context } = {}) {
    if (typeof runId !== 'string' || !runId.trim() || !/^[a-f0-9]{64}$/.test(manifestHash ?? '')) {
      throw new TypeError('Admission requires a run id and exact manifest hash.');
    }
    return retrySerializationConflicts(() => transaction(async (client) => {
      await client.query('SET TRANSACTION ISOLATION LEVEL SERIALIZABLE');
      const selected = await client.query(`SELECT * FROM ${relations.runs} WHERE id=$1 FOR UPDATE`, [runId]);
      const row = selected.rows[0];
      if (!row) throw new TypeError(`Unknown workflow run: ${runId}.`);
      await assertPostgresExpectedClaim(client, relations, runId, claim);
      const prior = await client.query(`SELECT * FROM ${relations.admissions} WHERE run_id=$1`, [runId]);
      if (prior.rows[0]) {
        if (prior.rows[0].manifest_sha256 !== manifestHash) {
          throw conflict(`Workflow run ${runId} was admitted with a different manifest.`);
        }
        return clone(prior.rows[0].receipt);
      }
      if (row.status !== 'ready-for-admission' || row.validated_manifest_sha256 !== manifestHash) {
        throw conflict(`Workflow run ${runId} is not ready for admission.`);
      }
      if (!manifest || manifest.runId !== runId || manifest.manifestHash !== manifestHash
        || !Array.isArray(manifest.commands) || !Array.isArray(manifest.targets)) {
        throw new TypeError('New admission requires the exact compiled manifest.');
      }
      const { manifestHash: suppliedHash, ...manifestFacts } = manifest;
      if (suppliedHash !== stagedValueHash(manifestFacts)) {
        throw conflict('The compiled workflow manifest changed after validation.');
      }
      if (typeof actor?.id !== 'string' || !actor.id.trim()) throw new TypeError('Admission actor id is required.');
      if (await authorizeCommit({ runId, actor: clone(actor), client, context }) !== true) {
        const error = new Error('Admission authorization was revoked before commit.');
        error.statusCode = 403;
        throw error;
      }
      const commandRows = await client.query(`
        SELECT * FROM ${relations.stagedCommands}
        WHERE run_id=$1 ORDER BY command_ordinal FOR UPDATE
      `, [runId]);
      const staged = commandRows.rows.map(mapStagedRow);
      assertStoredCommandsMatch(manifest, staged);
      await assertPreconditions({ manifest, staged, client, context, phase: 'before-targets' });
      const current = await readVersions(manifest.targets, client, { lock: true });
      if (!current || typeof current !== 'object' || Array.isArray(current)) {
        throw new TypeError('Admission target reader must return a version map.');
      }
      for (const target of manifest.targets) {
        if ((current[target.key] ?? null) !== (target.expectedVersion ?? null)) {
          throw Object.assign(new Error(`Target ${target.key} changed before admission could commit.`), {
            code: 'VERSION_CONFLICT', key: target.key,
            expectedVersion: target.expectedVersion, actualVersion: current[target.key] ?? null,
          });
        }
      }
      await assertPreconditions({ manifest, staged, client, context, phase: 'after-targets' });
      const byId = new Map(staged.map(command => [command.commandId, command]));
      const commandResults = [];
      for (const command of orderManifestCommands(manifest.commands)) {
        const stagedCommand = byId.get(command.commandId);
        const result = await applyCommand({ command: clone(command), stagedCommand: clone(stagedCommand), client, context });
        commandResults.push({
          commandId: command.commandId, commandType: command.commandType,
          result: clone(result), resultHash: stagedValueHash(result),
        });
      }
      const admittedAt = iso(now());
      const receipt = validateWorkflowAdmissionReceipt({
        admissionId: requireText(createId(), 'Admission id'),
        runId, workflowVersionId: manifest.workflowVersionId,
        manifestHash, admittedBy: actor.id, admittedAt,
        commandResults, summary: summarize(clone(commandResults)),
      }, manifest);
      await client.query(`
        INSERT INTO ${relations.admissions}
          (id,run_id,manifest_sha256,admitted_by,receipt,receipt_sha256,admitted_at)
        VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7)
      `, [receipt.admissionId, runId, manifestHash, actor.id,
        JSON.stringify(receipt), stagedValueHash(receipt), admittedAt]);
      await client.query(`
        UPDATE ${relations.stagedCommands}
        SET status='admitted',admitted_at=$3
        WHERE run_id=$1 AND manifest_sha256=$2
      `, [runId, manifestHash, admittedAt]);
      await client.query(`
        UPDATE ${relations.runs}
        SET status='admitted',admitted_manifest_sha256=$2,completed_at=$3,updated_at=$3,
            final_summary=$4::jsonb,claimed_by=NULL,claimed_at=NULL,heartbeat_at=NULL,lease_expires_at=NULL
        WHERE id=$1
      `, [runId, manifestHash, admittedAt, JSON.stringify(receipt.summary)]);
      return receipt;
    }), { maxAttempts });
  }
  return { admit };
}

export async function retrySerializationConflicts(operation, { maxAttempts = 3 } = {}) {
  if (typeof operation !== 'function') throw new TypeError('Serialization retry operation is required.');
  if (!Number.isInteger(maxAttempts) || maxAttempts < 1 || maxAttempts > 10) {
    throw new TypeError('Serialization retry maxAttempts must be an integer from 1 through 10.');
  }
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try { return await operation({ attempt, maxAttempts }); }
    catch (error) { if (error?.code !== '40001' || attempt === maxAttempts) throw error; }
  }
  throw new Error('Serialization retry exhausted without a result.');
}

function assertStoredCommandsMatch(manifest, staged) {
  if (manifest.commands.length !== staged.length) throw conflict('The staged workflow manifest changed after validation.');
  for (let index = 0; index < manifest.commands.length; index += 1) {
    const expected = manifest.commands[index];
    const actual = staged[index];
    if (expected.commandId !== actual.commandId || expected.payloadHash !== actual.payloadHash
      || expected.ordinal !== actual.ordinal || expected.commandType !== actual.commandType
      || expected.schemaVersion !== actual.schemaVersion || expected.assignmentId !== actual.assignmentId
      || expected.attemptId !== actual.attemptId
      || canonicalStagedJson(expected.expectedVersions) !== canonicalStagedJson(actual.expectedVersions)
      || canonicalStagedJson(expected.dependencyIds) !== canonicalStagedJson(actual.dependencyIds)
      || stagedValueHash(actual.payload) !== actual.payloadHash
      || actual.manifestHash !== manifest.manifestHash || !['validated', 'admitted'].includes(actual.status)) {
      throw conflict('The staged workflow manifest changed after validation.');
    }
  }
}

function mapStagedRow(row) {
  return {
    commandId: row.id, runId: row.run_id, assignmentId: row.assignment_id,
    attemptId: row.attempt_id, ordinal: Number(row.command_ordinal),
    commandType: row.command_type, schemaVersion: Number(row.schema_version),
    payload: clone(row.payload), payloadHash: row.payload_sha256,
    expectedVersions: clone(row.expected_versions), dependencyIds: [...row.dependency_ids],
    status: row.status, manifestHash: row.manifest_sha256,
  };
}

function iso(value) {
  const milliseconds = Date.parse(value);
  if (!Number.isFinite(milliseconds)) throw new TypeError('Admission time must be an ISO timestamp.');
  return new Date(milliseconds).toISOString();
}

function requireText(value, label) {
  if (typeof value !== 'string' || !value.trim()) throw new TypeError(`${label} is required.`);
  return value.trim();
}

function conflict(message) {
  const error = new Error(message);
  error.code = 'WORKFLOW_CONFLICT';
  error.statusCode = 409;
  return error;
}

function clone(value) { return structuredClone(value); }
