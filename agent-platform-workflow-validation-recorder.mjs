import { normalizeExpectedClaim, assertMemoryExpectedClaim, assertPostgresExpectedClaim } from "./agent-platform-claim-bound-journal.mjs";
import { resolvePostgresRelations } from "./postgres-relations.mjs";

const postgresRelationKeys = ["runs", "assignments", "stagedCommands", "validations"];

/** Persist one authoritative validation decision against an exact staged manifest. */
export function createWorkflowValidationRecorder({ database, journal, postgresRelations, expectedClaim, now = () => new Date().toISOString() } = {}) {
  const claim = normalizeExpectedClaim(expectedClaim);
  if (!database?.transaction && !journal?.inspectRun) {
    throw new TypeError('WorkflowValidationRecorder requires a database or journal.');
  }
  const relations = database
    ? resolvePostgresRelations(postgresRelations, postgresRelationKeys, "Workflow validation recorder")
    : null;

  async function record({ manifest, report, failure, finalSummary } = {}) {
    assertRecord(manifest, report, failure, finalSummary);
    if (database) {
      const persist = () => database.transaction(async (client) => {
        const locked = await client.query(`SELECT * FROM ${relations.runs} WHERE id=$1 FOR UPDATE`, [report.runId]);
        if (!locked.rows[0]) throw new TypeError(`Unknown workflow run: ${report.runId}.`);
        await assertPostgresExpectedClaim(client, relations, report.runId, claim);
        const currentStatus = locked.rows[0].status;
        const prior = await client.query(`
          SELECT passed,issues,measurements,completed_at
          FROM ${relations.validations}
          WHERE run_id=$1 AND manifest_sha256=$2 AND validator_name=$3 AND validator_version=$4
        `, [report.runId, report.manifestHash, report.validatorName, report.validatorVersion]);
        if (prior.rows[0]) {
          const same = Boolean(prior.rows[0].passed) === report.passed
            && canonicalJson(prior.rows[0].issues) === canonicalJson(report.issues)
            && canonicalJson(prior.rows[0].measurements) === canonicalJson(report.measurements);
          if (!same) throw conflict('A different validation already exists for this manifest.');
          const expectedStatus = report.passed ? 'ready-for-admission' : 'blocked';
          if (currentStatus !== expectedStatus) throw conflict(`Workflow run ${report.runId} has a validation without its terminal validation state.`);
          return;
        }
        if (!['running', 'validating'].includes(currentStatus)) {
          throw conflict(`Workflow run ${report.runId} is not accepting validation results.`);
        }
        const activeAssignments = await client.query(`
          SELECT id FROM ${relations.assignments}
          WHERE run_id=$1 AND status IN ('dispatching','running','unknown') LIMIT 1
        `, [report.runId]);
        if (activeAssignments.rows[0]) throw conflict(`Workflow run ${report.runId} still has active assignments.`);
        const staged = await client.query(`
          SELECT id,command_ordinal,payload_sha256,manifest_sha256,status
          FROM ${relations.stagedCommands}
          WHERE run_id=$1 ORDER BY command_ordinal FOR UPDATE
        `, [report.runId]);
        assertValidationCommandsCurrent(manifest, staged.rows);
        if (currentStatus === 'running') {
          await client.query(`UPDATE ${relations.runs} SET status='validating',updated_at=$2 WHERE id=$1`,
            [report.runId, report.completedAt]);
        }
        await client.query(`
          INSERT INTO ${relations.validations}
            (run_id,manifest_sha256,validator_name,validator_version,passed,issues,measurements,completed_at)
          VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8)
        `, [report.runId, report.manifestHash, report.validatorName, report.validatorVersion, report.passed,
          JSON.stringify(report.issues), JSON.stringify(report.measurements), report.completedAt]);
        if (report.passed) {
          await client.query(`
            UPDATE ${relations.stagedCommands}
            SET status='validated',manifest_sha256=$2,validated_at=$3
            WHERE run_id=$1 AND status='staged'
          `, [report.runId, report.manifestHash, report.completedAt]);
          await client.query(`
            UPDATE ${relations.runs}
            SET status='ready-for-admission',validated_manifest_sha256=$2,
                last_validation_passed_at=$3,updated_at=$3
            WHERE id=$1
          `, [report.runId, report.manifestHash, report.completedAt]);
        } else if (claim) {
          if (typeof journal?.blockRunForRevisionInTransaction !== "function") {
            throw new TypeError("Claim-bound validation requires transactional revision handoff.");
          }
          await journal.blockRunForRevisionInTransaction(client, {
            runId: report.runId, expectedClaim: claim, failure, finalSummary,
            outcome: { gate: "deterministic-validation", verdict: "revision-required",
              manifestHash: report.manifestHash, issues: report.issues },
          });
        } else {
          await client.query(`
            UPDATE ${relations.runs}
            SET status='blocked',failure=$2::jsonb,final_summary=$3::jsonb,
                completed_at=$4,updated_at=$4,claimed_by=NULL,claimed_at=NULL,
                heartbeat_at=NULL,lease_expires_at=NULL
            WHERE id=$1
          `, [report.runId, JSON.stringify(failure), JSON.stringify(finalSummary), report.completedAt]);
        }
      });
      if (claim && !report.passed && typeof journal?.withRevisionHandoff === "function") {
        await journal.withRevisionHandoff(persist);
      } else {
        await persist();
      }
      return;
    }
    const run = await journal.inspectRun({ runId: report.runId });
    if (claim && typeof journal.assertExpectedClaim === "function") {
      await journal.assertExpectedClaim({ runId: report.runId, expectedClaim: claim });
    } else {
      assertMemoryExpectedClaim(run, claim, now);
    }
    const claimInput = claim ? { expectedClaim: claim } : {};
    if (['running', 'validating'].includes(run.status)) {
      if (run.status === 'running') await journal.transitionRun({ ...claimInput, runId: report.runId, status: 'validating' });
      if (report.passed) {
        await journal.transitionRun({ ...claimInput, runId: report.runId, status: 'ready-for-admission', manifestHash: report.manifestHash });
      } else if (claim) {
        if (typeof journal.blockRunForRevision !== "function") {
          throw new TypeError("Claim-bound validation requires revision handoff.");
        }
        await journal.blockRunForRevision({
          runId: report.runId, expectedClaim: claim, failure, finalSummary,
          outcome: { gate: "deterministic-validation", verdict: "revision-required",
            manifestHash: report.manifestHash, issues: report.issues },
        });
      } else {
        await journal.transitionRun({ runId: report.runId, status: 'blocked', failure, finalSummary });
      }
    }
  }
  return { record };
}

function assertRecord(manifest, report, failure, finalSummary) {
  if (!manifest || !Array.isArray(manifest.commands) || typeof manifest.runId !== 'string'
    || !/^[a-f0-9]{64}$/.test(manifest.manifestHash ?? '')) throw new TypeError('A compiled workflow manifest is required.');
  if (!report || report.runId !== manifest.runId || report.manifestHash !== manifest.manifestHash
    || typeof report.validatorName !== 'string' || !report.validatorName.trim()
    || typeof report.validatorVersion !== 'string' || !report.validatorVersion.trim()
    || typeof report.passed !== 'boolean' || !Array.isArray(report.issues)
    || (report.passed && report.issues.length > 0)
    || !report.measurements || typeof report.measurements !== 'object' || Array.isArray(report.measurements)
    || !Number.isFinite(Date.parse(report.completedAt))) {
    throw new TypeError('Validation report must match the manifest and contain a consistent decision.');
  }
  if (!report.passed && (!failure || typeof failure !== 'object' || Array.isArray(failure)
    || !finalSummary || typeof finalSummary !== 'object' || Array.isArray(finalSummary))) {
    throw new TypeError('Failed validation requires application failure and final summary.');
  }
}

function assertValidationCommandsCurrent(manifest, rows) {
  if (manifest.commands.length !== rows.length) throw conflict('The staged workflow manifest changed while validation was being recorded.');
  for (let index = 0; index < manifest.commands.length; index += 1) {
    const expected = manifest.commands[index];
    const row = rows[index];
    if (row.id !== expected.commandId || Number(row.command_ordinal) !== expected.ordinal
      || row.payload_sha256 !== expected.payloadHash
      || (row.manifest_sha256 != null && row.manifest_sha256 !== manifest.manifestHash)
      || !['staged', 'validated'].includes(row.status)) {
      throw conflict('The staged workflow manifest changed while validation was being recorded.');
    }
  }
}

function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  if (value && typeof value === 'object') {
    return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`).join(',')}}`;
  }
  return JSON.stringify(value);
}

function conflict(message) {
  const error = new Error(message);
  error.code = 'WORKFLOW_CONFLICT';
  error.statusCode = 409;
  return error;
}
