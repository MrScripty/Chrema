import { assertNormalizedAssessmentDefinition,
  assertNormalizedContentAssessment } from "./content-assessment-contract.mjs";
import { resolvePostgresRelations } from "./postgres-relations.mjs";

/** Bind immutable assessment writes to two exact caller-owned relations.
 * Each insert and replay read should use the caller's transaction-scoped client.
 */
export function createPostgresContentAssessmentWriter({ relations } = {}) {
  const resolved = resolvePostgresRelations(relations, ["definitions", "assessments"], "Content assessment");
  return Object.freeze({
    insertDefinition: (client, value) => insertDefinition(client, value, resolved),
    insertAssessment: (client, value) => insertAssessment(client, value, resolved),
  });
}

async function insertDefinition(client, value, relations) {
  assertNormalizedAssessmentDefinition(value);
  await assertForeignKeysActive(client);
  await client.query(`
    INSERT INTO ${relations.definitions} (
      application_namespace,definition_id,definition_version,schema_profile,result_schema,
      result_schema_sha256,identity_sha256,recorded_at
    ) VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7,$8)
    ON CONFLICT (application_namespace,definition_id,definition_version) DO NOTHING
  `, [value.applicationNamespace,value.definitionId,value.version,value.schemaProfile,
    JSON.stringify(value.resultSchema),value.resultSchemaHash,value.identityHash,value.recordedAt]);
  const stored = await client.query(`
    SELECT * FROM ${relations.definitions}
    WHERE application_namespace=$1 AND definition_id=$2 AND definition_version=$3
  `, [value.applicationNamespace,value.definitionId,value.version]);
  if (stored.rows.length !== 1) {
    throw conflict(`Assessment definition ${value.applicationNamespace}:${value.definitionId}:${value.version} has different identity.`);
  }
  const durable = definitionRow(stored.rows[0]);
  try { assertNormalizedAssessmentDefinition(durable); }
  catch { throw conflict(`Assessment definition ${value.applicationNamespace}:${value.definitionId}:${value.version} has invalid stored identity.`); }
  if (durable.identityHash !== value.identityHash) {
    throw conflict(`Assessment definition ${value.applicationNamespace}:${value.definitionId}:${value.version} has different identity.`);
  }
  return durable;
}

async function insertAssessment(client, value, relations) {
  assertNormalizedContentAssessment(value);
  await assertForeignKeysActive(client);
  await client.query(`
    INSERT INTO ${relations.assessments} (
      id,application_namespace,assessment_key,key_sha256,content_version_id,
      definition_id,definition_version,evaluator_kind,evaluator_id,
      source_reference_kind,source_reference_id,assessed_at,result,identity_sha256,recorded_at
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14,$15)
    ON CONFLICT (application_namespace,assessment_key) DO NOTHING
  `, [value.assessmentId,value.applicationNamespace,value.assessmentKey,value.keyHash,
    value.contentVersionId,value.definitionId,value.definitionVersion,value.evaluatorIdentity.kind,
    value.evaluatorIdentity.id,value.sourceReference.kind,value.sourceReference.id,
    value.assessedAt,JSON.stringify(value.result),value.identityHash,value.recordedAt]);
  const stored = await client.query(`
    SELECT * FROM ${relations.assessments}
    WHERE application_namespace=$1 AND assessment_key=$2
  `, [value.applicationNamespace,value.assessmentKey]);
  if (stored.rows.length !== 1) {
    throw conflict(`Assessment ${value.applicationNamespace}:${value.assessmentKey} has different identity.`);
  }
  const durable = assessmentRow(stored.rows[0]);
  try { assertNormalizedContentAssessment(durable); }
  catch { throw conflict(`Assessment ${value.applicationNamespace}:${value.assessmentKey} has invalid stored identity.`); }
  if (durable.assessmentId !== value.assessmentId || durable.identityHash !== value.identityHash) {
    throw conflict(`Assessment ${value.applicationNamespace}:${value.assessmentKey} has different identity.`);
  }
  return durable;
}

async function assertForeignKeysActive(client) {
  const result = await client.query("SELECT current_setting('session_replication_role') AS session_replication_role");
  if (!["origin", "local"].includes(result.rows[0]?.session_replication_role)) {
    throw Object.assign(new Error("Content assessment persistence requires active foreign-key checks."),
      { code: "CONTENT_ASSESSMENT_BLOCKED" });
  }
}

function definitionRow(row) {
  return { applicationNamespace: row.application_namespace, definitionId: row.definition_id,
    version: row.definition_version, schemaProfile: row.schema_profile,
    resultSchema: row.result_schema, resultSchemaHash: row.result_schema_sha256,
    identityHash: row.identity_sha256, recordedAt: iso(row.recorded_at) };
}
function assessmentRow(row) {
  return { assessmentId: row.id, applicationNamespace: row.application_namespace,
    assessmentKey: row.assessment_key, keyHash: row.key_sha256,
    contentVersionId: row.content_version_id, definitionId: row.definition_id,
    definitionVersion: row.definition_version,
    evaluatorIdentity: { kind: row.evaluator_kind, id: row.evaluator_id },
    sourceReference: { kind: row.source_reference_kind, id: row.source_reference_id },
    assessedAt: iso(row.assessed_at), result: row.result,
    identityHash: row.identity_sha256, recordedAt: iso(row.recorded_at) };
}
function iso(value) { return value?.toISOString?.() ?? value; }
function conflict(message) { return Object.assign(new Error(message), { code: "CONFLICT" }); }
