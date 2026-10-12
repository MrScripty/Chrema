import {
  assertAssessmentIdentity,
  assertDecisionIdentity,
  assertDeliveryOutcomeIdentity,
  assertRequestIdentity,
  expectedAssessmentDefinitionIdentity,
  expectedPolicyIdentity,
} from "./content-transfer-contract.mjs";
import { resolvePostgresRelations } from "../shared/postgres-relations.mjs";

const relationKeys = ["policyDefinitions", "assessmentDefinitions", "assessments", "requests",
  "decisions", "deliveryOutcomes", "contentVersions"];

/**
 * Bind transfer persistence to an exact set of caller-owned PostgreSQL relations.
 * Every insert performs multiple statements; callers must pass a
 * transaction-scoped client when they require atomic persistence. This
 * factory does not acquire clients or manage transaction boundaries. Inserts
 * and decision transactions reject sessions that suppress foreign-key checks.
 */
export function createPostgresContentTransferPersistence({ relations } = {}) {
  const resolved = resolvePostgresRelations(relations, relationKeys, "Content transfer");
  return Object.freeze({
    insertTransferPolicyDefinition: (client, value) => insertTransferPolicyDefinition(client, value, resolved),
    insertTransferAssessmentDefinition: (client, value) => insertTransferAssessmentDefinition(client, value, resolved),
    insertTransferAssessment: (client, value) => insertTransferAssessment(client, value, resolved),
    insertTransferRequest: (client, value) => insertTransferRequest(client, value, resolved),
    insertTransferDecision: (client, value) => insertTransferDecision(client, value, resolved),
    insertTransferDeliveryOutcome: (client, value) => insertTransferDeliveryOutcome(client, value, resolved),
    createDecisionStore: ({ database } = {}) => createDecisionStore(database, resolved),
  });
}

async function insertTransferPolicyDefinition(client, definition, relations) {
  const expected = expectedPolicyIdentity(definition);
  if (definition.identityHash !== expected.identityHash
    || definition.configSchemaHash !== expected.configSchemaHash
    || definition.configHash !== expected.configHash) {
    throw new TypeError("Transfer policy definition identity is invalid.");
  }
  await assertForeignKeysActive(client);
  await client.query(`
    INSERT INTO ${relations.policyDefinitions} (
      application_namespace,policy_id,policy_version,config_schema,config,
      config_schema_sha256,config_sha256,identity_sha256,recorded_at
    ) VALUES ($1,$2,$3,$4::jsonb,$5::jsonb,$6,$7,$8,$9)
    ON CONFLICT (application_namespace,policy_id,policy_version) DO NOTHING
  `, [definition.applicationNamespace, definition.policyId, definition.policyVersion,
    JSON.stringify(definition.configSchema), JSON.stringify(definition.config), definition.configSchemaHash,
    definition.configHash, definition.identityHash, definition.recordedAt]);
  const stored = await client.query(`
    SELECT identity_sha256 FROM ${relations.policyDefinitions}
    WHERE application_namespace=$1 AND policy_id=$2 AND policy_version=$3
  `, [definition.applicationNamespace, definition.policyId, definition.policyVersion]);
  if (stored.rows[0]?.identity_sha256 !== definition.identityHash) {
    throw conflict(`Transfer policy ${definition.applicationNamespace}:${definition.policyId}:${definition.policyVersion} already has a different definition.`);
  }
  return definition;
}

async function insertTransferAssessmentDefinition(client, definition, relations) {
  const expected = expectedAssessmentDefinitionIdentity(definition);
  if (definition.identityHash !== expected.identityHash || definition.resultSchemaHash !== expected.resultSchemaHash) {
    throw new TypeError("Transfer assessment definition identity is invalid.");
  }
  await assertForeignKeysActive(client);
  await client.query(`
    INSERT INTO ${relations.assessmentDefinitions} (
      application_namespace,assessment_definition_id,assessment_definition_version,result_schema,
      result_schema_sha256,identity_sha256,recorded_at
    ) VALUES ($1,$2,$3,$4::jsonb,$5,$6,$7)
    ON CONFLICT (application_namespace,assessment_definition_id,assessment_definition_version) DO NOTHING
  `, [definition.applicationNamespace, definition.assessmentDefinitionId, definition.assessmentDefinitionVersion,
    JSON.stringify(definition.resultSchema), definition.resultSchemaHash, definition.identityHash, definition.recordedAt]);
  const stored = await client.query(`
    SELECT identity_sha256 FROM ${relations.assessmentDefinitions}
    WHERE application_namespace=$1 AND assessment_definition_id=$2 AND assessment_definition_version=$3
  `, [definition.applicationNamespace, definition.assessmentDefinitionId, definition.assessmentDefinitionVersion]);
  if (stored.rows[0]?.identity_sha256 !== definition.identityHash) {
    throw conflict(`Transfer assessment definition ${definition.applicationNamespace}:${definition.assessmentDefinitionId}:${definition.assessmentDefinitionVersion} already has a different definition.`);
  }
  return definition;
}

async function insertTransferAssessment(client, assessment, relations) {
  assertAssessmentIdentity(assessment);
  await assertForeignKeysActive(client);
  await client.query(`
    INSERT INTO ${relations.assessments} (
      id,application_namespace,assessment_definition_id,assessment_definition_version,content_version_id,
      request_id,destination_kind,destination_id,destination_action,evaluator_identity,
      configuration_id,configuration_version,outcome,result,limitations,execution_reference,
      identity_sha256,recorded_at
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11,$12,$13,$14::jsonb,$15::jsonb,$16,$17,$18)
    ON CONFLICT (id) DO NOTHING
  `, [assessment.assessmentId, assessment.applicationNamespace, assessment.assessmentDefinitionId,
    assessment.assessmentDefinitionVersion, assessment.contentVersionId, assessment.requestId,
    assessment.destination?.kind ?? null, assessment.destination?.id ?? null, assessment.destination?.action ?? null,
    JSON.stringify(assessment.evaluatorIdentity), assessment.configurationReference?.id ?? null,
    assessment.configurationReference?.version ?? null, assessment.outcome, JSON.stringify(assessment.result),
    assessment.limitations == null ? null : JSON.stringify(assessment.limitations), assessment.executionReference,
    assessment.identityHash, assessment.recordedAt]);
  const stored = await client.query(`
    SELECT identity_sha256 FROM ${relations.assessments} WHERE id=$1
  `, [assessment.assessmentId]);
  if (stored.rows[0]?.identity_sha256 !== assessment.identityHash) {
    throw conflict(`Transfer assessment ${assessment.assessmentId} already has a different identity.`);
  }
  return assessment;
}

async function insertTransferRequest(client, request, relations) {
  assertRequestIdentity(request);
  await assertForeignKeysActive(client);
  const retry = await client.query(`
    SELECT id,identity_sha256 FROM ${relations.requests}
    WHERE application_namespace=$1 AND retry_key=$2
  `, [request.applicationNamespace, request.retryKey]);
  if (retry.rows[0] && (retry.rows[0].id !== request.requestId
    || retry.rows[0].identity_sha256 !== request.identityHash)) {
    throw conflict(`Transfer retry key ${request.retryKey} already has a different request identity.`);
  }
  await client.query(`
    INSERT INTO ${relations.requests} (
      id,application_namespace,content_version_id,destination_kind,destination_id,destination_action,
      policy_id,policy_version,requester_identity,retry_key,identity_sha256,recorded_at
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11,$12)
    ON CONFLICT (application_namespace,retry_key) DO NOTHING
  `, [request.requestId, request.applicationNamespace, request.contentVersionId, request.destination.kind,
    request.destination.id, request.destination.action, request.policyId, request.policyVersion,
    JSON.stringify(request.requesterIdentity), request.retryKey, request.identityHash, request.recordedAt]);
  const stored = await client.query(`
    SELECT identity_sha256,retry_key FROM ${relations.requests} WHERE id=$1
  `, [request.requestId]);
  if (stored.rows[0]?.identity_sha256 !== request.identityHash || stored.rows[0]?.retry_key !== request.retryKey) {
    throw conflict(`Transfer request ${request.requestId} already has a different identity.`);
  }
  return request;
}

async function insertTransferDecision(client, decision, relations) {
  assertDecisionIdentity(decision);
  await assertForeignKeysActive(client);
  await client.query(`
    INSERT INTO ${relations.decisions} (
      id,application_namespace,request_id,content_version_id,destination_kind,destination_id,destination_action,
      policy_id,policy_version,assessment_ids,authorization_status,authorization_checked_at,
      authorization_authority_version,authorization_evidence,authorization_evidence_sha256,
      outcome,delivery_enabled,identity_sha256,recorded_at
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::text[],$11,$12,$13,$14::jsonb,$15,$16,$17,$18,$19)
    ON CONFLICT (id) DO NOTHING
  `, [decision.decisionId, decision.applicationNamespace, decision.requestId, decision.contentVersionId,
    decision.destination.kind, decision.destination.id, decision.destination.action, decision.policyId,
    decision.policyVersion, decision.assessmentIds, decision.authorization.status, decision.authorization.checkedAt,
    decision.authorization.authorityVersion, JSON.stringify(decision.authorization.evidence),
    decision.authorization.evidenceHash, decision.outcome, false, decision.identityHash, decision.recordedAt]);
  const stored = await client.query(`
    SELECT identity_sha256,delivery_enabled FROM ${relations.decisions} WHERE id=$1
  `, [decision.decisionId]);
  if (stored.rows[0]?.identity_sha256 !== decision.identityHash || stored.rows[0]?.delivery_enabled !== false) {
    throw conflict(`Transfer decision ${decision.decisionId} already has a different identity.`);
  }
  return decision;
}

async function insertTransferDeliveryOutcome(client, outcome, relations) {
  assertDeliveryOutcomeIdentity(outcome);
  await assertForeignKeysActive(client);
  const attempt = await client.query(`
    SELECT id,identity_sha256 FROM ${relations.deliveryOutcomes}
    WHERE decision_id=$1 AND attempt_key=$2
  `, [outcome.decisionId, outcome.attemptKey]);
  if (attempt.rows[0] && (attempt.rows[0].id !== outcome.deliveryOutcomeId
    || attempt.rows[0].identity_sha256 !== outcome.identityHash)) {
    throw conflict(`Delivery attempt ${outcome.attemptKey} already has a different outcome.`);
  }
  await client.query(`
    INSERT INTO ${relations.deliveryOutcomes} (
      id,decision_id,request_id,content_version_id,destination_kind,destination_id,destination_action,
      attempt_key,outcome,details,failure_code,identity_sha256,recorded_at
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11,$12,$13)
    ON CONFLICT (decision_id,attempt_key) DO NOTHING
  `, [outcome.deliveryOutcomeId, outcome.decisionId, outcome.requestId, outcome.contentVersionId,
    outcome.destination.kind, outcome.destination.id, outcome.destination.action, outcome.attemptKey,
    outcome.outcome, outcome.details == null ? null : JSON.stringify(outcome.details), outcome.failureCode,
    outcome.identityHash, outcome.recordedAt]);
  const stored = await client.query(`
    SELECT identity_sha256 FROM ${relations.deliveryOutcomes} WHERE id=$1
  `, [outcome.deliveryOutcomeId]);
  if (stored.rows[0]?.identity_sha256 !== outcome.identityHash) {
    throw conflict(`Delivery outcome ${outcome.deliveryOutcomeId} already has a different identity.`);
  }
  return outcome;
}

function conflict(message) { return Object.assign(new Error(message), { code: "CONFLICT" }); }

async function assertForeignKeysActive(client) {
  const result = await client.query("SELECT current_setting('session_replication_role') AS session_replication_role");
  if (!["origin", "local"].includes(result.rows[0]?.session_replication_role)) {
    throw failClosed("Content transfer persistence requires active foreign-key checks.");
  }
}

function createDecisionStore(database, relations) {
  if (typeof database?.transaction !== "function") {
    throw new TypeError("Content transfer decisions require a transaction-capable database.");
  }
  return Object.freeze({
    transaction: (work) => database.transaction(async (client) => {
      await assertForeignKeysActive(client);
      return work(client);
    }),
    async loadRequestForUpdate(client, namespace, requestId) {
      const result = await client.query(`
        SELECT r.*,p.identity_sha256 AS policy_identity_sha256
        FROM ${relations.requests} r
        JOIN ${relations.policyDefinitions} p
          ON p.application_namespace=r.application_namespace AND p.policy_id=r.policy_id AND p.policy_version=r.policy_version
        WHERE r.application_namespace=$1 AND r.id=$2 FOR UPDATE OF r
      `, [namespace, requestId]);
      return result.rows[0] ? requestRow(result.rows[0]) : null;
    },
    async loadDecision(client, requestId) {
      const result = await client.query(`
        SELECT * FROM ${relations.decisions} WHERE request_id=$1 ORDER BY recorded_at,id LIMIT 2
      `, [requestId]);
      if (result.rows.length > 1) throw failClosed("Transfer request has ambiguous decision history.");
      return result.rows[0] ? decisionRow(result.rows[0]) : null;
    },
    async loadPolicy(client, request) {
      const result = await client.query(`
        SELECT * FROM ${relations.policyDefinitions}
        WHERE application_namespace=$1 AND policy_id=$2 AND policy_version=$3
      `, [request.applicationNamespace, request.policyId, request.policyVersion]);
      return result.rows[0] ? policyRow(result.rows[0]) : null;
    },
    async loadContentVersion(client, request) {
      const result = await client.query(`
        SELECT * FROM ${relations.contentVersions} WHERE application_namespace=$1 AND id=$2
      `, [request.applicationNamespace, request.contentVersionId]);
      return result.rows[0] ? contentRow(result.rows[0]) : null;
    },
    async loadAssessments(client, request) {
      const result = await client.query(`
        SELECT * FROM ${relations.assessments}
        WHERE application_namespace=$1 AND content_version_id=$2
          AND (request_id IS NULL OR request_id=$3)
        ORDER BY recorded_at,id LIMIT 101
      `, [request.applicationNamespace, request.contentVersionId, request.requestId]);
      return result.rows.map(assessmentRow);
    },
    async loadAssessmentDefinition(client, reference) {
      const result = await client.query(`
        SELECT * FROM ${relations.assessmentDefinitions}
        WHERE application_namespace=$1 AND assessment_definition_id=$2 AND assessment_definition_version=$3
      `, [reference.applicationNamespace, reference.id, reference.version]);
      return result.rows[0] ? definitionRow(result.rows[0]) : null;
    },
    insertAssessment: (client, assessment) => insertTransferAssessment(client, assessment, relations),
    insertDecision: (client, decision) => insertTransferDecision(client, decision, relations),
  });
}


function failClosed(message) { return Object.assign(new Error(message), { code: "TRANSFER_WITHHELD" }); }

function requestRow(row) { return { requestId: row.id, applicationNamespace: row.application_namespace,
  contentVersionId: row.content_version_id, destination: { kind: row.destination_kind, id: row.destination_id, action: row.destination_action },
  policyId: row.policy_id, policyVersion: row.policy_version, policyIdentityHash: row.policy_identity_sha256,
  requesterIdentity: row.requester_identity, retryKey: row.retry_key, identityHash: row.identity_sha256, recordedAt: iso(row.recorded_at) }; }
function policyRow(row) { return { applicationNamespace: row.application_namespace, policyId: row.policy_id,
  policyVersion: row.policy_version, configSchema: row.config_schema, config: row.config,
  configSchemaHash: row.config_schema_sha256, configHash: row.config_sha256, identityHash: row.identity_sha256,
  recordedAt: iso(row.recorded_at) }; }
function contentRow(row) { return { applicationNamespace: row.application_namespace, versionId: row.id,
  contentId: row.content_id, contentType: row.content_type,
  capture: { status: row.capture_status, source: row.capture_source, sha256: row.content_sha256,
    hashEncoding: row.hash_encoding, byteLength: row.byte_length == null ? null : Number(row.byte_length) }, recordedAt: iso(row.recorded_at) }; }
function assessmentRow(row) { return { assessmentId: row.id, applicationNamespace: row.application_namespace,
  assessmentDefinitionId: row.assessment_definition_id, assessmentDefinitionVersion: row.assessment_definition_version,
  contentVersionId: row.content_version_id, requestId: row.request_id,
  destination: row.destination_kind == null ? null : { kind: row.destination_kind, id: row.destination_id, action: row.destination_action },
  evaluatorIdentity: row.evaluator_identity, configurationReference: row.configuration_id == null ? null : { id: row.configuration_id, version: row.configuration_version },
  outcome: row.outcome, result: row.result, limitations: row.limitations, executionReference: row.execution_reference,
  identityHash: row.identity_sha256, recordedAt: iso(row.recorded_at) }; }
function definitionRow(row) { return { applicationNamespace: row.application_namespace,
  assessmentDefinitionId: row.assessment_definition_id, assessmentDefinitionVersion: row.assessment_definition_version,
  resultSchema: row.result_schema, resultSchemaHash: row.result_schema_sha256, identityHash: row.identity_sha256,
  recordedAt: iso(row.recorded_at) }; }
function decisionRow(row) { return { decisionId: row.id, applicationNamespace: row.application_namespace,
  requestId: row.request_id, contentVersionId: row.content_version_id,
  destination: { kind: row.destination_kind, id: row.destination_id, action: row.destination_action },
  policyId: row.policy_id, policyVersion: row.policy_version, assessmentIds: row.assessment_ids,
  authorization: { status: row.authorization_status, checkedAt: iso(row.authorization_checked_at),
    authorityVersion: row.authorization_authority_version, evidence: row.authorization_evidence,
    evidenceHash: row.authorization_evidence_sha256 }, outcome: row.outcome, deliveryEnabled: row.delivery_enabled,
  identityHash: row.identity_sha256, recordedAt: iso(row.recorded_at) }; }
function iso(value) { return value?.toISOString?.() ?? value; }
