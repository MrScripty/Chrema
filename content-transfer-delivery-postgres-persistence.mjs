import { createPostgresContentTransferPersistence } from "./content-transfer-postgres-persistence.mjs";
import { resolvePostgresRelations } from "./postgres-relations.mjs";

const transferKeys = ["policyDefinitions", "assessmentDefinitions", "assessments", "requests",
  "decisions", "deliveryOutcomes", "contentVersions"];

/**
 * Bind the delivery coordinator's store contract to exact caller-owned tables.
 * The host supplies the content bytes; the coordinator verifies their length
 * and digest against immutable version metadata before invoking an adapter.
 */
export function createPostgresContentTransferDeliveryPersistence({
  database, transferRelations, deliveryAttempts, loadContentBody,
} = {}) {
  if (typeof database?.transaction !== "function") {
    throw new TypeError("Content transfer delivery requires a transaction-capable database.");
  }
  if (typeof loadContentBody !== "function") {
    throw new TypeError("Content transfer delivery requires a content body loader.");
  }
  const relations = resolvePostgresRelations(
    { ...transferRelations, deliveryAttempts }, [...transferKeys, "deliveryAttempts"], "Content transfer delivery",
  );
  const transfer = createPostgresContentTransferPersistence({ relations: transferRelations });

  return Object.freeze({
    transaction: (work) => database.transaction(async (client) => {
      await assertForeignKeysActive(client);
      return work(client);
    }),
    async loadRequestForUpdate(client, namespace, id) {
      const value = await client.query(`
        SELECT * FROM ${relations.requests}
        WHERE application_namespace=$1 AND id=$2 FOR UPDATE
      `, [namespace, id]);
      return value.rows[0] ? requestRow(value.rows[0]) : null;
    },
    async loadDecision(client, requestId) {
      const value = await client.query(`SELECT * FROM ${relations.decisions} WHERE request_id=$1`, [requestId]);
      return value.rows[0] ? decisionRow(value.rows[0]) : null;
    },
    async loadDeliverableContent(client, request) {
      const value = await client.query(`
        SELECT id,application_namespace,content_id,content_type,source_kind,source_record_id,source_field,
          capture_status,capture_source,content_sha256,hash_encoding,byte_length,recorded_at
        FROM ${relations.contentVersions}
        WHERE application_namespace=$1 AND id=$2
      `, [request.applicationNamespace, request.contentVersionId]);
      if (!value.rows[0]) return null;
      const metadata = contentMetadata(value.rows[0]);
      const body = await loadContentBody(client, metadata, structuredClone(request));
      return {
        versionId: metadata.versionId, applicationNamespace: metadata.applicationNamespace,
        body, capture: { status: metadata.capture.status, sha256: metadata.capture.sha256,
          hashEncoding: metadata.capture.hashEncoding, byteLength: metadata.capture.byteLength },
      };
    },
    async loadDeliveryHistory(client, requestId, options) {
      if (!options || typeof options !== "object" || Array.isArray(options)
        || Object.keys(options).length !== 2 || typeof options.decisionId !== "string"
        || !options.decisionId || typeof options.attemptKey !== "string" || !options.attemptKey) {
        throw new TypeError("Delivery history requires an exact decision id and attempt key.");
      }
      const result = await client.query(`
        SELECT
          (SELECT row_to_json(exact_row) FROM (
            SELECT * FROM ${relations.deliveryOutcomes}
            WHERE request_id=$1 AND decision_id=$2 AND attempt_key=$3 LIMIT 1
          ) exact_row) AS exact_outcome,
          EXISTS (
            SELECT 1 FROM ${relations.deliveryAttempts} claim
            WHERE claim.request_id=$1 AND claim.decision_id=$2 AND claim.attempt_key=$3
              AND NOT EXISTS (
                SELECT 1 FROM ${relations.deliveryOutcomes} outcome
                WHERE outcome.request_id=$1 AND outcome.decision_id=claim.decision_id
                  AND outcome.attempt_key=claim.attempt_key
              )
          ) AS exact_unresolved_claim,
          EXISTS (
            SELECT 1 FROM ${relations.deliveryAttempts} claim
            WHERE claim.request_id=$1 AND (claim.decision_id<>$2 OR claim.attempt_key<>$3)
              AND NOT EXISTS (
                SELECT 1 FROM ${relations.deliveryOutcomes} outcome
                WHERE outcome.request_id=$1 AND outcome.decision_id=claim.decision_id
                  AND outcome.attempt_key=claim.attempt_key
              )
          ) AS other_unresolved_claim,
          EXISTS (
            SELECT 1 FROM ${relations.deliveryOutcomes} outcome
            WHERE outcome.request_id=$1 AND outcome.outcome IN ('delivered','unknown')
          ) AS prior_delivered_or_unknown
      `, [requestId, options.decisionId, options.attemptKey]);
      if (result.rows.length !== 1) throw new Error("Delivery history query returned no classification.");
      const row = result.rows[0];
      return {
        kind: "classified-v1",
        exactOutcome: row.exact_outcome == null ? null : outcomeRow(row.exact_outcome),
        exactUnresolvedClaim: row.exact_unresolved_claim,
        otherUnresolvedClaim: row.other_unresolved_claim,
        priorDeliveredOrUnknown: row.prior_delivered_or_unknown,
      };
    },
    async insertAttemptClaim(client, claim) {
      await assertForeignKeysActive(client);
      await client.query(`INSERT INTO ${relations.deliveryAttempts} (
        id,application_namespace,decision_id,request_id,content_version_id,destination_kind,destination_id,destination_action,
        adapter_id,adapter_version,attempt_key,identity_sha256,claimed_at
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, [claim.attemptId, claim.applicationNamespace,
        claim.decisionId, claim.requestId, claim.contentVersionId, claim.destination.kind, claim.destination.id,
        claim.destination.action, claim.adapterId, claim.adapterVersion, claim.attemptKey, claim.identityHash, claim.claimedAt]);
      return claim;
    },
    async loadAttemptForUpdate(client, id) {
      const value = await client.query(`SELECT * FROM ${relations.deliveryAttempts} WHERE id=$1 FOR UPDATE`, [id]);
      return value.rows[0] ? claimRow(value.rows[0]) : null;
    },
    insertDeliveryOutcome: (client, receipt) => transfer.insertTransferDeliveryOutcome(client, receipt),
  });
}

async function assertForeignKeysActive(client) {
  const result = await client.query("SELECT current_setting('session_replication_role') AS session_replication_role");
  if (!["origin", "local"].includes(result.rows[0]?.session_replication_role)) {
    throw Object.assign(new Error("Content transfer delivery requires active foreign-key checks."),
      { code: "TRANSFER_BLOCKED" });
  }
}

function contentMetadata(row) {
  return Object.freeze({
    versionId: row.id, applicationNamespace: row.application_namespace,
    contentId: row.content_id, contentType: row.content_type,
    source: Object.freeze({ kind: row.source_kind, recordId: row.source_record_id, field: row.source_field }),
    capture: Object.freeze({ status: row.capture_status, source: row.capture_source,
      sha256: row.content_sha256, hashEncoding: row.hash_encoding,
      byteLength: row.byte_length == null ? null : Number(row.byte_length) }),
    recordedAt: row.recorded_at?.toISOString?.() ?? row.recorded_at,
  });
}

function requestRow(row) { return { requestId: row.id, applicationNamespace: row.application_namespace,
  contentVersionId: row.content_version_id, destination: { kind: row.destination_kind, id: row.destination_id,
    action: row.destination_action }, policyId: row.policy_id, policyVersion: row.policy_version,
  requesterIdentity: row.requester_identity, retryKey: row.retry_key, identityHash: row.identity_sha256 }; }
function decisionRow(row) { return { decisionId: row.id, applicationNamespace: row.application_namespace,
  requestId: row.request_id, contentVersionId: row.content_version_id,
  destination: { kind: row.destination_kind, id: row.destination_id, action: row.destination_action },
  policyId: row.policy_id, policyVersion: row.policy_version, outcome: row.outcome,
  deliveryEnabled: row.delivery_enabled, identityHash: row.identity_sha256 }; }
function claimRow(row) { return { attemptId: row.id, applicationNamespace: row.application_namespace,
  decisionId: row.decision_id, requestId: row.request_id, contentVersionId: row.content_version_id,
  destination: { kind: row.destination_kind, id: row.destination_id, action: row.destination_action },
  adapterId: row.adapter_id, adapterVersion: row.adapter_version, attemptKey: row.attempt_key,
  identityHash: row.identity_sha256, claimedAt: row.claimed_at?.toISOString?.() ?? row.claimed_at }; }
function outcomeRow(row) { return { deliveryOutcomeId: row.id, decisionId: row.decision_id,
  requestId: row.request_id, contentVersionId: row.content_version_id,
  destination: { kind: row.destination_kind, id: row.destination_id, action: row.destination_action },
  attemptKey: row.attempt_key, outcome: row.outcome, details: row.details,
  failureCode: row.failure_code, identityHash: row.identity_sha256,
  recordedAt: row.recorded_at?.toISOString?.() ?? row.recorded_at }; }
