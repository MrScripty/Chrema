import { contentVersionIdentity, observedUsageIdentity, hashValue } from "./content-ledger-contract.mjs";
import { resolvePostgresRelations } from "../shared/postgres-relations.mjs";
import { assertContentLedgerSchemaProfile, assertProfileJsonValue,
  validateContentLedgerProfileSchema } from "./content-ledger-postgres-profile.mjs";

const relationKeys = ["extensionDefinitions", "contentVersions", "observedUsage"];

/**
 * Bind immutable content-ledger writes to explicit PostgreSQL relations.
 * Each operation issues multiple statements, so callers must supply a
 * transaction-scoped client when they require atomic persistence. This writer
 * deliberately does not acquire connections or manage transaction boundaries.
 */
export function createPostgresContentLedgerWriter({ relations, schemaProfile } = {}) {
  assertContentLedgerSchemaProfile(schemaProfile);
  const resolved = resolvePostgresRelations(relations, relationKeys, "Content ledger", {
    missingRelations: "extensionDefinitions, contentVersions, and observedUsage",
  });
  return Object.freeze({
    insertContentExtensionDefinition: (client, definition) => insertContentExtensionDefinition(client, definition, resolved, schemaProfile),
    insertContentVersion: (client, version) => insertContentVersion(client, version, resolved, schemaProfile),
    insertObservedUsage: (client, usage) => insertObservedUsage(client, usage, resolved, schemaProfile),
    insertObservedContent: (client, value) => insertObservedContent(client, value, resolved, schemaProfile),
  });
}

async function insertContentExtensionDefinition(client, definition, relations, schemaProfile) {
  if (schemaProfile !== undefined) validateContentLedgerProfileSchema(definition.schema, schemaProfile);
  const expectedSchemaHash = hashValue(definition.schema);
  const expectedIdentityHash = hashValue({ applicationNamespace: definition.applicationNamespace,
    definitionId: definition.definitionId, version: definition.version, schemaHash: expectedSchemaHash });
  if (definition.schemaHash !== expectedSchemaHash || definition.identityHash !== expectedIdentityHash) {
    throw new TypeError("Content extension definition identity is invalid.");
  }
  const inserted = await client.query(`
    INSERT INTO ${relations.extensionDefinitions} (
      application_namespace,definition_id,definition_version,value_schema,
      schema_sha256,identity_sha256,recorded_at
    ) VALUES ($1,$2,$3,$4::jsonb,$5,$6,$7)
    ON CONFLICT (application_namespace,definition_id,definition_version) DO NOTHING
    RETURNING application_namespace,definition_id,definition_version,identity_sha256
  `, [definition.applicationNamespace,definition.definitionId,definition.version,
    JSON.stringify(definition.schema),definition.schemaHash,definition.identityHash,definition.recordedAt]);
  if (inserted?.rowCount === 1 && Array.isArray(inserted.rows) && inserted.rows.length === 1
    && inserted.rows[0] !== null && typeof inserted.rows[0] === "object") {
    const row = inserted.rows[0];
    if (row.application_namespace !== definition.applicationNamespace
      || row.definition_id !== definition.definitionId
      || row.definition_version !== definition.version
      || row.identity_sha256 !== definition.identityHash) {
      throw conflict(`Content extension ${definition.applicationNamespace}:${definition.definitionId}:${definition.version} already has a different definition.`);
    }
    return definition;
  }
  const stored = await client.query(`
    SELECT identity_sha256 FROM ${relations.extensionDefinitions}
    WHERE application_namespace=$1 AND definition_id=$2 AND definition_version=$3
  `, [definition.applicationNamespace,definition.definitionId,definition.version]);
  if (stored.rows[0]?.identity_sha256 !== definition.identityHash) {
    throw conflict(`Content extension ${definition.applicationNamespace}:${definition.definitionId}:${definition.version} already has a different definition.`);
  }
  return definition;
}

async function insertObservedContent(client, value, relations, schemaProfile) {
  assertContentVersionIdentity(value, schemaProfile, "Observed content");
  assertObservedUsageIdentity(value, value, "Observed content");
  await insertContentVersion(client, value, relations, schemaProfile);
  return persistObservedUsage(client, value, value, relations);
}

function assertContentVersionIdentity(value, schemaProfile, label = "Content version") {
  if (schemaProfile !== undefined) {
    assertProfileJsonValue(value.extensions, "Content extensions");
    if (value.extensions === null || Array.isArray(value.extensions)
      || typeof value.extensions !== "object") {
      throw new TypeError("Content extensions must be a JSON object for capture-v1.");
    }
  }
  const expectedVersionIdentityHash = hashValue(contentVersionIdentity(value));
  if (value.versionIdentityHash !== expectedVersionIdentityHash
    || value.versionId !== `content-version-${expectedVersionIdentityHash}`) {
    throw new TypeError(`${label} identity is invalid.`);
  }
  if (value.source.kind === "host-capture" && (value.capture.status !== "complete"
    || value.capture.hashEncoding !== "raw-bytes-v1"
    || typeof value.capture.sha256 !== "string" || !/^[0-9a-f]{64}$/.test(value.capture.sha256)
    || !Number.isInteger(value.capture.byteLength) || value.capture.byteLength < 0)) {
    throw new TypeError("Host captures require complete capture metadata with a content hash, byte length, and raw-bytes-v1 hash encoding.");
  }
}

function assertObservedUsageIdentity(value, version, label = "Observed usage") {
  const expectedUsageIdentityHash = hashValue(observedUsageIdentity(value, contentVersionIdentity(version)));
  if (value.runId !== version.runId || value.usageIdentityHash !== expectedUsageIdentityHash
    || value.usageId !== `content-usage-${expectedUsageIdentityHash}`) {
    throw new TypeError(`${label} identity is invalid.`);
  }
}

async function insertContentVersion(client, value, relations, schemaProfile) {
  assertContentVersionIdentity(value, schemaProfile);
  const insertedVersion = await client.query(`
    INSERT INTO ${relations.contentVersions} (
      id,run_id,application_namespace,content_id,content_type,content_schema_version,
      extension_definition_id,extension_definition_version,extensions,
      source_kind,source_record_id,source_field,capture_status,capture_source,
      content_sha256,hash_encoding,byte_length,identity_sha256,recorded_at
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
    ON CONFLICT (id) DO NOTHING
    RETURNING id,identity_sha256
  `, [value.versionId,value.runId,value.applicationNamespace,value.contentId,value.contentType,value.contentSchemaVersion,
    value.extensionDefinitionId,value.extensionDefinitionVersion,JSON.stringify(value.extensions),value.source.kind,
    value.source.recordId,value.source.field,value.capture.status,value.capture.source,value.capture.sha256,
    value.capture.hashEncoding,value.capture.byteLength,value.versionIdentityHash,value.recordedAt]);
  if (insertedVersion?.rowCount === 1 && Array.isArray(insertedVersion.rows)
    && insertedVersion.rows.length === 1 && insertedVersion.rows[0] !== null
    && typeof insertedVersion.rows[0] === "object") {
    if (insertedVersion.rows[0].id !== value.versionId
      || insertedVersion.rows[0].identity_sha256 !== value.versionIdentityHash) {
      throw conflict(`Content version ${value.versionId} already has different identity.`);
    }
  } else {
    const storedVersion = await client.query(`SELECT identity_sha256 FROM ${relations.contentVersions} WHERE id=$1`, [value.versionId]);
    if (storedVersion.rows[0]?.identity_sha256 !== value.versionIdentityHash) throw conflict(`Content version ${value.versionId} already has different identity.`);
  }
  return { contentVersionId: value.versionId };
}

async function insertObservedUsage(client, value, relations, schemaProfile) {
  const version = value.contentVersion;
  assertContentVersionIdentity(version, schemaProfile);
  assertObservedUsageIdentity(value, version);
  if (schemaProfile !== undefined) assertProfileJsonValue(value, "Observed usage");
  const storedVersion = await client.query(`SELECT identity_sha256 FROM ${relations.contentVersions} WHERE id=$1`, [version.versionId]);
  if (storedVersion.rows[0]?.identity_sha256 !== version.versionIdentityHash) {
    throw conflict(`Content version ${version.versionId} does not exist or has different identity.`);
  }
  const result = await persistObservedUsage(client, value, version, relations);
  return { observedUsageId: result.observedUsageId };
}

async function persistObservedUsage(client, value, version, relations) {
  const insertedUsage = await client.query(`
    INSERT INTO ${relations.observedUsage} (
      id,content_version_id,run_id,assignment_id,attempt_id,thread_id,turn_id,
      direction,usage_role,observation_key,capture_completeness,row_lineage_status,
      query_truncated,identity_sha256,recorded_at
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
    ON CONFLICT (id) DO NOTHING
    RETURNING id,identity_sha256
  `, [value.usageId,version.versionId,value.runId,value.assignmentId,value.attemptId,value.threadId,value.turnId,
    value.direction,value.usageRole,value.observationKey,version.capture.status,value.lineageStatus,
    value.queryTruncated,value.usageIdentityHash,value.recordedAt]);
  if (insertedUsage?.rowCount === 1 && Array.isArray(insertedUsage.rows)
    && insertedUsage.rows.length === 1 && insertedUsage.rows[0] !== null
    && typeof insertedUsage.rows[0] === "object") {
    if (insertedUsage.rows[0].id !== value.usageId
      || insertedUsage.rows[0].identity_sha256 !== value.usageIdentityHash) {
      throw conflict(`Observed usage ${value.usageId} already has different identity.`);
    }
  } else {
    const storedUsage = await client.query(`SELECT identity_sha256 FROM ${relations.observedUsage} WHERE id=$1`, [value.usageId]);
    if (storedUsage.rows[0]?.identity_sha256 !== value.usageIdentityHash) throw conflict(`Observed usage ${value.usageId} already has different identity.`);
  }
  return { contentVersionId: version.versionId, observedUsageId: value.usageId };
}

function conflict(message) { return Object.assign(new Error(message), { code: "CONFLICT" }); }
