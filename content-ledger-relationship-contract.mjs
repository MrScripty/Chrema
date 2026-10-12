import { hashValue } from "./content-ledger-contract.mjs";
import { assertContentLedgerSchemaProfile, assertProfileJsonValue,
  validateContentLedgerProfileSchema } from "./content-ledger-postgres-profile.mjs";
import { valueSchemaIssues } from "./value-schema.mjs";

const namePattern = /^[a-z0-9][a-z0-9._:-]{0,199}$/;
const versionIdPattern = /^content-version-[0-9a-f]{64}$/;
const relationshipIdPattern = /^content-relationship-[0-9a-f]{64}$/;
const hashPattern = /^[0-9a-f]{64}$/;
const inputKeys = new Set(["applicationNamespace", "relationshipType", "relationshipSchemaVersion",
  "fromVersionId", "toVersionId", "sourceReference", "extensions"]);
const normalizedKeys = new Set([...inputKeys, "extensionDefinitionId", "extensionDefinitionVersion",
  "relationshipId", "identityHash", "recordedAt"]);

/** Normalize an attributed, directional relationship between exact content versions. */
export function normalizeContentRelationship(input, { extensionDefinition = null, recordedAt, schemaProfile } = {}) {
  assertContentLedgerSchemaProfile(schemaProfile);
  assertKeys(input, inputKeys, "Content relationship");
  const applicationNamespace = name(input.applicationNamespace, "Application namespace");
  if (extensionDefinition != null) {
    if (extensionDefinition.applicationNamespace !== applicationNamespace) {
      throw new TypeError("Relationship extension definition must use the relationship application namespace.");
    }
    name(extensionDefinition.definitionId, "Extension definition id");
    positiveInteger(extensionDefinition.version, "Extension definition version");
    if (schemaProfile !== undefined) {
      validateContentLedgerProfileSchema(extensionDefinition.schema, schemaProfile);
    }
  }
  const extensions = copyExtensions(input.extensions ?? {}, schemaProfile);
  if (extensionDefinition != null) {
    const issues = valueSchemaIssues(extensions, extensionDefinition.schema, "extensions");
    if (issues.length) {
      throw new TypeError(`Relationship extensions do not match their registered schema: ${issues[0].code} at ${issues[0].path}.`);
    }
  } else if (Object.keys(extensions).length) {
    throw new TypeError("Relationship extensions require a registered extension definition.");
  }
  const core = {
    applicationNamespace,
    relationshipType: name(input.relationshipType, "Relationship type"),
    relationshipSchemaVersion: positiveInteger(input.relationshipSchemaVersion, "Relationship schema version"),
    fromVersionId: versionId(input.fromVersionId, "From content version id"),
    toVersionId: versionId(input.toVersionId, "To content version id"),
    sourceReference: sourceReference(input.sourceReference, schemaProfile),
    extensionDefinitionId: extensionDefinition?.definitionId ?? null,
    extensionDefinitionVersion: extensionDefinition?.version ?? null,
    extensions,
  };
  const identityHash = hashValue(contentRelationshipIdentity(core));
  return { ...core, relationshipId: `content-relationship-${identityHash}`, identityHash,
    recordedAt: isoTimestamp(recordedAt) };
}

/** Identity excludes recording time; direction and attribution are identity-bearing. */
export function contentRelationshipIdentity(value) {
  return {
    applicationNamespace: value.applicationNamespace,
    relationshipType: value.relationshipType,
    relationshipSchemaVersion: value.relationshipSchemaVersion,
    fromVersionId: value.fromVersionId,
    toVersionId: value.toVersionId,
    sourceReference: value.sourceReference,
    extensionDefinitionId: value.extensionDefinitionId,
    extensionDefinitionVersion: value.extensionDefinitionVersion,
    extensions: value.extensions,
  };
}

/** Recheck a normalized payload before persistence; the database validates registered extensions. */
export function assertNormalizedContentRelationship(value, { schemaProfile } = {}) {
  assertContentLedgerSchemaProfile(schemaProfile);
  assertKeys(value, normalizedKeys, "Normalized content relationship");
  name(value.applicationNamespace, "Application namespace");
  name(value.relationshipType, "Relationship type");
  positiveInteger(value.relationshipSchemaVersion, "Relationship schema version");
  versionId(value.fromVersionId, "From content version id");
  versionId(value.toVersionId, "To content version id");
  if (sourceReference(value.sourceReference, schemaProfile).id !== value.sourceReference.id) {
    throw new TypeError("Relationship source id must be normalized text.");
  }
  const hasDefinitionId = value.extensionDefinitionId != null;
  if (hasDefinitionId !== (value.extensionDefinitionVersion != null)) {
    throw new TypeError("Relationship extension definition id and version must appear together.");
  }
  if (hasDefinitionId) {
    name(value.extensionDefinitionId, "Extension definition id");
    positiveInteger(value.extensionDefinitionVersion, "Extension definition version");
  }
  const extensions = copyExtensions(value.extensions, schemaProfile);
  if (!hasDefinitionId && Object.keys(extensions).length) {
    throw new TypeError("Relationship extensions require a registered extension definition.");
  }
  if (JSON.stringify(extensions) !== JSON.stringify(value.extensions)) {
    throw new TypeError("Relationship extensions must be normalized JSON.");
  }
  const expectedHash = hashValue(contentRelationshipIdentity(value));
  if (!hashPattern.test(value.identityHash) || value.identityHash !== expectedHash
    || !relationshipIdPattern.test(value.relationshipId)
    || value.relationshipId !== `content-relationship-${expectedHash}`) {
    throw new TypeError("Content relationship identity is invalid.");
  }
  if (isoTimestamp(value.recordedAt) !== value.recordedAt) {
    throw new TypeError("Content relationship recorded time must be normalized ISO time.");
  }
  return value;
}

function assertKeys(value, allowed, label) {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new TypeError(`${label} must be an object.`);
  const unknown = Object.keys(value).filter((key) => !allowed.has(key));
  if (unknown.length) throw new TypeError(`${label} has unsupported fields: ${unknown.sort().join(", ")}.`);
}

function name(value, label) {
  if (typeof value !== "string" || !namePattern.test(value)) throw new TypeError(`${label} is invalid.`);
  return value;
}

function positiveInteger(value, label) {
  if (!Number.isInteger(value) || value < 1 || value > 2147483647) throw new TypeError(`${label} must be a positive integer.`);
  return value;
}

function versionId(value, label) {
  if (typeof value !== "string" || !versionIdPattern.test(value)) throw new TypeError(`${label} is invalid.`);
  return value;
}

function sourceReference(value, schemaProfile) {
  assertKeys(value, new Set(["kind", "id"]), "Relationship source reference");
  if (schemaProfile !== undefined) assertProfileJsonValue(value.id, "Relationship source id");
  if (typeof value.id !== "string" || !value.id.trim()) {
    throw new TypeError("Relationship source id is required.");
  }
  return { kind: name(value.kind, "Relationship source kind"), id: value.id.trim() };
}

function copyExtensions(value, schemaProfile) {
  if (schemaProfile !== undefined) assertProfileJsonValue(value, "Relationship extensions");
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError("Relationship extensions must be a JSON object.");
  }
  let copy;
  try { copy = JSON.parse(JSON.stringify(value)); } catch {
    throw new TypeError("Relationship extensions must contain JSON values.");
  }
  if (!copy || typeof copy !== "object" || Array.isArray(copy)) {
    throw new TypeError("Relationship extensions must be a JSON object.");
  }
  return copy;
}

function isoTimestamp(value) {
  const parsed = new Date(value);
  if (!value || Number.isNaN(parsed.valueOf())) throw new TypeError("Content relationship recorded time is invalid.");
  return parsed.toISOString();
}
