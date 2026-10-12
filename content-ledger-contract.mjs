import { createHash } from "node:crypto";
import { valueSchemaIssues, validateValueSchema } from "./value-schema.mjs";
import { assertContentLedgerSchemaProfile, assertProfileJsonValue,
  validateContentLedgerProfileSchema } from "./content-ledger-postgres-profile.mjs";

// contentVersionIdentity, observedUsageIdentity, and hashValue are exported only as
// internal persistence helpers. Normalization is not authority or semantic proof.

const captureStatuses = new Set(["complete", "partial", "redacted", "unavailable"]);
const usageDirections = new Set(["supplied-input", "produced-output"]);
const lineageStatuses = new Set(["exact", "unknown", "not-applicable"]);
const contentVersionKeys = [
  "applicationNamespace", "contentId", "contentType", "contentSchemaVersion", "extensions", "source", "capture", "runId",
];
const observedUsageKeys = [
  "assignmentId", "attemptId", "threadId", "turnId", "direction", "usageRole", "observationKey", "lineageStatus", "queryTruncated",
];

export function normalizeContentExtensionDefinition(input, recordedAt, { schemaProfile } = {}) {
  assertContentLedgerSchemaProfile(schemaProfile);
  assertKnownKeys(input, new Set(["applicationNamespace", "definitionId", "version", "schema"]), "Content extension definition");
  if (schemaProfile !== undefined) assertProfileJsonValue(input.schema, "Content extension schema");
  const definition = {
    applicationNamespace: requireName(input?.applicationNamespace, "Application namespace"),
    definitionId: requireName(input?.definitionId, "Extension definition id"),
    version: positiveInteger(input?.version, "Extension definition version"),
    schema: cloneJson(input?.schema),
    recordedAt: isoTimestamp(recordedAt, "Extension definition recorded time"),
  };
  if (schemaProfile === undefined) validateValueSchema(definition.schema, "Content extension schema");
  else validateContentLedgerProfileSchema(definition.schema, schemaProfile);
  definition.schemaHash = hashValue(definition.schema);
  definition.identityHash = hashValue({
    applicationNamespace: definition.applicationNamespace,
    definitionId: definition.definitionId,
    version: definition.version,
    schemaHash: definition.schemaHash,
  });
  return definition;
}

/** Normalize capture metadata without asserting that an agent received or produced it. */
export function normalizeContentVersion(input, { extensionDefinition = null, recordedAt, schemaProfile } = {}) {
  assertContentLedgerSchemaProfile(schemaProfile);
  assertKnownKeys(input, new Set(contentVersionKeys), "Content version");
  const applicationNamespace = requireName(input?.applicationNamespace, "Application namespace");
  if (extensionDefinition && extensionDefinition.applicationNamespace !== applicationNamespace) {
    throw new TypeError("Content extension definition must use the observed content application namespace.");
  }
  const capture = normalizeCapture(input?.capture);
  if (schemaProfile !== undefined) assertProfileJsonValue(input?.extensions ?? {}, "Content extensions");
  const extensions = cloneJson(input?.extensions ?? {});
  if (schemaProfile !== undefined && (extensions === null || Array.isArray(extensions)
    || typeof extensions !== "object")) {
    throw new TypeError("Content extensions must be a JSON object for capture-v1.");
  }
  if (extensionDefinition) {
    if (schemaProfile !== undefined) validateContentLedgerProfileSchema(extensionDefinition.schema, schemaProfile);
    const issues = valueSchemaIssues(extensions, extensionDefinition.schema, "extensions");
    if (issues.length) throw new TypeError(`Content extensions do not match their registered schema: ${issues[0].code} at ${issues[0].path}.`);
  } else if (Object.keys(extensions).length) {
    throw new TypeError("Content extensions require a registered extension definition.");
  }
  const source = {
    kind: enumValue(input?.source?.kind, "Content source kind", new Set(["workflow-event-response", "attempt-final-output", "host-capture"])),
    recordId: requireText(input?.source?.recordId, "Content source record id"),
    field: requireText(input?.source?.field, "Content source field"),
  };
  assertHostCapture(source, capture);
  const base = {
    applicationNamespace,
    contentId: requireText(input?.contentId, "Stable content id"),
    contentType: requireName(input?.contentType, "Content type"),
    contentSchemaVersion: positiveInteger(input?.contentSchemaVersion ?? 1, "Content schema version"),
    extensionDefinitionId: extensionDefinition?.definitionId ?? null,
    extensionDefinitionVersion: extensionDefinition?.version ?? null,
    extensions,
    source,
    capture,
    runId: requireText(input?.runId, "Workflow run id"),
  };
  if (schemaProfile !== undefined) assertProfileJsonValue(base, "Content version");
  const versionIdentityHash = hashValue(contentVersionIdentity(base));
  return {
    ...base,
    versionId: `content-version-${versionIdentityHash}`,
    versionIdentityHash,
    recordedAt: isoTimestamp(recordedAt, "Content recorded time"),
  };
}

/** Bind an explicit usage observation to a normalized version; capture alone is not usage. */
export function normalizeObservedUsage(input, { recordedAt, schemaProfile } = {}) {
  assertContentLedgerSchemaProfile(schemaProfile);
  assertKnownKeys(input, new Set(["contentVersion", ...observedUsageKeys]), "Observed usage");
  assertKnownKeys(input.contentVersion, new Set([
    ...contentVersionKeys, "extensionDefinitionId", "extensionDefinitionVersion", "versionId", "versionIdentityHash", "recordedAt",
  ]), "Observed usage content version");
  if (schemaProfile !== undefined) assertProfileJsonValue(input.contentVersion, "Observed usage content version");
  const contentVersion = cloneJson(input.contentVersion);
  const versionIdentity = contentVersionIdentity(contentVersion);
  const versionIdentityHash = hashValue(versionIdentity);
  if (contentVersion.versionIdentityHash !== versionIdentityHash
    || contentVersion.versionId !== `content-version-${versionIdentityHash}`) {
    throw new TypeError("Observed usage content version identity is invalid.");
  }
  assertHostCapture(contentVersion.source, contentVersion.capture);
  const base = {
    contentVersion,
    runId: requireText(contentVersion.runId, "Workflow run id"),
    assignmentId: requireText(input?.assignmentId, "Workflow assignment id"),
    attemptId: requireText(input?.attemptId, "Workflow attempt id"),
    threadId: optionalText(input?.threadId),
    turnId: optionalText(input?.turnId),
    direction: enumValue(input?.direction, "Observed usage direction", usageDirections),
    usageRole: requireName(input?.usageRole, "Observed usage role"),
    observationKey: requireText(input?.observationKey, "Observed usage key"),
    lineageStatus: enumValue(input?.lineageStatus, "Row lineage status", lineageStatuses),
    queryTruncated: input?.queryTruncated == null ? null : Boolean(input.queryTruncated),
  };
  if (schemaProfile !== undefined) assertProfileJsonValue(base, "Observed usage");
  const usageIdentityHash = hashValue(observedUsageIdentity(base, versionIdentity));
  return {
    ...base,
    usageId: `content-usage-${usageIdentityHash}`,
    usageIdentityHash,
    recordedAt: isoTimestamp(recordedAt, "Observed usage recorded time"),
  };
}

export function normalizeObservedContent(input, { extensionDefinition = null, recordedAt, schemaProfile } = {}) {
  assertContentLedgerSchemaProfile(schemaProfile);
  assertKnownKeys(input, new Set([...contentVersionKeys, ...observedUsageKeys]), "Observed content");
  const contentVersion = normalizeContentVersion(pickKeys(input, contentVersionKeys), {
    extensionDefinition, recordedAt, schemaProfile,
  });
  const usage = normalizeObservedUsage({ contentVersion, ...pickKeys(input, observedUsageKeys) }, { recordedAt, schemaProfile });
  const { versionId, versionIdentityHash, recordedAt: versionRecordedAt, ...base } = contentVersion;
  const { contentVersion: usageVersion, usageId, usageIdentityHash, recordedAt: usageRecordedAt, ...lineage } = usage;
  return { ...base, ...lineage, versionId, versionIdentityHash, usageId, usageIdentityHash, recordedAt: versionRecordedAt };
}

function pickKeys(value, keys) {
  return Object.fromEntries(keys.filter((key) => Object.hasOwn(value, key)).map((key) => [key, value[key]]));
}

function assertHostCapture(source, capture) {
  if (source.kind !== "host-capture") return;
  if (capture.status !== "complete" || capture.hashEncoding !== "raw-bytes-v1") {
    throw new TypeError("Host captures require complete capture metadata with raw-bytes-v1 hash encoding.");
  }
  requireHash(capture.sha256, "Host capture content hash");
  nonnegativeInteger(capture.byteLength, "Host capture byte length");
}

function normalizeCapture(value) {
  const capture = value && typeof value === "object" && !Array.isArray(value) ? value : {};
  const status = enumValue(capture.status, "Capture status", captureStatuses);
  const sha256 = capture.sha256 == null ? null : requireHash(capture.sha256, "Capture content hash");
  const byteLength = capture.byteLength == null ? null : nonnegativeInteger(capture.byteLength, "Capture byte length");
  if (status === "complete" && (sha256 == null || byteLength == null)) throw new TypeError("Complete captures require a content hash and byte length.");
  return { status, source: optionalText(capture.source), sha256, byteLength,
    hashEncoding: sha256 == null ? null : requireName(capture.hashEncoding, "Capture hash encoding") };
}

export function contentVersionIdentity(value) {
  return {
    runId: value.runId,
    applicationNamespace: value.applicationNamespace, contentId: value.contentId,
    contentType: value.contentType, contentSchemaVersion: value.contentSchemaVersion,
    extensionDefinitionId: value.extensionDefinitionId,
    extensionDefinitionVersion: value.extensionDefinitionVersion,
    extensions: value.extensions, source: value.source, capture: value.capture,
  };
}

export function observedUsageIdentity(value, versionIdentity) {
  return { ...versionIdentity, runId: value.runId, assignmentId: value.assignmentId,
    attemptId: value.attemptId, threadId: value.threadId, turnId: value.turnId,
    direction: value.direction, usageRole: value.usageRole, observationKey: value.observationKey,
    lineageStatus: value.lineageStatus, queryTruncated: value.queryTruncated };
}

export function hashValue(value) { return createHash("sha256").update(stableJson(value)).digest("hex"); }

function stableJson(value) {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  if (value && typeof value === "object") return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stableJson(value[key])}`).join(",")}}`;
  return JSON.stringify(value);
}

function cloneJson(value) { return JSON.parse(JSON.stringify(value)); }

function requireText(value, label) { if (typeof value !== "string" || !value.trim()) throw new TypeError(`${label} is required.`); return value.trim(); }

function optionalText(value) { return value == null ? null : requireText(value, "Text"); }

function requireName(value, label) { const text = requireText(value, label); if (!/^[a-z0-9][a-z0-9._:-]{0,199}$/.test(text)) throw new TypeError(`${label} is invalid.`); return text; }

function requireHash(value, label) { const text = requireText(value, label); if (!/^[0-9a-f]{64}$/.test(text)) throw new TypeError(`${label} must be a SHA-256 digest.`); return text; }

function positiveInteger(value, label) { if (!Number.isInteger(value) || value < 1) throw new TypeError(`${label} must be a positive integer.`); return value; }

function nonnegativeInteger(value, label) { if (!Number.isInteger(value) || value < 0) throw new TypeError(`${label} must be a non-negative integer.`); return value; }

function enumValue(value, label, values) { if (!values.has(value)) throw new TypeError(`${label} is invalid.`); return value; }

function isoTimestamp(value, label) { const parsed = new Date(value); if (!value || Number.isNaN(parsed.valueOf())) throw new TypeError(`${label} is invalid.`); return parsed.toISOString(); }

function assertKnownKeys(value, allowed, label) {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new TypeError(`${label} must be an object.`);
  const unknown = Object.keys(value).filter((key) => !allowed.has(key));
  if (unknown.length) throw new TypeError(`${label} has unsupported fields: ${unknown.sort().join(", ")}.`);
}
