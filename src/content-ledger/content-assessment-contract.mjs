import { hashValue } from "./content-ledger-contract.mjs";
import { CONTENT_LEDGER_POSTGRES_SCHEMA_PROFILE, assertProfileJsonValue,
  validateContentLedgerProfileSchema } from "./content-ledger-postgres-profile.mjs";
import { valueSchemaIssues } from "../shared/value-schema.mjs";

const namePattern = /^[a-z0-9][a-z0-9._:-]{0,199}$/;
const versionPattern = /^content-version-[0-9a-f]{64}$/;
const hashPattern = /^[0-9a-f]{64}$/;
const definitionKeys = new Set(["applicationNamespace", "definitionId", "version", "schemaProfile", "resultSchema"]);
const assessmentKeys = new Set(["applicationNamespace", "assessmentKey", "contentVersionId",
  "evaluatorIdentity", "sourceReference", "assessedAt", "result"]);

/** A versioned application-owned schema for fallible judgments about content. */
export function normalizeAssessmentDefinition(input, { recordedAt } = {}) {
  assertKeys(input, definitionKeys, "Content assessment definition");
  if (input.schemaProfile !== CONTENT_LEDGER_POSTGRES_SCHEMA_PROFILE) {
    throw new TypeError("Content assessment definitions require capture-v1.");
  }
  assertProfileJsonValue(input.resultSchema, "Assessment result schema");
  const resultSchema = copyJson(input.resultSchema);
  validateContentLedgerProfileSchema(resultSchema, input.schemaProfile, "Assessment result schema");
  const base = {
    applicationNamespace: name(input.applicationNamespace, "Application namespace"),
    definitionId: name(input.definitionId, "Assessment definition id"),
    version: positiveInteger(input.version, "Assessment definition version"),
    schemaProfile: input.schemaProfile,
    resultSchema,
  };
  const resultSchemaHash = hashValue(resultSchema);
  return { ...base, resultSchemaHash, identityHash: hashValue({ ...base, resultSchemaHash }),
    recordedAt: isoTimestamp(recordedAt, "Assessment definition recorded time") };
}

/** A caller-keyed assessment is an attributed claim, not a universal verdict.
 * Evaluator and source references describe caller claims; they authenticate neither.
 */
export function normalizeContentAssessment(input, { definition, recordedAt } = {}) {
  assertKeys(input, assessmentKeys, "Content assessment");
  assertNormalizedAssessmentDefinition(definition);
  const applicationNamespace = name(input.applicationNamespace, "Application namespace");
  if (definition.applicationNamespace !== applicationNamespace) {
    throw new TypeError("Assessment definition must use the assessment application namespace.");
  }
  assertProfileJsonValue(input.result, "Assessment result");
  const result = copyJson(input.result);
  const issues = valueSchemaIssues(result, definition.resultSchema, "assessment.result");
  if (issues.length) {
    throw new TypeError(`Content assessment result does not match its definition: ${issues[0].code} at ${issues[0].path}.`);
  }
  const base = {
    applicationNamespace,
    assessmentKey: boundedText(input.assessmentKey, "Assessment key", 500),
    contentVersionId: versionId(input.contentVersionId),
    definitionId: definition.definitionId,
    definitionVersion: definition.version,
    evaluatorIdentity: attribution(input.evaluatorIdentity, "Evaluator identity"),
    sourceReference: attribution(input.sourceReference, "Assessment source reference"),
    assessedAt: isoTimestamp(input.assessedAt, "Assessment time"),
    result,
  };
  const keyHash = hashValue({ applicationNamespace, assessmentKey: base.assessmentKey });
  return { ...base, assessmentId: `content-assessment-${keyHash}`, keyHash,
    identityHash: hashValue(contentAssessmentIdentity(base)),
    recordedAt: isoTimestamp(recordedAt, "Assessment recorded time") };
}

export function contentAssessmentIdentity(value) {
  return { applicationNamespace: value.applicationNamespace, assessmentKey: value.assessmentKey,
    contentVersionId: value.contentVersionId, definitionId: value.definitionId,
    definitionVersion: value.definitionVersion, evaluatorIdentity: value.evaluatorIdentity,
    sourceReference: value.sourceReference, assessedAt: value.assessedAt, result: value.result };
}

/** Recheck all caller-controlled facts before issuing a persistence query. */
export function assertNormalizedAssessmentDefinition(value) {
  assertKeys(value, new Set([...definitionKeys, "resultSchemaHash", "identityHash", "recordedAt"]),
    "Normalized assessment definition");
  if (value.schemaProfile !== CONTENT_LEDGER_POSTGRES_SCHEMA_PROFILE) {
    throw new TypeError("Content assessment definitions require capture-v1.");
  }
  name(value.applicationNamespace, "Application namespace");
  name(value.definitionId, "Assessment definition id");
  positiveInteger(value.version, "Assessment definition version");
  assertProfileJsonValue(value.resultSchema, "Assessment result schema");
  validateContentLedgerProfileSchema(value.resultSchema, value.schemaProfile, "Assessment result schema");
  const resultSchemaHash = hashValue(value.resultSchema);
  const expected = hashValue({ applicationNamespace: value.applicationNamespace,
    definitionId: value.definitionId, version: value.version, schemaProfile: value.schemaProfile,
    resultSchema: value.resultSchema, resultSchemaHash });
  if (value.resultSchemaHash !== resultSchemaHash || value.identityHash !== expected
    || !hashPattern.test(value.identityHash)) throw new TypeError("Content assessment definition identity is invalid.");
  if (isoTimestamp(value.recordedAt, "Assessment definition recorded time") !== value.recordedAt) {
    throw new TypeError("Assessment definition recorded time must be canonical ISO time.");
  }
  return value;
}

export function assertNormalizedContentAssessment(value) {
  assertKeys(value, new Set([...assessmentKeys, "definitionId", "definitionVersion", "assessmentId",
    "keyHash", "identityHash", "recordedAt"]), "Normalized content assessment");
  name(value.applicationNamespace, "Application namespace");
  if (boundedText(value.assessmentKey, "Assessment key", 500) !== value.assessmentKey) {
    throw new TypeError("Assessment key must be normalized text.");
  }
  versionId(value.contentVersionId);
  name(value.definitionId, "Assessment definition id");
  positiveInteger(value.definitionVersion, "Assessment definition version");
  const evaluatorIdentity = attribution(value.evaluatorIdentity, "Evaluator identity");
  const sourceReference = attribution(value.sourceReference, "Assessment source reference");
  if (evaluatorIdentity.id !== value.evaluatorIdentity.id
    || sourceReference.id !== value.sourceReference.id) {
    throw new TypeError("Assessment attribution ids must be normalized text.");
  }
  if (isoTimestamp(value.assessedAt, "Assessment time") !== value.assessedAt
    || isoTimestamp(value.recordedAt, "Assessment recorded time") !== value.recordedAt) {
    throw new TypeError("Assessment times must be canonical ISO time.");
  }
  assertProfileJsonValue(value.result, "Assessment result");
  const keyHash = hashValue({ applicationNamespace: value.applicationNamespace,
    assessmentKey: value.assessmentKey });
  const identityHash = hashValue(contentAssessmentIdentity(value));
  if (value.keyHash !== keyHash || value.assessmentId !== `content-assessment-${keyHash}`
    || value.identityHash !== identityHash || !hashPattern.test(value.identityHash)) {
    throw new TypeError("Content assessment identity is invalid.");
  }
  return value;
}

function attribution(value, label) {
  assertKeys(value, new Set(["kind", "id"]), label);
  return { kind: name(value.kind, `${label} kind`), id: boundedText(value.id, `${label} id`, 500) };
}

function assertKeys(value, allowed, label) {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new TypeError(`${label} must be an object.`);
  const extra = Object.keys(value).filter((key) => !allowed.has(key));
  if (extra.length) throw new TypeError(`${label} has unsupported fields: ${extra.sort().join(", ")}.`);
}

function name(value, label) {
  if (typeof value !== "string" || !namePattern.test(value)) throw new TypeError(`${label} is invalid.`);
  return value;
}
function boundedText(value, label, max) {
  assertProfileJsonValue(value, label);
  if (typeof value !== "string" || !value.trim() || value.length > max) throw new TypeError(`${label} is invalid.`);
  return value.trim();
}
function versionId(value) {
  if (typeof value !== "string" || !versionPattern.test(value)) throw new TypeError("Content version id is invalid.");
  return value;
}
function positiveInteger(value, label) {
  if (!Number.isInteger(value) || value < 1 || value > 2147483647) throw new TypeError(`${label} must be a positive integer.`);
  return value;
}
function isoTimestamp(value, label) {
  const match = typeof value === "string"
    ? /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,3}))?(Z|[+-]\d{2}:\d{2})$/.exec(value)
    : null;
  if (!match) throw new TypeError(`${label} must be an ISO timestamp string.`);
  const [, yearText, monthText, dayText, hourText, minuteText, secondText, , zone] = match;
  const year = Number(yearText);
  const month = Number(monthText);
  const day = Number(dayText);
  const leap = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  const offset = zone === "Z" ? null : zone.slice(1).split(":").map(Number);
  if (month < 1 || month > 12 || day < 1 || day > days[month - 1]
    || Number(hourText) > 23 || Number(minuteText) > 59 || Number(secondText) > 59
    || (offset && (offset[0] > 23 || offset[1] > 59))) {
    throw new TypeError(`${label} is invalid.`);
  }
  const date = new Date(value);
  if (Number.isNaN(date.valueOf())) throw new TypeError(`${label} is invalid.`);
  return date.toISOString();
}
function copyJson(value) { return JSON.parse(JSON.stringify(value)); }
