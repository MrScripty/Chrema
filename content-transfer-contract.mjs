import { createHash } from "node:crypto";

import { valueSchemaIssues, validateValueSchema } from "./value-schema.mjs";

const assessmentOutcomes = new Set(["passed", "failed", "inconclusive"]);
const authorizationStatuses = new Set(["authorized", "unauthorized", "unknown"]);
const decisionOutcomes = new Set(["eligible", "withheld", "denied"]);
const deliveryOutcomes = new Set(["delivered", "failed", "unknown"]);

/**
 * Normalize an application-owned, immutable transfer policy version.
 *
 * The configuration is kept in its own envelope. It is never spread onto a
 * request or decision, so application fields cannot replace platform facts.
 */
export function normalizeTransferPolicyDefinition(input, recordedAtOrOptions = {}) {
  assertKnownKeys(input, new Set([
    "applicationNamespace", "policyId", "version", "policyVersion", "configSchema", "config", "recordedAt",
  ]), "Transfer policy definition");
  const options = normalizeOptions(recordedAtOrOptions);
  const configSchema = cloneJson(input?.configSchema);
  validateValueSchema(configSchema, "Transfer policy config schema");
  const config = cloneJson(input?.config);
  const issues = valueSchemaIssues(config, configSchema, "policy.config");
  if (issues.length) {
    throw new TypeError(`Transfer policy config does not match its schema: ${issues[0].code} at ${issues[0].path}.`);
  }
  const base = {
    applicationNamespace: requireName(input?.applicationNamespace, "Application namespace"),
    policyId: requireName(input?.policyId, "Transfer policy id"),
    policyVersion: positiveInteger(input?.version ?? input?.policyVersion, "Transfer policy version"),
    configSchema,
    config,
  };
  const configSchemaHash = hashValue(configSchema);
  const configHash = hashValue(config);
  const identityHash = hashValue({ ...base, configSchemaHash, configHash });
  return {
    ...base,
    configSchemaHash,
    configHash,
    identityHash,
    recordedAt: isoTimestamp(input?.recordedAt ?? options.recordedAt, "Transfer policy recorded time"),
  };
}

/** Normalize an application-owned schema for assessment result values. */
export function normalizeTransferAssessmentDefinition(input, recordedAtOrOptions = {}) {
  assertKnownKeys(input, new Set([
    "applicationNamespace", "definitionId", "assessmentDefinitionId", "version", "assessmentDefinitionVersion",
    "resultSchema", "recordedAt",
  ]), "Transfer assessment definition");
  const options = normalizeOptions(recordedAtOrOptions);
  const resultSchema = cloneJson(input?.resultSchema);
  validateValueSchema(resultSchema, "Transfer assessment result schema");
  const base = {
    applicationNamespace: requireName(input?.applicationNamespace, "Application namespace"),
    assessmentDefinitionId: requireName(input?.assessmentDefinitionId ?? input?.definitionId, "Assessment definition id"),
    assessmentDefinitionVersion: positiveInteger(
      input?.assessmentDefinitionVersion ?? input?.version,
      "Assessment definition version",
    ),
    resultSchema,
  };
  const resultSchemaHash = hashValue(resultSchema);
  return {
    ...base,
    resultSchemaHash,
    identityHash: hashValue({ ...base, resultSchemaHash }),
    recordedAt: isoTimestamp(input?.recordedAt ?? options.recordedAt, "Assessment definition recorded time"),
  };
}

/**
 * Normalize a fallible assessment. The content version is mandatory; the
 * request and destination references are optional and are retained exactly
 * when supplied.
 */
export function normalizeTransferAssessment(input, recordedAtOrOptions = {}) {
  assertKnownKeys(input, new Set([
    "applicationNamespace", "assessmentDefinitionId", "assessmentDefinitionVersion", "definitionId", "definitionVersion",
    "contentVersionId", "requestId", "destination", "destinationKind", "destinationId", "action",
    "evaluatorIdentity", "evaluator", "configurationReference", "configurationId", "configurationVersion",
    "outcome", "result", "resultSchema", "assessmentDefinition", "limitations", "executionReference", "recordedAt",
  ]), "Transfer assessment");
  const options = normalizeOptions(recordedAtOrOptions);
  const definition = options.assessmentDefinition ?? input?.assessmentDefinition ?? null;
  const resultSchema = definition?.resultSchema ?? input?.resultSchema ?? null;
  if (resultSchema == null) throw new TypeError("Transfer assessment requires a registered result definition.");
  validateValueSchema(resultSchema, "Transfer assessment result schema");
  const result = cloneJson(input?.result);
  const issues = valueSchemaIssues(result, resultSchema, "assessment.result");
  if (issues.length) {
    throw new TypeError(`Transfer assessment result does not match its schema: ${issues[0].code} at ${issues[0].path}.`);
  }
  const destination = normalizeOptionalDestination(input, "Transfer assessment destination");
  const evaluatorIdentity = normalizeIdentity(
    input?.evaluatorIdentity ?? input?.evaluator,
    "Assessment evaluator identity",
  );
  const configurationReference = normalizeConfigurationReference(input);
  const base = {
    applicationNamespace: requireName(input?.applicationNamespace, "Application namespace"),
    assessmentDefinitionId: requireName(
      input?.assessmentDefinitionId ?? input?.definitionId ?? definition?.assessmentDefinitionId,
      "Assessment definition id",
    ),
    assessmentDefinitionVersion: positiveInteger(
      input?.assessmentDefinitionVersion ?? input?.definitionVersion ?? definition?.assessmentDefinitionVersion,
      "Assessment definition version",
    ),
    contentVersionId: requireText(input?.contentVersionId, "Content version id"),
    requestId: optionalText(input?.requestId),
    destination,
    evaluatorIdentity,
    configurationReference,
    outcome: enumValue(input?.outcome, "Assessment outcome", assessmentOutcomes),
    result,
    limitations: input?.limitations == null ? null : cloneJson(input.limitations),
    executionReference: optionalText(input?.executionReference),
  };
  if (definition) assertAssessmentDefinitionMatches(base, definition);
  const identityHash = hashValue(base);
  return {
    ...base,
    assessmentId: `transfer-assessment-${identityHash}`,
    identityHash,
    recordedAt: isoTimestamp(input?.recordedAt ?? options.recordedAt, "Assessment recorded time"),
  };
}

/** Normalize a destination-bound transfer request with an exact retry key. */
export function normalizeTransferRequest(input, recordedAtOrOptions = {}) {
  assertKnownKeys(input, new Set([
    "applicationNamespace", "contentVersionId", "destination", "destinationKind", "destinationId", "action",
    "policyId", "policyVersion", "policyDefinition", "requesterIdentity", "trustedRequesterIdentity", "retryKey", "recordedAt",
  ]), "Transfer request");
  const options = normalizeOptions(recordedAtOrOptions);
  const policy = options.policyDefinition ?? input?.policyDefinition ?? null;
  const destination = normalizeRequiredDestination(input, "Transfer request destination");
  const requesterIdentity = normalizeIdentity(
    input?.trustedRequesterIdentity ?? input?.requesterIdentity,
    "Trusted requester identity",
  );
  const base = {
    applicationNamespace: requireName(input?.applicationNamespace, "Application namespace"),
    contentVersionId: requireText(input?.contentVersionId, "Content version id"),
    destination,
    policyId: requireName(input?.policyId ?? policy?.policyId, "Transfer policy id"),
    policyVersion: positiveInteger(input?.policyVersion ?? policy?.policyVersion, "Transfer policy version"),
    requesterIdentity,
    retryKey: requireBoundedText(input?.retryKey, "Transfer retry key", 500),
  };
  if (policy) assertPolicyMatches(base, policy);
  const identityHash = hashValue(base);
  return {
    ...base,
    requestId: `transfer-request-${identityHash}`,
    identityHash,
    recordedAt: isoTimestamp(input?.recordedAt ?? options.recordedAt, "Transfer request recorded time"),
  };
}

/**
 * Normalize a decision using a live, server-derived authorization observation.
 * Delivery is intentionally disabled in every decision produced here.
 */
export function normalizeTransferDecision(input, recordedAtOrOptions = {}) {
  assertKnownKeys(input, new Set([
    "applicationNamespace", "requestId", "contentVersionId", "destination", "destinationKind", "destinationId", "action",
    "policyId", "policyVersion", "assessmentIds", "assessments", "outcome", "authorization",
    "authorizationStatus", "authorizationCheckedAt", "authorizationAuthorityVersion", "authorizationEvidence",
    "authorizationEvidenceHash", "deliveryEnabled", "recordedAt",
  ]), "Transfer decision");
  const options = normalizeOptions(recordedAtOrOptions);
  const request = options.request ?? input?.request ?? null;
  const destination = normalizeRequiredDestination(input, "Transfer decision destination");
  const authorization = normalizeAuthorization(input, options);
  const assessments = normalizeDecisionAssessments(input, options);
  const assessmentIds = assessments.map((assessment) => assessment.assessmentId ?? assessment.id ?? assessment);
  if (assessmentIds.some((id) => typeof id !== "string" || !id.trim())) {
    throw new TypeError("Transfer decision assessment ids must be non-empty text.");
  }
  const uniqueAssessmentIds = [...new Set(assessmentIds.map((id) => id.trim()))].sort();
  if (uniqueAssessmentIds.length !== assessmentIds.length) throw new TypeError("Transfer decision assessment ids must be unique.");
  const assessmentOutcomes = assessments.filter((value) => value && typeof value === "object" && value.outcome)
    .map((value) => value.outcome);
  if (assessmentOutcomes.some((outcome) => !assessmentOutcomesSet(outcome))) {
    throw new TypeError("Transfer decision assessment outcome is invalid.");
  }
  const blockingAssessment = assessmentOutcomes.some((outcome) => outcome === "failed" || outcome === "inconclusive");
  const base = {
    applicationNamespace: requireName(input?.applicationNamespace ?? request?.applicationNamespace, "Application namespace"),
    requestId: requireText(input?.requestId ?? request?.requestId, "Transfer request id"),
    contentVersionId: requireText(input?.contentVersionId ?? request?.contentVersionId, "Content version id"),
    destination,
    policyId: requireName(input?.policyId ?? request?.policyId, "Transfer policy id"),
    policyVersion: positiveInteger(input?.policyVersion ?? request?.policyVersion, "Transfer policy version"),
    assessmentIds: uniqueAssessmentIds,
    authorization,
    outcome: enumValue(input?.outcome ?? deriveDecisionOutcome(authorization.status, blockingAssessment), "Transfer decision outcome", decisionOutcomes),
    deliveryEnabled: false,
  };
  if (input?.deliveryEnabled === true) throw new TypeError("Transfer decisions cannot enable delivery.");
  if (base.outcome === "eligible" && (authorization.status !== "authorized" || blockingAssessment)) {
    throw new TypeError("Failed, inconclusive, or unauthorized transfer decisions cannot be eligible.");
  }
  if (request) assertDecisionMatchesRequest(base, request);
  for (const assessment of assessments) {
    if (assessment && typeof assessment === "object" && assessment.assessmentId) {
      assertAssessmentMatchesDecision(assessment, base);
    }
  }
  const identityHash = hashValue(base);
  return {
    ...base,
    decisionId: `transfer-decision-${identityHash}`,
    identityHash,
    recordedAt: isoTimestamp(input?.recordedAt ?? options.recordedAt, "Transfer decision recorded time"),
  };
}

/** Normalize an immutable observation of a delivery attempt. */
export function normalizeTransferDeliveryOutcome(input, recordedAtOrOptions = {}) {
  assertKnownKeys(input, new Set([
    "decisionId", "requestId", "contentVersionId", "destination", "destinationKind", "destinationId", "action",
    "attemptKey", "outcome", "details", "failureCode", "decision", "request", "recordedAt",
  ]), "Transfer delivery outcome");
  const options = normalizeOptions(recordedAtOrOptions);
  const decision = options.decision ?? input?.decision ?? null;
  const request = options.request ?? input?.request ?? null;
  const destination = normalizeRequiredDestination(input, "Delivery outcome destination");
  const base = {
    decisionId: requireText(input?.decisionId ?? decision?.decisionId, "Transfer decision id"),
    requestId: requireText(input?.requestId ?? decision?.requestId ?? request?.requestId, "Transfer request id"),
    contentVersionId: requireText(input?.contentVersionId ?? decision?.contentVersionId ?? request?.contentVersionId, "Content version id"),
    destination,
    attemptKey: requireBoundedText(input?.attemptKey, "Delivery attempt key", 500),
    outcome: enumValue(input?.outcome, "Delivery outcome", deliveryOutcomes),
    details: input?.details == null ? null : cloneJson(input.details),
    failureCode: optionalText(input?.failureCode),
  };
  if (decision) assertDeliveryMatchesDecision(base, decision);
  if (request) {
    if (base.requestId !== request.requestId || base.contentVersionId !== request.contentVersionId) {
      throw new TypeError("Delivery outcome is not bound to the exact request.");
    }
  }
  const identityHash = hashValue(base);
  return {
    ...base,
    deliveryOutcomeId: `transfer-delivery-${identityHash}`,
    identityHash,
    recordedAt: isoTimestamp(input?.recordedAt ?? options.recordedAt, "Delivery outcome recorded time"),
  };
}

// Short aliases keep the generic contract convenient for callers that already
// use the content-ledger normalizer naming style.
export const normalizePolicyDefinition = normalizeTransferPolicyDefinition;
export const normalizeAssessmentDefinition = normalizeTransferAssessmentDefinition;
export const normalizeAssessment = normalizeTransferAssessment;
export const normalizeTransfer = normalizeTransferRequest;
export const normalizeDecision = normalizeTransferDecision;
export const normalizeDeliveryOutcome = normalizeTransferDeliveryOutcome;

function normalizeOptions(value) {
  if (typeof value === "string") return { recordedAt: value };
  if (value == null || typeof value !== "object" || Array.isArray(value)) return {};
  return value;
}

// Persistence checks are exported for the SQL module, but are not re-exported
// by the historical content-transfer public module.
export function expectedPolicyIdentity(value) {
  const configSchemaHash = hashValue(value.configSchema);
  const configHash = hashValue(value.config);
  return { configSchemaHash, configHash,
    identityHash: hashValue({ applicationNamespace: value.applicationNamespace, policyId: value.policyId,
      policyVersion: value.policyVersion, configSchema: value.configSchema, config: value.config,
      configSchemaHash, configHash }) };
}

export function expectedAssessmentDefinitionIdentity(value) {
  const resultSchemaHash = hashValue(value.resultSchema);
  return { resultSchemaHash,
    identityHash: hashValue({ applicationNamespace: value.applicationNamespace,
      assessmentDefinitionId: value.assessmentDefinitionId, assessmentDefinitionVersion: value.assessmentDefinitionVersion,
      resultSchema: value.resultSchema, resultSchemaHash }) };
}

function assertPolicyMatches(value, policy) {
  if (value.applicationNamespace !== policy.applicationNamespace || value.policyId !== policy.policyId
    || value.policyVersion !== policy.policyVersion) throw new TypeError("Transfer request policy version is not exact.");
}

function assertAssessmentDefinitionMatches(value, definition) {
  if (value.applicationNamespace !== definition.applicationNamespace
    || value.assessmentDefinitionId !== definition.assessmentDefinitionId
    || value.assessmentDefinitionVersion !== definition.assessmentDefinitionVersion) {
    throw new TypeError("Transfer assessment result definition is not exact.");
  }
}

export function assertRequestIdentity(value) {
  if (value.requestId !== `transfer-request-${hashValue(requestIdentity(value))}`
    || value.identityHash !== hashValue(requestIdentity(value))) throw new TypeError("Transfer request identity is invalid.");
}

export function assertAssessmentIdentity(value) {
  const expected = hashValue(assessmentIdentity(value));
  if (value.assessmentId !== `transfer-assessment-${expected}` || value.identityHash !== expected) {
    throw new TypeError("Transfer assessment identity is invalid.");
  }
}

export function assertDecisionIdentity(value) {
  const expected = hashValue(decisionIdentity(value));
  if (value.decisionId !== `transfer-decision-${expected}` || value.identityHash !== expected
    || value.deliveryEnabled !== false) throw new TypeError("Transfer decision identity is invalid.");
}

export function assertDeliveryOutcomeIdentity(value) {
  const expected = hashValue(deliveryOutcomeIdentity(value));
  if (value.deliveryOutcomeId !== `transfer-delivery-${expected}` || value.identityHash !== expected) {
    throw new TypeError("Transfer delivery outcome identity is invalid.");
  }
}

function requestIdentity(value) {
  return {
    applicationNamespace: value.applicationNamespace,
    contentVersionId: value.contentVersionId,
    destination: value.destination,
    policyId: value.policyId,
    policyVersion: value.policyVersion,
    requesterIdentity: value.requesterIdentity,
    retryKey: value.retryKey,
  };
}

function assessmentIdentity(value) {
  return {
    applicationNamespace: value.applicationNamespace,
    assessmentDefinitionId: value.assessmentDefinitionId,
    assessmentDefinitionVersion: value.assessmentDefinitionVersion,
    contentVersionId: value.contentVersionId,
    requestId: value.requestId,
    destination: value.destination,
    evaluatorIdentity: value.evaluatorIdentity,
    configurationReference: value.configurationReference,
    outcome: value.outcome,
    result: value.result,
    limitations: value.limitations,
    executionReference: value.executionReference,
  };
}

function decisionIdentity(value) {
  return {
    applicationNamespace: value.applicationNamespace,
    requestId: value.requestId,
    contentVersionId: value.contentVersionId,
    destination: value.destination,
    policyId: value.policyId,
    policyVersion: value.policyVersion,
    assessmentIds: value.assessmentIds,
    authorization: value.authorization,
    outcome: value.outcome,
    deliveryEnabled: false,
  };
}

function deliveryOutcomeIdentity(value) {
  return {
    decisionId: value.decisionId,
    requestId: value.requestId,
    contentVersionId: value.contentVersionId,
    destination: value.destination,
    attemptKey: value.attemptKey,
    outcome: value.outcome,
    details: value.details,
    failureCode: value.failureCode,
  };
}

function normalizeDecisionAssessments(input, options) {
  const values = input?.assessments ?? options.assessments ?? input?.assessmentIds ?? [];
  if (!Array.isArray(values)) throw new TypeError("Transfer decision assessments must be an array.");
  return values;
}

function normalizeAuthorization(input, options) {
  const supplied = options.authorization ?? input?.authorization ?? null;
  const raw = supplied ?? {
    status: input?.authorizationStatus,
    checkedAt: input?.authorizationCheckedAt,
    authorityVersion: input?.authorizationAuthorityVersion,
    evidence: input?.authorizationEvidence,
    evidenceHash: input?.authorizationEvidenceHash,
  };
  assertKnownKeys(raw, new Set(["status", "checkedAt", "authorityVersion", "evidence", "evidenceHash"]), "Authorization evidence");
  const evidence = cloneJson(raw.evidence);
  if (!evidence || typeof evidence !== "object" || Array.isArray(evidence)) {
    throw new TypeError("Authorization evidence must be an object.");
  }
  const evidenceHash = hashValue(evidence);
  if (raw.evidenceHash != null && raw.evidenceHash !== evidenceHash) throw new TypeError("Authorization evidence hash is invalid.");
  return {
    status: enumValue(raw.status, "Authorization status", authorizationStatuses),
    checkedAt: isoTimestamp(raw.checkedAt, "Authorization checked time"),
    authorityVersion: optionalText(raw.authorityVersion),
    evidence,
    evidenceHash,
  };
}

function deriveDecisionOutcome(status, blockingAssessment) {
  if (status === "unauthorized") return "denied";
  if (status === "unknown" || blockingAssessment) return "withheld";
  return "eligible";
}

function assessmentOutcomesSet(value) { return assessmentOutcomes.has(value); }

function assertDecisionMatchesRequest(value, request) {
  if (value.applicationNamespace !== request.applicationNamespace || value.requestId !== request.requestId
    || value.contentVersionId !== request.contentVersionId || value.policyId !== request.policyId
    || value.policyVersion !== request.policyVersion || stableJson(value.destination) !== stableJson(request.destination)) {
    throw new TypeError("Transfer decision is not bound to the exact request, policy, content, or destination.");
  }
}

function assertAssessmentMatchesDecision(assessment, decision) {
  if (assessment.applicationNamespace !== decision.applicationNamespace
    || assessment.contentVersionId !== decision.contentVersionId
    || (assessment.requestId != null && assessment.requestId !== decision.requestId)
    || (assessment.destination != null && stableJson(assessment.destination) !== stableJson(decision.destination))) {
    throw new TypeError("Transfer decision assessment is not bound to the exact request, content, or destination.");
  }
}

function assertDeliveryMatchesDecision(value, decision) {
  if (value.decisionId !== decision.decisionId || value.requestId !== decision.requestId
    || value.contentVersionId !== decision.contentVersionId
    || stableJson(value.destination) !== stableJson(decision.destination)) {
    throw new TypeError("Delivery outcome is not bound to the exact decision.");
  }
}

function normalizeRequiredDestination(input, label) {
  const destination = normalizeDestination(input?.destination ?? {
    kind: input?.destinationKind,
    id: input?.destinationId,
    action: input?.action,
  }, label);
  if (!destination) throw new TypeError(`${label} is required.`);
  return destination;
}

function normalizeOptionalDestination(input, label) {
  const hasAny = input?.destination != null || input?.destinationKind != null || input?.destinationId != null || input?.action != null;
  if (!hasAny) return null;
  return normalizeRequiredDestination(input, label);
}

function normalizeDestination(value, label) {
  if (value == null) return null;
  assertKnownKeys(value, new Set(["kind", "id", "action"]), label);
  return {
    kind: requireName(value.kind, `${label} kind`),
    id: requireBoundedText(value.id, `${label} id`, 500),
    action: requireName(value.action, `${label} action`),
  };
}

function normalizeIdentity(value, label) {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new TypeError(`${label} is required.`);
  assertKnownKeys(value, new Set(["kind", "id"]), label);
  return { kind: requireName(value.kind, `${label} kind`), id: requireBoundedText(value.id, `${label} id`, 500) };
}

function normalizeConfigurationReference(input) {
  if (input?.configurationReference != null) {
    assertKnownKeys(input.configurationReference, new Set(["id", "version"]), "Assessment configuration reference");
    return { id: requireName(input.configurationReference.id, "Assessment configuration id"),
      version: positiveInteger(input.configurationReference.version, "Assessment configuration version") };
  }
  const hasId = input?.configurationId != null || input?.configurationVersion != null;
  if (!hasId) return null;
  return { id: requireName(input?.configurationId, "Assessment configuration id"),
    version: positiveInteger(input?.configurationVersion, "Assessment configuration version") };
}

function assertAssessmentIdentityInput(value) {
  if (value == null) return;
  if (typeof value !== "object") throw new TypeError("Transfer assessment must be an object.");
}

function assertKnownKeys(value, allowed, label) {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new TypeError(`${label} must be an object.`);
  const unknown = Object.keys(value).filter((key) => !allowed.has(key));
  if (unknown.length) throw new TypeError(`${label} has unsupported fields: ${unknown.sort().join(", ")}.`);
}

function requireText(value, label) {
  if (typeof value !== "string" || !value.trim()) throw new TypeError(`${label} is required.`);
  return value.trim();
}

function requireBoundedText(value, label, maximum) {
  const text = requireText(value, label);
  if (text.length > maximum) throw new TypeError(`${label} is too long.`);
  return text;
}

function optionalText(value) { return value == null ? null : requireText(value, "Text"); }

function requireName(value, label) {
  const text = requireText(value, label);
  if (!/^[a-z0-9][a-z0-9._:-]{0,199}$/.test(text)) throw new TypeError(`${label} is invalid.`);
  return text;
}

function positiveInteger(value, label) {
  if (!Number.isInteger(value) || value < 1) throw new TypeError(`${label} must be a positive integer.`);
  return value;
}

function enumValue(value, label, values) {
  if (!values.has(value)) throw new TypeError(`${label} is invalid.`);
  return value;
}

function isoTimestamp(value, label) {
  const parsed = new Date(value);
  if (!value || Number.isNaN(parsed.valueOf())) throw new TypeError(`${label} is invalid.`);
  return parsed.toISOString();
}

function cloneJson(value) {
  if (value === undefined) throw new TypeError("JSON value is required.");
  try { return JSON.parse(JSON.stringify(value)); } catch { throw new TypeError("Value must be JSON serializable."); }
}

function hashValue(value) { return createHash("sha256").update(stableJson(value)).digest("hex"); }

function stableJson(value) {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stableJson(value[key])}`).join(",")}}`;
  }
  const encoded = JSON.stringify(value);
  if (encoded === undefined) throw new TypeError("Value must be JSON serializable.");
  return encoded;
}
