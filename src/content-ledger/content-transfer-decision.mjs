import {
  normalizeTransferAssessment,
  normalizeTransferDecision,
} from "./content-transfer-contract.mjs";

const assessmentOutcomes = new Set(["passed", "failed", "inconclusive"]);
const authorizationStatuses = new Set(["authorized", "unauthorized", "unknown"]);

export function createContentTransferDecisionService({
  store, accessControl, registrations, authorizeDestination, clock = () => new Date(),
} = {}) {
  requireMethod(store, "transaction", "Content transfer store");
  for (const method of ["loadRequestForUpdate", "loadDecision", "loadPolicy", "loadContentVersion", "loadAssessments", "loadAssessmentDefinition", "insertAssessment", "insertDecision"]) {
    requireMethod(store, method, "Content transfer store");
  }
  requireMethod(accessControl, "assertContext", "AccessControl");
  if (typeof authorizeDestination !== "function") throw new TypeError("Destination authorization callback is required.");
  const registry = normalizeRegistrations(registrations);

  return Object.freeze({
    async decide(input) {
      assertKnownKeys(input, new Set(["context", "applicationNamespace", "requestId", "signal"]));
      input?.signal?.throwIfAborted();
      const namespace = requireName(input?.applicationNamespace, "Application namespace");
      const requestId = requireId(input?.requestId, "Transfer request id", "transfer-request-");
      const state = accessControl.assertContext(input?.context);
      const requester = requesterIdentity(state);

      return store.transaction(async (client) => {
        const request = await store.loadRequestForUpdate(client, namespace, requestId);
        const lockedRequester = requesterIdentity(accessControl.assertContext(input?.context));
        if (!request) throw notFound("Transfer request was not found.");
        assertCurrentRequester(request, requester, lockedRequester);

        const prior = await store.loadDecision(client, requestId);
        if (prior) {
          assertCurrentRequester(request, requester, requesterIdentity(accessControl.assertContext(input?.context)));
          return Object.freeze({ decision: prior, replayed: true, authorizationFresh: false });
        }

        const registration = registry.get(registrationKey(request));
        if (!registration || registration.policyIdentityHash !== request.policyIdentityHash) {
          throw failClosed("The exact transfer policy evaluator is unavailable.");
        }
        const [policy, content, availableAssessments, evaluationDefinition] = await Promise.all([
          store.loadPolicy(client, request),
          store.loadContentVersion(client, request),
          store.loadAssessments(client, request),
          store.loadAssessmentDefinition(client, { applicationNamespace: namespace, ...registration.evaluationDefinition }),
        ]);
        validateLoaded(request, registration, policy, content, availableAssessments, evaluationDefinition);
        const required = selectRequiredAssessments(request, registration.requiredAssessments, availableAssessments);
        const evaluation = runEvaluator(registration, { request, policy, content, assessments: required.selected });
        const assessmentRecordedAt = timestamp(clock);
        const assessmentInput = {
          applicationNamespace: namespace,
          assessmentDefinitionId: registration.evaluationDefinition.id,
          assessmentDefinitionVersion: registration.evaluationDefinition.version,
          contentVersionId: request.contentVersionId,
          requestId: request.requestId,
          destination: request.destination,
          evaluatorIdentity: registration.evaluatorIdentity,
          configurationReference: { id: request.policyId, version: request.policyVersion },
          outcome: required.blocked ? "inconclusive" : evaluation.outcome,
          result: { ...evaluation.result, requiredAssessmentIds: required.selected.map((value) => value.assessmentId),
            missingRequirements: required.missing },
          limitations: required.blocked ? { code: "required-assessment-unavailable" } : evaluation.limitations,
          executionReference: request.requestId,
        };
        let serviceAssessment;
        try {
          serviceAssessment = normalizeTransferAssessment(assessmentInput, { assessmentDefinition: evaluationDefinition, recordedAt: assessmentRecordedAt });
        } catch (error) {
          serviceAssessment = normalizeTransferAssessment({ ...assessmentInput, outcome: "inconclusive",
            result: { ...structuredClone(registration.failureResult), evaluatorFailure: boundedName(error),
              requiredAssessmentIds: required.selected.map((value) => value.assessmentId), missingRequirements: required.missing },
            limitations: { code: "invalid-evaluator-result" } }, { assessmentDefinition: evaluationDefinition, recordedAt: assessmentRecordedAt });
        }

        let authorization;
        let authorizationFailure = null;
        try {
          const suppliedAuthorization = await authorizeDestination({
            context: input.context,
            request: deepFreeze(structuredClone(request)),
            contentVersion: deepFreeze(structuredClone(content)),
            policy: deepFreeze(structuredClone(policy)),
            signal: input.signal,
          });
          authorization = normalizeAuthorization(suppliedAuthorization);
        } catch (error) {
          authorizationFailure = { error, checkedAt: timestamp(clock) };
        }
        input?.signal?.throwIfAborted();
        assertCurrentRequester(request, requester, requesterIdentity(accessControl.assertContext(input?.context)));
        const recordedAt = timestamp(clock);
        if (authorizationFailure) {
          authorization = failedAuthorization(authorizationFailure.error, authorizationFailure.checkedAt);
        } else if (new Date(authorization.checkedAt).getTime() > new Date(recordedAt).getTime()) {
          authorization = failedAuthorization(new TypeError("Destination authorization checked time cannot be after the decision recorded time."), recordedAt);
        }
        const allAssessments = [...required.selected, serviceAssessment];
        const blocked = required.blocked || serviceAssessment.outcome !== "passed";
        const outcome = authorization.status === "unauthorized" ? "denied"
          : authorization.status !== "authorized" || blocked ? "withheld" : "eligible";
        const decision = normalizeTransferDecision({
          applicationNamespace: namespace,
          requestId: request.requestId,
          contentVersionId: request.contentVersionId,
          destination: request.destination,
          policyId: request.policyId,
          policyVersion: request.policyVersion,
          assessments: allAssessments,
          authorization,
          outcome,
        }, { request, recordedAt });
        input?.signal?.throwIfAborted();
        assertCurrentRequester(request, requester, requesterIdentity(accessControl.assertContext(input?.context)));
        await store.insertAssessment(client, serviceAssessment);
        await store.insertDecision(client, decision);
        return Object.freeze({ decision, replayed: false, authorizationFresh: true });
      });
    },
  });
}

function normalizeRegistrations(values) {
  if (!Array.isArray(values) || values.length > 100) throw new TypeError("Transfer evaluator registrations must be a bounded array.");
  const result = new Map();
  for (const value of values) {
    assertKnownKeys(value, new Set(["applicationNamespace", "policyId", "policyVersion", "policyIdentityHash",
      "evaluatorIdentity", "evaluationDefinition", "requiredAssessments", "failureResult", "evaluate"]));
    const registration = Object.freeze({
      applicationNamespace: requireName(value.applicationNamespace, "Application namespace"),
      policyId: requireName(value.policyId, "Policy id"),
      policyVersion: positiveInteger(value.policyVersion, "Policy version"),
      policyIdentityHash: requireHash(value.policyIdentityHash, "Policy identity hash"),
      evaluatorIdentity: normalizeIdentity(value.evaluatorIdentity),
      evaluationDefinition: normalizeDefinitionRef(value.evaluationDefinition),
      requiredAssessments: normalizeRequirements(value.requiredAssessments),
      failureResult: structuredClone(value.failureResult),
      evaluate: typeof value.evaluate === "function" ? value.evaluate : invalidEvaluator(),
    });
    const key = registrationKey(registration);
    if (result.has(key)) throw new TypeError(`Duplicate transfer evaluator registration ${key}.`);
    result.set(key, registration);
  }
  return result;
}

function selectRequiredAssessments(request, requirements, values) {
  if (!Array.isArray(values) || values.length > 100) throw failClosed("Transfer assessments are invalid or unbounded.");
  const selected = [];
  const missing = [];
  let blocked = false;
  for (const requirement of requirements) {
    const matches = values.filter((value) => assessmentMatches(request, requirement, value));
    if (matches.length !== 1) { missing.push(requirementKey(requirement)); blocked = true; continue; }
    selected.push(matches[0]);
    if (matches[0].outcome !== "passed") blocked = true;
  }
  return { selected: selected.sort((a, b) => a.assessmentId.localeCompare(b.assessmentId)), missing, blocked };
}

function assessmentMatches(request, requirement, value) {
  if (!value || value.applicationNamespace !== request.applicationNamespace
    || value.contentVersionId !== request.contentVersionId
    || value.assessmentDefinitionId !== requirement.id || value.assessmentDefinitionVersion !== requirement.version
    || !sameIdentity(value.evaluatorIdentity, requirement.evaluatorIdentity)
    || !sameConfiguration(value.configurationReference, requirement.configurationReference)
    || !assessmentOutcomes.has(value.outcome)) return false;
  if (requirement.binding === "request" && value.requestId !== request.requestId) return false;
  if (requirement.binding === "destination" && (value.requestId !== request.requestId
    || JSON.stringify(value.destination) !== JSON.stringify(request.destination))) return false;
  return true;
}

function runEvaluator(registration, inputs) {
  try {
    const value = registration.evaluate(deepFreeze(structuredClone(inputs)));
    if (value && typeof value.then === "function") {
      Promise.resolve(value).catch(() => undefined);
      throw new TypeError("Transfer evaluators must be synchronous.");
    }
    assertKnownKeys(value, new Set(["outcome", "result", "limitations"]));
    if (!assessmentOutcomes.has(value.outcome)) throw new TypeError("Transfer evaluator outcome is invalid.");
    return { outcome: value.outcome, result: structuredClone(value.result), limitations: value.limitations == null ? null : structuredClone(value.limitations) };
  } catch (error) {
    return { outcome: "inconclusive", result: { ...structuredClone(registration.failureResult), evaluatorFailure: boundedName(error) },
      limitations: { code: "evaluator-failed" } };
  }
}

function validateLoaded(request, registration, policy, content, assessments, definition) {
  if (!policy || policy.applicationNamespace !== request.applicationNamespace || policy.policyId !== request.policyId
    || policy.policyVersion !== request.policyVersion || policy.identityHash !== request.policyIdentityHash) {
    throw failClosed("The exact transfer policy is unavailable.");
  }
  if (!content || content.applicationNamespace !== request.applicationNamespace || content.versionId !== request.contentVersionId
    || content.capture?.status !== "complete") throw failClosed("The exact complete content version is unavailable.");
  if (!Array.isArray(assessments)) throw failClosed("Transfer assessments are unavailable.");
  if (!definition || definition.applicationNamespace !== request.applicationNamespace
    || definition.assessmentDefinitionId !== registration.evaluationDefinition.id
    || definition.assessmentDefinitionVersion !== registration.evaluationDefinition.version) {
    throw failClosed("The transfer evaluation definition is unavailable.");
  }
}

function normalizeAuthorization(value) {
  assertKnownKeys(value, new Set(["status", "checkedAt", "authorityVersion", "evidence"]));
  if (!authorizationStatuses.has(value.status) || !value.evidence || typeof value.evidence !== "object" || Array.isArray(value.evidence)) {
    throw new TypeError("Destination authorization result is invalid.");
  }
  const checkedAt = normalizeTimestamp(value.checkedAt, "Destination authorization checked time");
  return { status: value.status, checkedAt, authorityVersion: value.authorityVersion == null ? null : requireText(value.authorityVersion, "Authority version"),
    evidence: cloneJson(value.evidence) };
}

function failedAuthorization(error, checkedAt) {
  return { status: "unknown", checkedAt, authorityVersion: null,
    evidence: { code: "authorization-check-failed", errorName: boundedName(error) } };
}

function normalizeRequirements(values = []) {
  if (!Array.isArray(values) || values.length > 50) throw new TypeError("Required assessments must be a bounded array.");
  const seen = new Set();
  return Object.freeze(values.map((value) => {
    assertKnownKeys(value, new Set(["id", "version", "evaluatorIdentity", "configurationReference", "binding"]));
    const result = Object.freeze({ id: requireName(value.id, "Assessment definition id"),
      version: positiveInteger(value.version, "Assessment definition version"),
      evaluatorIdentity: normalizeIdentity(value.evaluatorIdentity),
      configurationReference: value.configurationReference == null ? null : normalizeDefinitionRef(value.configurationReference),
      binding: ["content", "request", "destination"].includes(value.binding) ? value.binding : invalidBinding() });
    const key = requirementKey(result);
    if (seen.has(key)) throw new TypeError(`Duplicate required assessment ${key}.`);
    seen.add(key); return result;
  }));
}

function requesterIdentity(state) { return { kind: state.kind, id: requireText(state.principalId, "Requester principal id") }; }
function assertCurrentRequester(request, initial, current) {
  if (!sameIdentity(current, initial) || !sameIdentity(request.requesterIdentity, current)) {
    throw notFound("Transfer request was not found.");
  }
}
function registrationKey(value) { return JSON.stringify([value.applicationNamespace,value.policyId,value.policyVersion]); }
function requirementKey(value) { return JSON.stringify([value.id,value.version,value.evaluatorIdentity.kind,value.evaluatorIdentity.id,
  value.configurationReference?.id ?? null,value.configurationReference?.version ?? null,value.binding]); }
function normalizeDefinitionRef(value) { return { id: requireName(value?.id, "Evaluation definition id"), version: positiveInteger(value?.version, "Evaluation definition version") }; }
function normalizeIdentity(value) { return { kind: requireName(value?.kind, "Identity kind"), id: requireText(value?.id, "Identity id") }; }
function sameIdentity(left, right) { return left?.kind === right?.kind && left?.id === right?.id; }
function sameConfiguration(left, right) { return (left == null && right == null)
  || (left?.id === right?.id && left?.version === right?.version); }
function deepFreeze(value) { if (value && typeof value === "object" && !Object.isFrozen(value)) { Object.freeze(value); for (const item of Object.values(value)) deepFreeze(item); } return value; }
function cloneJson(value) { try { return JSON.parse(JSON.stringify(value)); } catch { throw new TypeError("Value must be JSON serializable."); } }
function timestamp(clock) { const value = clock(); const time = value instanceof Date ? value : new Date(value); if (!Number.isFinite(time.valueOf())) throw new TypeError("Transfer decision clock is invalid."); return time.toISOString(); }
function normalizeTimestamp(value, label) {
  if (value instanceof Date) {
    if (!Number.isFinite(value.valueOf())) throw new TypeError(`${label} is invalid.`);
    return value.toISOString();
  }
  if (typeof value !== "string") throw new TypeError(`${label} is invalid.`);
  const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?(Z|([+-])(\d{2})(?::?(\d{2}))?)$/.exec(value);
  if (!match) throw new TypeError(`${label} is invalid.`);
  const [year, month, day, hour, minute, second] = match.slice(1, 7).map(Number);
  const calendar = new Date(0);
  calendar.setUTCFullYear(year, month - 1, day);
  calendar.setUTCHours(hour, minute, second, 0);
  if (calendar.getUTCFullYear() !== year || calendar.getUTCMonth() !== month - 1 || calendar.getUTCDate() !== day
    || calendar.getUTCHours() !== hour || calendar.getUTCMinutes() !== minute || calendar.getUTCSeconds() !== second
    || (match[9] && (Number(match[10]) > 23 || Number(match[11] ?? 0) > 59))) {
    throw new TypeError(`${label} is invalid.`);
  }
  const milliseconds = Date.parse(value);
  if (!Number.isFinite(milliseconds)) throw new TypeError(`${label} is invalid.`);
  return new Date(milliseconds).toISOString();
}
function boundedName(error) { const name = typeof error?.name === "string" ? error.name : "Error"; return name.slice(0, 100); }
function requireMethod(value, method, label) { if (!value || typeof value[method] !== "function") throw new TypeError(`${label} requires ${method}().`); }
function requireText(value, label) { if (typeof value !== "string" || !value.trim()) throw new TypeError(`${label} is required.`); return value.trim(); }
function requireName(value, label) { const text = requireText(value, label); if (!/^[a-z0-9][a-z0-9._:-]{0,199}$/.test(text)) throw new TypeError(`${label} is invalid.`); return text; }
function requireHash(value, label) { const text = requireText(value, label); if (!/^[0-9a-f]{64}$/.test(text)) throw new TypeError(`${label} is invalid.`); return text; }
function requireId(value, label, prefix) { const text = requireText(value, label); if (!text.startsWith(prefix) || !/^[a-z0-9-]+$/.test(text)) throw new TypeError(`${label} is invalid.`); return text; }
function positiveInteger(value, label) { if (!Number.isInteger(value) || value < 1) throw new TypeError(`${label} must be positive.`); return value; }
function assertKnownKeys(value, allowed) { if (!value || typeof value !== "object" || Array.isArray(value)) throw new TypeError("A bounded object is required."); const unknown = Object.keys(value).filter((key) => !allowed.has(key)); if (unknown.length) throw new TypeError(`Unsupported fields: ${unknown.sort().join(", ")}.`); }
function failClosed(message) { return Object.assign(new Error(message), { code: "TRANSFER_WITHHELD" }); }
function notFound(message) { return Object.assign(new Error(message), { code: "NOT_FOUND", statusCode: 404 }); }
function invalidEvaluator() { throw new TypeError("Transfer evaluator callback is required."); }
function invalidBinding() { throw new TypeError("Required assessment binding is invalid."); }
