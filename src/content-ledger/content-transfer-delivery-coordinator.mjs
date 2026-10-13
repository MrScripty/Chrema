import { createHash } from "node:crypto";
import { assertDeliveryOutcomeIdentity, normalizeTransferDeliveryOutcome } from "./content-transfer-contract.mjs";

const adapterOutcomes = new Set(["delivered", "failed", "unknown"]);

// Store contract: transaction(work) supplies an opaque transaction token to every store method;
// delivery outcome persistence is awaited before the transaction and deliver call return.
export function createContentTransferDeliveryCoordinator({
  store, accessControl, registrations, authorizeDestination, clock = () => new Date(),
} = {}) {
  for (const method of ["transaction", "loadRequestForUpdate", "loadDecision", "loadDeliverableContent",
    "loadDeliveryHistory", "insertAttemptClaim", "loadAttemptForUpdate", "insertDeliveryOutcome"]) requireMethod(store, method);
  requireMethod(accessControl, "assertContext");
  if (typeof authorizeDestination !== "function") throw new TypeError("Destination authorization callback is required.");
  const registry = normalizeRegistrations(registrations);

  return Object.freeze({
    async deliver(input) {
      assertInput(input);
      input.signal?.throwIfAborted();
      const state = accessControl.assertContext(input.context);
      const namespace = requireName(input.applicationNamespace, "Application namespace");
      const requestId = requireId(input.requestId, "transfer-request-");
      const attemptKey = requireBounded(input.attemptKey, "Attempt key", 500);

      const prepared = await store.transaction(async (client) => {
        const request = await store.loadRequestForUpdate(client, namespace, requestId);
        if (!request || request.requesterIdentity?.kind !== state.kind || request.requesterIdentity?.id !== state.principalId) throw notFound();
        const [decision, content] = await Promise.all([
          store.loadDecision(client, requestId), store.loadDeliverableContent(client, request),
        ]);
        validateExact(request, decision, content);
        const history = await store.loadDeliveryHistory(client, requestId, { decisionId: decision.decisionId, attemptKey });
        const registration = registry.get(registrationKey(namespace, request.destination.kind, request.destination.action));
        if (!registration) throw blocked("No exact delivery adapter is registered.");
        const historyResult = classifyHistory(history, request, decision, attemptKey);
        if (historyResult.replay) return { replay: historyResult.replay, request, decision, content };
        if (historyResult.blocked) throw blocked(historyResult.reason);
        const claim = normalizeClaim({ request, decision, registration, attemptKey, claimedAt: timestamp(clock) });
        await store.insertAttemptClaim(client, claim);
        return { request, decision, content, registration, claim };
      });
      let authorization;
      try {
        authorization = normalizeAuthorization(await authorizeDestination({
          context: input.context, request: frozen(prepared.request), decision: frozen(prepared.decision),
          contentVersion: frozen(withoutBody(prepared.content)), signal: input.signal,
        }));
      } catch (error) {
        authorization = { status: "unknown", authorityVersion: null, evidence: { code: "authorization-check-failed", errorName: errorName(error) } };
      }
      input.signal?.throwIfAborted();

      if (prepared.replay) {
        if (authorization.status === "authorized") return Object.freeze({ ...prepared.replay, replayed: true, authorizationFresh: true });
        const receipt = prepared.replay.receipt;
        return Object.freeze({ receipt: { deliveryOutcomeId: receipt.deliveryOutcomeId, outcome: receipt.outcome,
          recordedAt: receipt.recordedAt }, replayed: true, authorizationFresh: true, detailsWithheld: true });
      }

      let adapterResult;
      if (authorization.status !== "authorized") {
        adapterResult = { outcome: "failed", failureCode: authorization.status === "unauthorized" ? "authorization-denied" : "authorization-unknown",
          details: { noEffect: true } };
      } else {
        try {
          adapterResult = normalizeAdapterResult(await prepared.registration.deliver({
            content: prepared.content.body, contentVersion: frozen(withoutBody(prepared.content)),
            request: frozen(prepared.request), decision: frozen(prepared.decision),
            destination: frozen(prepared.request.destination), idempotencyKey: prepared.claim.attemptId,
            authorization: frozen(authorization), signal: input.signal,
          }));
        } catch (error) {
          adapterResult = { outcome: "unknown", failureCode: "adapter-error", details: { errorName: errorName(error) } };
        }
      }

      let receipt;
      try {
        receipt = buildReceipt(prepared, attemptKey, adapterResult, authorization, timestamp(clock));
      } catch (error) {
        receipt = buildReceipt(prepared, attemptKey, { outcome: "unknown", failureCode: "receipt-metadata-invalid",
          details: { errorName: errorName(error) } }, { status: authorization.status,
          authorityVersion: authorization.authorityVersion, evidence: { code: "details-withheld" } }, timestamp(clock));
      }
      try {
        return await store.transaction(async (client) => {
          const claim = await store.loadAttemptForUpdate(client, prepared.claim.attemptId);
          if (!claim || claim.identityHash !== prepared.claim.identityHash) throw blocked("Delivery attempt claim is unavailable.");
          const history = await store.loadDeliveryHistory(client, requestId,
            { decisionId: prepared.decision.decisionId, attemptKey });
          const existing = exactOutcomeFromHistory(history, prepared.request, prepared.decision, attemptKey);
          if (existing) return Object.freeze({ receipt: existing, replayed: true });
          await store.insertDeliveryOutcome(client, receipt);
          return Object.freeze({ receipt, replayed: false });
        });
      } catch (error) {
        throw Object.assign(new Error("Delivery outcome is uncertain; automatic resend is blocked by the durable attempt claim."),
          { code: "DELIVERY_UNCERTAIN", cause: error });
      }
    },
  });
}

function classifyHistory(history, request, decision, attemptKey) {
  if (history?.kind === "classified-v1") {
    const classification = classifiedHistory(history, request, decision, attemptKey);
    if (classification.exactOutcome) return { replay: { receipt: classification.exactOutcome } };
    if (classification.exactUnresolvedClaim) return { blocked: true, reason: "This delivery attempt is unresolved." };
    if (classification.otherUnresolvedClaim) return { blocked: true, reason: "A prior delivery attempt is unresolved." };
    if (classification.priorDeliveredOrUnknown) return { blocked: true, reason: "A prior delivery may have completed." };
    return { blocked: false };
  }
  if (history && Object.hasOwn(history, "kind")) throw blocked("Delivery history classification is invalid.");
  if (!history || !Array.isArray(history.claims) || !Array.isArray(history.outcomes)) throw blocked("Delivery history is unavailable.");
  const exactOutcome = history.outcomes.find((value) => value.decisionId === decision.decisionId && value.attemptKey === attemptKey);
  if (exactOutcome) return { replay: { receipt: exactOutcome } };
  const exactClaim = history.claims.find((value) => value.decisionId === decision.decisionId && value.attemptKey === attemptKey);
  if (exactClaim) return { blocked: true, reason: "This delivery attempt is unresolved." };
  if (history.claims.some((claim) => !history.outcomes.some((outcome) => outcome.decisionId === claim.decisionId && outcome.attemptKey === claim.attemptKey)))
    return { blocked: true, reason: "A prior delivery attempt is unresolved." };
  if (history.outcomes.some((value) => value.outcome === "delivered" || value.outcome === "unknown"))
    return { blocked: true, reason: "A prior delivery may have completed." };
  return { blocked: false };
}

function exactOutcomeFromHistory(history, request, decision, attemptKey) {
  if (history?.kind === "classified-v1") return classifiedHistory(history, request, decision, attemptKey).exactOutcome;
  if (history && Object.hasOwn(history, "kind")) throw blocked("Delivery history classification is invalid.");
  if (!history || !Array.isArray(history.claims) || !Array.isArray(history.outcomes)) throw blocked("Delivery history is unavailable.");
  return history.outcomes.find((value) => value.decisionId === decision.decisionId && value.attemptKey === attemptKey) ?? null;
}

function classifiedHistory(history, request, decision, attemptKey) {
  const keys = ["kind", "exactOutcome", "exactUnresolvedClaim", "otherUnresolvedClaim", "priorDeliveredOrUnknown"];
  if (!history || typeof history !== "object" || Array.isArray(history)
    || Object.keys(history).length !== keys.length || keys.some((key) => !Object.hasOwn(history, key))
    || history.kind !== "classified-v1"
    || [history.exactUnresolvedClaim, history.otherUnresolvedClaim, history.priorDeliveredOrUnknown]
      .some((value) => typeof value !== "boolean")) {
    throw blocked("Delivery history classification is invalid.");
  }
  const receipt = history.exactOutcome;
  if (receipt !== null) {
    const receiptKeys = ["deliveryOutcomeId", "decisionId", "requestId", "contentVersionId", "destination",
      "attemptKey", "outcome", "details", "failureCode", "identityHash", "recordedAt"];
    if (!receipt || typeof receipt !== "object" || Array.isArray(receipt)
      || Object.keys(receipt).length !== receiptKeys.length
      || receiptKeys.some((key) => !Object.hasOwn(receipt, key))
      || receipt.decisionId !== decision.decisionId || receipt.requestId !== request.requestId
      || receipt.contentVersionId !== request.contentVersionId || receipt.attemptKey !== attemptKey
      || !receipt.destination || typeof receipt.destination !== "object" || Array.isArray(receipt.destination)
      || Object.keys(receipt.destination).length !== 3
      || ["kind", "id", "action"].some((key) => receipt.destination[key] !== request.destination[key])
      || !["delivered", "failed", "unknown"].includes(receipt.outcome)
      || typeof receipt.recordedAt !== "string" || !Number.isFinite(Date.parse(receipt.recordedAt))
      || (receipt.failureCode !== null && typeof receipt.failureCode !== "string")) {
      throw blocked("Exact delivery history receipt is invalid.");
    }
    try { assertDeliveryOutcomeIdentity(receipt); }
    catch { throw blocked("Exact delivery history receipt is invalid."); }
  }
  return history;
}

function validateExact(request, decision, content) {
  if (!decision || decision.outcome !== "eligible" || decision.deliveryEnabled !== false
    || decision.requestId !== request.requestId || decision.contentVersionId !== request.contentVersionId
    || JSON.stringify(decision.destination) !== JSON.stringify(request.destination)) throw blocked("An exact eligible decision is required.");
  if (!content || content.versionId !== request.contentVersionId || content.applicationNamespace !== request.applicationNamespace
    // A host capture declares raw bytes. A string body supplies those exact
    // UTF-8 bytes, which must still match the immutable length and digest.
    || content.capture.status !== "complete" || !["utf8-v1", "raw-bytes-v1"].includes(content.capture.hashEncoding) || typeof content.body !== "string"
    || Buffer.byteLength(content.body) !== content.capture.byteLength
    || createHash("sha256").update(content.body).digest("hex") !== content.capture.sha256) throw blocked("Exact deliverable content is unavailable.");
}

function normalizeClaim({ request, decision, registration, attemptKey, claimedAt }) {
  const base = { applicationNamespace: request.applicationNamespace, decisionId: decision.decisionId, requestId: request.requestId,
    contentVersionId: request.contentVersionId, destination: request.destination, adapterId: registration.adapterId,
    adapterVersion: registration.adapterVersion, attemptKey };
  const identityHash = hash(base);
  return Object.freeze({ ...base, attemptId: `transfer-attempt-${identityHash}`, identityHash, claimedAt });
}
function normalizeRegistrations(values) {
  if (!Array.isArray(values) || values.length > 100) throw new TypeError("Delivery registrations must be a bounded array.");
  const map = new Map();
  for (const value of values) {
    const keys = new Set(["applicationNamespace","destinationKind","destinationAction","adapterId","adapterVersion","deliver"]);
    assertKeys(value, keys);
    if (typeof value.deliver !== "function") throw new TypeError("Delivery adapter callback is required.");
    const item = Object.freeze({ applicationNamespace: requireName(value.applicationNamespace,"Application namespace"),
      destinationKind: requireName(value.destinationKind,"Destination kind"), destinationAction: requireName(value.destinationAction,"Destination action"),
      adapterId: requireName(value.adapterId,"Adapter id"), adapterVersion: positive(value.adapterVersion), deliver: value.deliver });
    const key = registrationKey(item.applicationNamespace,item.destinationKind,item.destinationAction);
    if (map.has(key)) throw new TypeError(`Duplicate delivery registration ${key}.`); map.set(key,item);
  }
  return map;
}
function normalizeAdapterResult(value) {
  assertKeys(value,new Set(["outcome","details","failureCode","noEffect"]));
  if (!adapterOutcomes.has(value.outcome) || value.outcome === "failed" && value.noEffect !== true)
    throw new TypeError("A failed delivery result must prove that no effect occurred.");
  return { outcome:value.outcome, details:value.details == null ? {} : boundedJsonObject(value.details,"Adapter details",120000),
    failureCode:value.failureCode == null ? null : requireBounded(value.failureCode,"Failure code",200) };
}
function normalizeAuthorization(value) { assertKeys(value,new Set(["status","authorityVersion","evidence"]));
  if (!["authorized","unauthorized","unknown"].includes(value.status) || !value.evidence || typeof value.evidence!=="object" || Array.isArray(value.evidence))
    throw new TypeError("Authorization result is invalid.");
  return { status:value.status, authorityVersion:value.authorityVersion==null?null:requireBounded(value.authorityVersion,"Authority version",200),
    evidence:boundedJsonObject(value.evidence,"Authorization evidence",120000) }; }
function assertInput(value) { assertKeys(value,new Set(["context","applicationNamespace","requestId","attemptKey","signal"])); }
function assertKeys(value,allowed) { if (!value || typeof value!=="object" || Array.isArray(value)) throw new TypeError("A bounded object is required."); const extra=Object.keys(value).filter((key)=>!allowed.has(key)); if(extra.length) throw new TypeError(`Unsupported fields: ${extra.sort().join(", ")}.`); }
function timestamp(clock) { const value=clock(); const date=value instanceof Date?value:new Date(value); if(!Number.isFinite(date.valueOf())) throw new TypeError("Delivery clock is invalid."); return date.toISOString(); }
function frozen(value) { return deepFreeze(structuredClone(value)); }
function deepFreeze(value) { if(value&&typeof value==="object"&&!Object.isFrozen(value)){Object.freeze(value);for(const child of Object.values(value))deepFreeze(child);}return value; }
function withoutBody(value) { const copy=structuredClone(value); delete copy.body; return copy; }
function hash(value) { return createHash("sha256").update(stable(value)).digest("hex"); }
function stable(value) { if(Array.isArray(value))return`[${value.map(stable).join(",")}]`;if(value&&typeof value==="object")return`{${Object.keys(value).sort().map((key)=>`${JSON.stringify(key)}:${stable(value[key])}`).join(",")}}`;return JSON.stringify(value); }
function requireMethod(value,method) { if(!value||typeof value[method]!=="function")throw new TypeError(`Delivery dependency requires ${method}().`); }
function requireBounded(value,label,max) { if(typeof value!=="string"||!value.trim()||value.length>max||invalidPostgresText(value))throw new TypeError(`${label} is invalid.`);return value.trim(); }
function requireName(value,label) { const text=requireBounded(value,label,200);if(!/^[a-z0-9][a-z0-9._:-]{0,199}$/.test(text))throw new TypeError(`${label} is invalid.`);return text; }
function requireId(value,prefix) { const text=requireBounded(value,"Identifier",100);if(!text.startsWith(prefix)||!/^[a-z0-9-]+$/.test(text))throw new TypeError("Identifier is invalid.");return text; }
function positive(value) { if(!Number.isInteger(value)||value<1)throw new TypeError("Adapter version must be positive.");return value; }
function registrationKey(namespace,kind,action) { return JSON.stringify([namespace,kind,action]); }
function boundedJsonObject(value,label,max) { let encoded; try { encoded=JSON.stringify(value); } catch { throw new TypeError(`${label} must be JSON.`); }
  if(encoded===undefined||Buffer.byteLength(encoded)>max)throw new TypeError(`${label} is invalid or too large.`);const parsed=JSON.parse(encoded);
  if(!parsed||typeof parsed!=="object"||Array.isArray(parsed))throw new TypeError(`${label} must be an object.`);validateJsonValues(parsed,label);return parsed; }
function validateJsonValues(value,label) { if(typeof value==="number"&&!Number.isSafeInteger(value))throw new TypeError(`${label} numbers must be safe integers.`);
  if(typeof value==="string"&&invalidPostgresText(value))throw new TypeError(`${label} contains unsupported Unicode.`);
  if(value&&typeof value==="object")for(const [key,child] of Object.entries(value)){if(invalidPostgresText(key))throw new TypeError(`${label} contains unsupported Unicode.`);validateJsonValues(child,label);} }
function errorName(error) { return (typeof error?.name==="string"?error.name:"Error").slice(0,100); }
function invalidPostgresText(value) { return /\u0000|[\uD800-\uDFFF]/u.test(value); }
function blocked(message) { return Object.assign(new Error(message),{code:"TRANSFER_BLOCKED"}); }
function notFound() { return Object.assign(new Error("Transfer request was not found."),{code:"NOT_FOUND",statusCode:404}); }

function buildReceipt(prepared,attemptKey,result,authorization,recordedAt) {
  const details=boundedJsonObject({...result.details,adapter:{id:prepared.registration.adapterId,version:prepared.registration.adapterVersion},
    authorization:{status:authorization.status,authorityVersion:authorization.authorityVersion,evidence:authorization.evidence}},
  "Combined delivery receipt details",100000);
  return normalizeTransferDeliveryOutcome({ decisionId:prepared.decision.decisionId,requestId:prepared.request.requestId,
    contentVersionId:prepared.request.contentVersionId,destination:prepared.request.destination,attemptKey,outcome:result.outcome,
    details,
    failureCode:result.failureCode },{decision:prepared.decision,request:prepared.request,recordedAt});
}
