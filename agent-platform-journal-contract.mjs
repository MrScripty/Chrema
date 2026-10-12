import { createHash } from "node:crypto";

export function normalizeModelSettings(value, { requireModel }) {
  if (value == null && !requireModel) return null;
  if (!value || typeof value !== "object") throw new TypeError("Model settings are required.");
  return {
    model: requireModel ? requireText(value.model, "Model") : optionalText(value.model),
    reasoningEffort: optionalText(value.reasoningEffort),
    serviceTier: optionalText(value.serviceTier),
    provider: optionalText(value.provider),
  };
}

export function normalizeUsage(value) {
  if (!value || typeof value !== "object") throw new TypeError("Token usage is required.");
  const usage = {};
  for (const field of [
    "inputTokens", "cachedInputTokens", "cacheWriteInputTokens", "outputTokens",
    "reasoningOutputTokens", "totalTokens",
  ]) {
    const number = value[field] ?? 0;
    if (!Number.isInteger(number) || number < 0) throw new TypeError(`${field} must be a non-negative integer.`);
    usage[field] = number;
  }
  if (usage.totalTokens < usage.inputTokens + usage.outputTokens) {
    throw new TypeError("Total tokens cannot be less than input plus output tokens.");
  }
  if (usage.cachedInputTokens + usage.cacheWriteInputTokens > usage.inputTokens) {
    throw new TypeError("Cached and cache-write input tokens cannot exceed input tokens.");
  }
  if (usage.reasoningOutputTokens > usage.outputTokens) {
    throw new TypeError("Reasoning output tokens cannot exceed output tokens.");
  }
  return usage;
}

export function versionIdentity(version) {
  return {
    id: version.id,
    workflowType: version.workflowType,
    predecessorId: version.predecessorId,
    definition: version.definition,
    definitionHash: version.definitionHash,
    codeRevision: version.codeRevision,
    changeReason: version.changeReason,
    improvementHypothesis: version.improvementHypothesis,
    reviewedBy: version.reviewedBy,
  };
}

export function normalizeVersion(input, createdAt) {
  if (!input || typeof input !== "object") throw new TypeError("Workflow version is required.");
  const definition = cloneJson(input.definition ?? {});
  return {
    id: requireText(input.id, "Workflow version id"),
    workflowType: requireText(input.workflowType, "Workflow type"),
    predecessorId: optionalText(input.predecessorId),
    definition,
    definitionHash: hashValue(definition),
    codeRevision: requireText(input.codeRevision, "Workflow code revision"),
    changeReason: requireText(input.changeReason, "Workflow change reason"),
    improvementHypothesis: requireText(input.improvementHypothesis, "Workflow improvement hypothesis"),
    reviewedBy: requireText(input.reviewedBy, "Workflow review actor"),
    createdAt,
  };
}

export function normalizePriceVersion(input, createdAt) {
  if (!input || typeof input !== "object" || Array.isArray(input)) {
    throw new TypeError("Model price version is required.");
  }
  const price = {
    id: requireText(input.id, "Model price version id"),
    provider: requireText(input.provider, "Model price provider"),
    model: requireText(input.model, "Priced model"),
    serviceTier: optionalText(input.serviceTier),
    currency: requireText(input.currency ?? "USD", "Model price currency").toUpperCase(),
    inputPerMillion: nonNegativeNumber(input.inputPerMillion, "Input price"),
    cachedInputPerMillion: nonNegativeNumber(input.cachedInputPerMillion, "Cached-input price"),
    cacheWriteInputPerMillion: input.cacheWriteInputPerMillion == null
      ? null
      : nonNegativeNumber(input.cacheWriteInputPerMillion, "Cache-write input price"),
    outputPerMillion: nonNegativeNumber(input.outputPerMillion, "Output price"),
    reasoningOutputPerMillion: input.reasoningOutputPerMillion == null
      ? null
      : nonNegativeNumber(input.reasoningOutputPerMillion, "Reasoning-output price"),
    effectiveFrom: isoTimestamp(input.effectiveFrom, "Model price effectiveFrom"),
    effectiveUntil: input.effectiveUntil == null ? null : isoTimestamp(input.effectiveUntil, "Model price effectiveUntil"),
    sourceReference: requireText(input.sourceReference, "Model price source reference"),
    reviewedBy: requireText(input.reviewedBy, "Model price review actor"),
    createdAt: isoTimestamp(createdAt, "Model price creation time"),
  };
  if (!/^[A-Z]{3}$/.test(price.currency)) throw new TypeError("Model price currency must be a three-letter code.");
  if (price.effectiveUntil && price.effectiveUntil <= price.effectiveFrom) {
    throw new TypeError("Model price effectiveUntil must be after effectiveFrom.");
  }
  price.priceHash = hashValue({
    id: price.id,
    provider: price.provider,
    model: price.model,
    serviceTier: price.serviceTier,
    currency: price.currency,
    inputPerMillion: price.inputPerMillion,
    cachedInputPerMillion: price.cachedInputPerMillion,
    cacheWriteInputPerMillion: price.cacheWriteInputPerMillion,
    outputPerMillion: price.outputPerMillion,
    reasoningOutputPerMillion: price.reasoningOutputPerMillion,
    effectiveFrom: price.effectiveFrom,
    effectiveUntil: price.effectiveUntil,
    sourceReference: price.sourceReference,
    reviewedBy: price.reviewedBy,
  });
  return price;
}

export function normalizeAttemptPrice(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError("Attempt price must be an object.");
  }
  return { versionId: requireText(value.versionId, "Attempt price version id") };
}

export function normalizeMoney(value, label) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError(`${label} must be an object.`);
  }
  const currency = requireText(value.currency, `${label} currency`).toUpperCase();
  if (!/^[A-Z]{3}$/.test(currency)) throw new TypeError(`${label} currency must be a three-letter code.`);
  return { amount: nonNegativeNumber(value.amount, `${label} amount`), currency };
}

function hashValue(value) {
  return createHash("sha256").update(stableJson(value)).digest("hex");
}

function stableJson(value) {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stableJson(value[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

function cloneJson(value) {
  if (value == null || typeof value !== "object") throw new TypeError("Workflow JSON values must be objects or arrays.");
  try {
    return JSON.parse(JSON.stringify(value));
  } catch (error) {
    throw new TypeError("Workflow JSON values must be serializable.", { cause: error });
  }
}

function nonNegativeNumber(value, label) {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    throw new TypeError(`${label} must be a non-negative finite number.`);
  }
  return value;
}

function isoTimestamp(value, label) {
  const milliseconds = Date.parse(value);
  if (!Number.isFinite(milliseconds)) throw new TypeError(`${label} must be an ISO timestamp.`);
  return new Date(milliseconds).toISOString();
}

function requireText(value, label) {
  if (typeof value !== "string" || !value.trim()) throw new TypeError(`${label} is required.`);
  return value.trim();
}

function optionalText(value) {
  if (value == null) return null;
  return requireText(value, "Optional text");
}
