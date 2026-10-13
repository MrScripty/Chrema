import { createHash } from "node:crypto";
import { containsProhibitedAuditSecret } from "../shared/agent-audit-safety.mjs";

const MAX_EVENT_BYTES = 1_000_000;
const MAX_SEARCH_EVIDENCE_BYTES = 100_000;
const MAX_SEARCH_ITEMS = 48;
const MAX_SEARCH_RESULTS = 20;
const usageFields = [
  "inputTokens",
  "cachedInputTokens",
  "cacheWriteInputTokens",
  "outputTokens",
  "reasoningOutputTokens",
  "totalTokens",
];

/**
 * Adapts Codex app-server notifications to the append-only workflow journal.
 * Attempt summaries and costs are always projected back from stored events.
 */
export function createCodexTelemetry({ journal, prices = [], now = () => new Date().toISOString() } = {}) {
  if (!journal || typeof journal.appendEvent !== "function" || typeof journal.inspectRun !== "function") {
    throw new TypeError("Codex telemetry requires an AgentWorkflowJournal.");
  }
  const reviewedPrices = prices.map(normalizePrice);

  return {
    async capture(message, context) {
      const normalized = normalizeMessage(message);
      const lineage = normalizeLineage(context);
      const payloadHash = hashValue(normalized);
      const identity = sourceIdentity(normalized, payloadHash);
      return journal.appendEvent({
        ...lineage,
        sourceKey: `codex:${identity}:${payloadHash.slice(0, 24)}`,
        eventType: eventType(normalized.method),
        threadId: optionalText(normalized.params?.threadId) ?? optionalText(context?.threadId),
        turnId: optionalText(normalized.params?.turnId ?? normalized.params?.turn?.id) ?? optionalText(context?.turnId),
        occurredAt: eventTimestamp(normalized),
        payload: normalized,
      });
    },

    async finalizeAttempt({ runId, assignmentId, attemptId, result = {}, error = null, billedCost = null } = {}) {
      const lineage = normalizeLineage({ runId, assignmentId, attemptId });
      const run = await journal.inspectRun({ runId: lineage.runId });
      const assignment = run.assignments.find(({ assignmentId: id }) => id === lineage.assignmentId);
      const attempt = assignment?.attempts.find(({ attemptId: id }) => id === lineage.attemptId);
      if (!attempt) throw new TypeError(`Unknown workflow attempt: ${lineage.attemptId}.`);
      const events = run.events
        .filter((event) => event.attemptId === lineage.attemptId)
        .sort(compareEvents);
      const capturedResult = captureAttemptResult(result, events, lineage.attemptId);
      if (attempt.outcome !== null) {
        const storedResult = { ...(attempt.result ?? {}) };
        delete storedResult.telemetryIssues;
        delete storedResult.telemetryFinalizedAt;
        delete storedResult.providerProfilePrices;
        const comparableResult = Object.hasOwn(storedResult, "outputCapture")
          ? capturedResult : cloneJson(result, "Attempt result");
        // Historical attempts predate this projection. Replay their original result.
        if (!Object.hasOwn(storedResult, "providerSearchEvidence")) delete comparableResult.providerSearchEvidence;
        if (!Object.hasOwn(storedResult, "outputCapture")) delete comparableResult.providerSearchEvidence;
        if (stableJson(storedResult) !== stableJson(comparableResult)
          || (error != null && error !== attempt.error)
          || (billedCost != null && stableJson(billedCost) !== stableJson(attempt.billedCost))) {
          const conflict = new Error(`Attempt ${lineage.attemptId} is already complete with a different result.`);
          conflict.code = "WORKFLOW_CONFLICT";
          conflict.statusCode = 409;
          throw conflict;
        }
        return attempt;
      }

      const projection = projectTelemetry(events);
      const profileUsage = capturedResult.providerProfileUsage ?? [];
      const price = profileUsage.length ? null : selectPrice(reviewedPrices, projection.observed, attempt.startedAt);
      const issues = [...projection.issues];
      if (!projection.completion) issues.push("missing-turn-completion");
      if (!projection.observed?.model) issues.push("missing-observed-model");
      if (!projection.observed?.provider) issues.push("missing-observed-provider");
      if (!projection.usage) issues.push("missing-token-usage");
      if (!price && !profileUsage.length) issues.push("missing-reviewed-price");

      let apiEquivalentCost = null;
      let providerProfilePrices;
      if (profileUsage.length) {
        const selectedPrices = profileUsage.map(record => selectPrice(reviewedPrices, record.observed, attempt.startedAt));
        if (profileUsage.some(record => !record.usage)) issues.push('missing-part-token-usage');
        else if (selectedPrices.some(price => !price)) issues.push('missing-reviewed-part-price');
        else if (new Set(selectedPrices.map(price => price.currency)).size !== 1) issues.push('mixed-part-price-currencies');
        else {
          const costs = profileUsage.map((record, index) => calculateApiEquivalentCost(record.usage, selectedPrices[index]));
          apiEquivalentCost = { currency: costs[0].currency, amount: Math.round(costs.reduce((sum, cost) => sum + cost.amount, 0) * 100_000_000) / 100_000_000 };
          for (const selected of new Map(selectedPrices.map(price => [price.id, price])).values()) await journal.registerPriceVersion(selected);
          providerProfilePrices = profileUsage.map((record, index) => ({ partId: record.partId, profileId: record.profileId,
            providerThreadId: record.providerThreadId, providerTurnId: record.providerTurnId, versionId: selectedPrices[index].id }));
        }
      }
      if (price && projection.usage) {
        apiEquivalentCost = calculateApiEquivalentCost(projection.usage, price);
        await journal.registerPriceVersion(price);
      }
      const telemetryStatus = projection.inconsistent
        ? "inconsistent"
        : issues.length === 0 ? "complete" : "incomplete";
      const outcome = completionOutcome(projection.completion);

      return journal.finishAttempt({
        attemptId: lineage.attemptId,
        outcome,
        observed: projection.observed,
        usage: projection.usage,
        telemetryStatus,
        price: price ? { versionId: price.id } : null,
        apiEquivalentCost,
        billedCost,
        result: {
          ...capturedResult,
          ...(providerProfilePrices ? { providerProfilePrices } : {}),
          telemetryIssues: [...new Set(issues)].sort(),
          telemetryFinalizedAt: now(),
        },
        error: redactAuditValue(error ?? completionError(projection.completion)),
      });
    },
  };
}

function captureAttemptResult(result, events, attemptId) {
  const copied = cloneJson(result, "Attempt result");
  // This metadata is computed by the host, never supplied by an agent.
  delete copied.outputCapture;
  delete copied.providerSearchEvidence;
  delete copied.providerProfileUsage;
  delete copied.providerProfilePrices;
  delete copied.rawText;
  const safe = redactAuditValue(copied);
  const terminal = events.findLast((event) => event.payload?.method === "turn/completed");
  let text = typeof result.rawText === "string" ? result.rawText : null;
  let source = text == null ? "unavailable" : "runtime-response";
  if (text == null) {
    const messages = new Map();
    for (const event of events) {
      const item = event.payload?.params?.item;
      if (event.payload?.method === "item/completed" && item?.type === "agentMessage"
        && item.phase === "final_answer" && typeof item.text === "string") {
        messages.set(item.id ?? event.sourceKey, item.text);
      }
    }
    if (messages.size) {
      text = [...messages.values()].join("\n\n");
      source = "lifecycle-final-answer";
    }
  }
  const outputCapture = { version: 1, status: "unavailable", source, text: null, sha256: null, byteLength: null };
  if (text != null) {
    outputCapture.byteLength = Buffer.byteLength(text);
    if (containsProhibitedAuditSecret(text) || text.includes("[REDACTED]")) {
      outputCapture.status = "redacted";
    } else if (outputCapture.byteLength > 500_000) {
      // Keep a digest, not an unbounded copy; never call a missing body complete.
      outputCapture.status = "partial";
      outputCapture.sha256 = createHash("sha256").update(text).digest("hex");
    } else {
      outputCapture.status = terminal?.payload?.params?.turn?.status === "completed" ? "complete" : "partial";
      outputCapture.text = text;
      outputCapture.sha256 = createHash("sha256").update(text).digest("hex");
    }
  }
  const providerProfileUsage = projectProviderProfileUsage(events);
  return { ...safe, outputCapture, providerSearchEvidence: projectProviderSearchEvidence(events, { attemptId }),
    ...(providerProfileUsage.length ? { providerProfileUsage } : {}) };
}

function projectProviderProfileUsage(events) {
  const seen = new Set();
  return events.filter(event => event.payload?.method === 'host-part/provider-observed').map(event => {
    const { partId, profileId, evidence } = event.payload.params;
    const record = { partId: requiredText(partId, 'Provider part'), profileId: requiredText(profileId, 'Provider profile'),
      providerThreadId: requiredText(evidence?.threadId, 'Provider thread'), providerTurnId: requiredText(evidence?.turnId, 'Provider turn'),
      observed: settingsFrom(evidence.observed, null), usage: normalizeProviderProfileUsage(evidence.usage) };
    const key = JSON.stringify([profileId, record.providerThreadId, record.providerTurnId]);
    if (seen.has(key)) throw new TypeError('Duplicate part provider observation.');
    seen.add(key);
    return record;
  });
}

function normalizeProviderProfileUsage(input) {
  // Required measured counters must be known before optional-counter defaults
  // are applied. The provider bounds missing/invalid counters to null.
  if (!input || typeof input !== 'object' || Array.isArray(input)
    || ['inputTokens', 'outputTokens', 'totalTokens'].some(field =>
      !Number.isSafeInteger(input[field]) || input[field] < 0)) return null;
  return normalizeUsage(input);
}

/** A bounded projection of provider-reported native web-search notifications. */
export function projectProviderSearchEvidence(events, { attemptId = null } = {}) {
  const items = [];
  const issues = new Set();
  const seen = new Set();
  const completedItems = new Map();
  const started = new Set();
  const completed = new Set();
  let searchSeen = false;
  const orderedEvents = Array.isArray(events) ? [...events].sort((left, right) => {
    const leftKey = `${left?.occurredAt ?? left?.receivedAt ?? ""}:${left?.eventId ?? left?.sourceKey ?? ""}`;
    const rightKey = `${right?.occurredAt ?? right?.receivedAt ?? ""}:${right?.eventId ?? right?.sourceKey ?? ""}`;
    return leftKey.localeCompare(rightKey);
  }) : [];
  for (const event of orderedEvents) {
    if (attemptId != null && event?.attemptId !== attemptId) continue;
    const message = event?.payload;
    const item = message?.params?.item;
    if (item?.type !== "webSearch") continue;
    if (message.method !== "item/started" && message.method !== "item/completed") continue;
    searchSeen = true;
    // Capture normalizes envelope lineage; the payload retains provider bytes.
    const providerThreadId = message.params.threadId ?? event.threadId;
    const providerTurnId = message.params.turnId ?? message.params.turn?.id ?? event.turnId;
    const rawThreadId = safeSearchIdentifier(providerThreadId);
    const rawTurnId = safeSearchIdentifier(providerTurnId);
    const rawItemId = safeSearchIdentifier(item.id);
    const threadId = projectSearchIdentifier(providerThreadId, "thread", issues);
    const turnId = projectSearchIdentifier(providerTurnId, "turn", issues);
    const itemId = projectSearchIdentifier(item.id, "item", issues);
    // Group by exact raw identity, including identifiers too long to publish.
    // Missing or unsafe identities cannot establish an item lifecycle.
    const identity = rawThreadId !== null && rawTurnId !== null && rawItemId !== null
      ? JSON.stringify([rawThreadId, rawTurnId, rawItemId]) : null;
    if (message.method === "item/started") {
      if (identity !== null) started.add(identity);
      continue;
    }
    if (identity !== null) completed.add(identity);
    const eventId = projectSearchIdentifier(event.eventId, "event", issues);
    const rawEventId = safeSearchIdentifier(event.eventId);
    const replayKey = rawEventId !== null
      ? JSON.stringify(["event", rawEventId])
      : JSON.stringify(["fallback", safeSearchIdentifier(event.sourceKey) ?? JSON.stringify(message)]);
    if (seen.has(replayKey)) continue;
    seen.add(replayKey);
    if (identity !== null) {
      const prior = completedItems.get(identity) ?? new Set();
      const fingerprint = hashValue(message.params);
      if (prior.has(fingerprint)) continue;
      if (prior.size) issues.add("conflicting-search-item-content");
      prior.add(fingerprint);
      completedItems.set(identity, prior);
    }
    if (items.length >= MAX_SEARCH_ITEMS) {
      issues.add("search-item-limit");
      continue;
    }
    const action = item.action;
    const queryMetadata = projectSearchQueryMetadata(action);
    if (!action || typeof action !== "object" || Array.isArray(action)
      || action.type !== "search") {
      issues.add("unknown-search-action-format");
    }
    if (!Array.isArray(item.results)) issues.add("unknown-search-results-format");
    const rawResults = Array.isArray(item.results) ? item.results : [];
    if (rawResults.length > MAX_SEARCH_RESULTS) issues.add("search-result-limit");
    const results = [];
    for (const result of rawResults.slice(0, MAX_SEARCH_RESULTS)) {
      if (!result || result.type !== "text_result" || typeof result.url !== "string") {
        issues.add("unknown-search-result-format");
        continue;
      }
      // A shortened URL is a different resource and cannot be matching evidence.
      if (result.url.length > 2_000) {
        issues.add("search-result-url-limit");
        continue;
      }
      const url = boundedSearchText(result.url, 2_000, issues);
      if (!url || !validSearchUrl(url)) {
        issues.add("invalid-search-result-url");
        continue;
      }
      results.push({
        refId: projectSearchIdentifier(result.ref_id, "ref", issues),
        url,
        title: boundedSearchText(result.title, 300, issues),
        domain: boundedSearchText(result.domain, 200, issues),
        snippet: boundedSearchText(result.snippet, 600, issues),
      });
    }
    const projected = {
      eventId,
      threadId,
      turnId,
      itemId,
      occurredAt: boundedSearchText(event.occurredAt ?? event.receivedAt, 64, issues),
      query: boundedSearchText(item.query, 500, new Set()),
      action: {
        type: boundedSearchText(action?.type, 80, issues),
        ...queryMetadata,
      },
      results,
    };
    if (Buffer.byteLength(JSON.stringify([...items, projected])) > MAX_SEARCH_EVIDENCE_BYTES) {
      issues.add("search-evidence-byte-limit");
      continue;
    }
    items.push(projected);
  }
  for (const identity of started) {
    if (!completed.has(identity)) issues.add("incomplete-search-item");
  }
  const projection = {
    version: 1,
    status: !searchSeen ? "unavailable" : items.length && issues.size === 0 ? "complete" : "partial",
    origin: "provider-reported-search",
    attemptId,
    items,
    issues: [...issues].sort(),
  };
  // The item budget excludes metadata. Enforce the limit on the full envelope,
  // including the issue that records any omitted items.
  if (Buffer.byteLength(JSON.stringify(projection)) > MAX_SEARCH_EVIDENCE_BYTES) {
    issues.add("search-evidence-byte-limit");
    projection.status = "partial";
    projection.issues = [...issues].sort();
    while (items.length && Buffer.byteLength(JSON.stringify(projection)) > MAX_SEARCH_EVIDENCE_BYTES) {
      items.pop();
    }
    // attemptId is the only unbounded envelope field; never shorten its identity.
    if (Buffer.byteLength(JSON.stringify(projection)) > MAX_SEARCH_EVIDENCE_BYTES) {
      projection.attemptId = null;
    }
  }
  return projection;
}

// Advisory query status: available is safe exact text, absent is no text, omitted is malformed or unsafe metadata.
// Query omissions do not change the completeness of exact result/event capture.
function projectSearchQueryMetadata(action) {
  const absent = { queries: [], queryMetadataStatus: "absent" };
  const omitted = { queries: [], queryMetadataStatus: "omitted" };
  if (!action || typeof action !== "object" || Array.isArray(action)) return absent;
  const queries = Object.hasOwn(action, "queries") ? action.queries
    : Object.hasOwn(action, "query") ? [action.query] : [];
  if (!Array.isArray(queries) || queries.length > 8) return omitted;
  if (queries.length === 0) return absent;
  const projected = [];
  for (let index = 0; index < queries.length; index += 1) {
    if (!Object.hasOwn(queries, index)) return omitted;
    const query = queries[index];
    if (typeof query !== "string" || !query.trim() || query.length > 500
      || containsProhibitedAuditSecret(query) || query.includes("[REDACTED]")) return omitted;
    projected.push(query);
  }
  return { queries: projected, queryMetadataStatus: "available" };
}

function safeSearchIdentifier(value) {
  if (typeof value !== "string" || value.length === 0
    || containsProhibitedAuditSecret(value) || value.includes("[REDACTED]")) return null;
  return value;
}

function projectSearchIdentifier(value, name, issues) {
  if (value == null || value === "") {
    issues.add(`missing-${name}-id`);
    return null;
  }
  if (typeof value === "string" && (containsProhibitedAuditSecret(value) || value.includes("[REDACTED]"))) {
    issues.add(`redacted-${name}-id`);
    return null;
  }
  if (safeSearchIdentifier(value) === null) {
    issues.add(`invalid-${name}-id`);
    return null;
  }
  if (value.length > 160) {
    issues.add(`${name}-id-limit`);
    return null;
  }
  return value;
}

function boundedSearchText(value, maximumLength, issues) {
  if (typeof value !== "string") return null;
  if (containsProhibitedAuditSecret(value) || value.includes("[REDACTED]")) {
    issues.add("redacted-search-field");
    return null;
  }
  if (value.length > maximumLength) {
    issues.add("truncated-search-field");
    return value.slice(0, maximumLength);
  }
  return value;
}

function validSearchUrl(value) {
  try {
    const url = new URL(value);
    return (url.protocol === "http:" || url.protocol === "https:")
      && !!url.hostname && !url.username && !url.password;
  } catch {
    return false;
  }
}

export function calculateApiEquivalentCost(usage, inputPrice) {
  const price = normalizePrice(inputPrice);
  const tokens = normalizeUsage(usage);
  const uncachedInput = tokens.inputTokens - tokens.cachedInputTokens - tokens.cacheWriteInputTokens;
  if (uncachedInput < 0) throw new TypeError("Cached and cache-write input tokens cannot exceed input tokens.");
  if (tokens.cacheWriteInputTokens > 0 && price.cacheWriteInputPerMillion == null) {
    throw new TypeError("A cache-write price is required when cache-write tokens were observed.");
  }
  const nonReasoningOutput = tokens.outputTokens - tokens.reasoningOutputTokens;
  const reasoningRate = price.reasoningOutputPerMillion ?? price.outputPerMillion;
  const millionTokenCost =
    uncachedInput * price.inputPerMillion
    + tokens.cachedInputTokens * price.cachedInputPerMillion
    + tokens.cacheWriteInputTokens * (price.cacheWriteInputPerMillion ?? 0)
    + nonReasoningOutput * price.outputPerMillion
    + tokens.reasoningOutputTokens * reasoningRate;
  return {
    amount: Math.round((millionTokenCost / 1_000_000) * 100_000_000) / 100_000_000,
    currency: price.currency,
  };
}

function projectTelemetry(events) {
  let observed = null;
  let completion = null;
  const usageEvents = [];
  const issues = [];

  for (const event of events) {
    const message = event.payload;
    if (message.method === "thread/settings/updated") {
      observed = settingsFrom(message.params, observed);
    } else if (isReroute(message.method)) {
      observed = settingsFrom(message.params, observed);
    } else if (message.method === "thread/tokenUsage/updated") {
      const tokenUsage = message.params?.tokenUsage;
      if (tokenUsage?.last) {
        usageEvents.push({
          turnId: message.params?.turnId ?? event.turnId ?? null,
          kind: "request",
          usage: normalizeUsage(tokenUsage.last),
          total: tokenUsage.total ? normalizeUsage(tokenUsage.total) : null,
        });
      } else if (tokenUsage?.total) {
        usageEvents.push({
          turnId: message.params?.turnId ?? event.turnId ?? null,
          kind: "cumulative",
          usage: normalizeUsage(tokenUsage.total),
        });
      }
    } else if (message.method === "turn/completed") {
      if (completion && hashValue(completion) !== hashValue(message.params ?? {})) {
        issues.push("conflicting-turn-completions");
      }
      completion = message.params ?? {};
    }
  }

  const profileUsage = projectProviderProfileUsage(events);
  if (profileUsage.length && usageEvents.length) issues.push('mixed-part-and-thread-token-usage');
  const usageProjection = profileUsage.some(record => !record.usage) ? { usage: null, issues: [] }
    : projectUsage(profileUsage.length ? profileUsage.map(record => ({ kind: 'request',
      turnId: JSON.stringify([record.profileId, record.providerThreadId, record.providerTurnId]), usage: record.usage })) : usageEvents);
  issues.push(...usageProjection.issues);
  return {
    observed,
    completion,
    usage: usageProjection.usage,
    issues,
    inconsistent: issues.length > 0,
  };
}

function projectUsage(events) {
  if (events.length === 0) return { usage: null, issues: [] };
  const requests = events.filter(({ kind }) => kind === "request");
  if (requests.length) {
    if (requests.length !== events.length) {
      return { usage: null, issues: ["mixed-token-usage-notification-shapes"] };
    }
    return projectRequestUsage(requests);
  }
  const aggregate = zeroUsage();
  const issues = [];
  let previousCumulative = null;
  let currentTurnId = null;
  let finalForTurn = null;

  function closeTurn() {
    if (!finalForTurn) return;
    if (previousCumulative && countersAtLeast(finalForTurn, previousCumulative)) {
      addUsage(aggregate, subtractUsage(finalForTurn, previousCumulative));
    } else {
      addUsage(aggregate, finalForTurn);
    }
    previousCumulative = finalForTurn;
    finalForTurn = null;
  }

  for (const event of events) {
    if (finalForTurn && event.turnId !== currentTurnId) closeTurn();
    if (finalForTurn && event.turnId === currentTurnId && !countersAtLeast(event.usage, finalForTurn)) {
      issues.push(`decreased-cumulative-token-counter:${currentTurnId ?? "unknown-turn"}`);
    }
    currentTurnId = event.turnId;
    finalForTurn = event.usage;
  }
  closeTurn();
  return { usage: aggregate, issues };
}

function projectRequestUsage(events) {
  const aggregate = zeroUsage();
  const issues = [];
  let previous = null;
  for (const event of events) {
    if (event.total && previous?.total && event.turnId === previous.turnId) {
      if (!countersAtLeast(event.total, previous.total)) {
        issues.push(`decreased-cumulative-token-counter:${event.turnId ?? "unknown-turn"}`);
      } else if (!sameUsage(subtractUsage(event.total, previous.total), event.usage)) {
        issues.push(`request-token-counter-mismatch:${event.turnId ?? "unknown-turn"}`);
      }
    }
    addUsage(aggregate, event.usage);
    previous = event;
  }
  return { usage: aggregate, issues };
}

function sameUsage(left, right) {
  return usageFields.every((field) => left[field] === right[field]);
}

function settingsFrom(params = {}, previous = null) {
  const source = params.threadSettings && typeof params.threadSettings === "object"
    ? params.threadSettings
    : params.settings && typeof params.settings === "object" ? params.settings : params;
  const model = optionalText(source.model ?? source.toModel ?? source.newModel) ?? previous?.model ?? null;
  const provider = optionalText(source.modelProvider ?? source.provider ?? source.toProvider) ?? previous?.provider ?? null;
  const reasoningEffort = optionalText(source.reasoningEffort ?? source.effort) ?? previous?.reasoningEffort ?? null;
  const serviceTier = optionalText(source.serviceTier) ?? previous?.serviceTier ?? null;
  return { model, provider, reasoningEffort, serviceTier };
}

function selectPrice(prices, observed, startedAt) {
  if (!observed?.model || !observed?.provider) return null;
  const at = Date.parse(startedAt);
  return prices
    .filter((price) => price.provider === observed.provider
      && price.model === observed.model
      && (price.serviceTier ?? null) === (observed.serviceTier ?? null)
      && Date.parse(price.effectiveFrom) <= at
      && (price.effectiveUntil == null || at < Date.parse(price.effectiveUntil)))
    .sort((left, right) => right.effectiveFrom.localeCompare(left.effectiveFrom))[0] ?? null;
}

function completionOutcome(completion) {
  const status = completion?.turn?.status ?? completion?.status;
  if (status === "completed") return "completed";
  if (["interrupted", "cancelled", "canceled"].includes(status)) return "interrupted";
  return "failed";
}

function completionError(completion) {
  const value = completion?.turn?.error ?? completion?.error;
  if (value == null) return null;
  return typeof value === "string" ? value : stableJson(value);
}

function normalizePrice(input) {
  if (!input || typeof input !== "object" || Array.isArray(input)) throw new TypeError("A reviewed model price is required.");
  const normalized = {
    id: requiredText(input.id, "Price id"),
    provider: requiredText(input.provider, "Price provider"),
    model: requiredText(input.model, "Priced model"),
    serviceTier: optionalText(input.serviceTier),
    currency: requiredText(input.currency, "Price currency").toUpperCase(),
    inputPerMillion: nonNegative(input.inputPerMillion, "Input price"),
    cachedInputPerMillion: nonNegative(input.cachedInputPerMillion, "Cached-input price"),
    cacheWriteInputPerMillion: input.cacheWriteInputPerMillion == null ? null : nonNegative(input.cacheWriteInputPerMillion, "Cache-write price"),
    outputPerMillion: nonNegative(input.outputPerMillion, "Output price"),
    reasoningOutputPerMillion: input.reasoningOutputPerMillion == null ? null : nonNegative(input.reasoningOutputPerMillion, "Reasoning-output price"),
    effectiveFrom: iso(input.effectiveFrom, "Price effectiveFrom"),
    effectiveUntil: input.effectiveUntil == null ? null : iso(input.effectiveUntil, "Price effectiveUntil"),
    sourceReference: requiredText(input.sourceReference, "Price source reference"),
    reviewedBy: requiredText(input.reviewedBy, "Price reviewer"),
  };
  if (!/^[A-Z]{3}$/.test(normalized.currency)) throw new TypeError("Price currency must be a three-letter code.");
  return normalized;
}

function normalizeUsage(input) {
  if (!input || typeof input !== "object" || Array.isArray(input)) throw new TypeError("Token usage is required.");
  const usage = {};
  for (const field of usageFields) {
    const value = input[field] ?? 0;
    if (!Number.isInteger(value) || value < 0) throw new TypeError(`${field} must be a non-negative integer.`);
    usage[field] = value;
  }
  if (usage.cachedInputTokens + usage.cacheWriteInputTokens > usage.inputTokens) {
    throw new TypeError("Cached and cache-write input tokens cannot exceed input tokens.");
  }
  if (usage.reasoningOutputTokens > usage.outputTokens) {
    throw new TypeError("Reasoning output tokens cannot exceed output tokens.");
  }
  if (usage.totalTokens < usage.inputTokens + usage.outputTokens) {
    throw new TypeError("Total tokens cannot be less than input plus output tokens.");
  }
  return usage;
}

function normalizeMessage(message) {
  if (!message || typeof message !== "object" || Array.isArray(message)) throw new TypeError("A Codex notification is required.");
  const normalized = redactAuditValue({
    method: requiredText(message.method, "Codex notification method"),
    params: message.params ?? {},
    ...(message.emittedAtMs == null ? {} : { emittedAtMs: finiteTimestamp(message.emittedAtMs) }),
  });
  const json = stableJson(normalized);
  if (Buffer.byteLength(json) > MAX_EVENT_BYTES) throw new TypeError(`Codex notification exceeds ${MAX_EVENT_BYTES} bytes.`);
  return JSON.parse(json);
}

function normalizeLineage(context) {
  return {
    runId: requiredText(context?.runId, "Workflow run id"),
    assignmentId: requiredText(context?.assignmentId, "Workflow assignment id"),
    attemptId: requiredText(context?.attemptId, "Workflow attempt id"),
  };
}

function sourceIdentity(message, payloadHash) {
  const params = message.params ?? {};
  const thread = params.threadId ?? params.thread?.id ?? "unknown-thread";
  const turn = params.turnId ?? params.turn?.id ?? "unknown-turn";
  const upstream = params.eventId ?? params.id ?? message.id;
  return [message.method, thread, turn, upstream ?? payloadHash.slice(0, 16)]
    .map((part) => String(part).replaceAll(/[^A-Za-z0-9._:-]/g, "_"))
    .join(":")
    .slice(0, 440);
}

function eventType(method) {
  return `codex.${method.replaceAll("/", ".")}`;
}

function isReroute(method) {
  return method === "model/rerouted" || method === "turn/model/rerouted" || method === "thread/model/rerouted";
}

function eventTimestamp(message) {
  if (message.emittedAtMs != null) return new Date(message.emittedAtMs).toISOString();
  const params = message.params;
  for (const value of [params?.occurredAt, params?.timestamp, params?.createdAt, params?.completedAt]) {
    if (value != null && Number.isFinite(Date.parse(value))) return new Date(value).toISOString();
  }
  return null;
}

function finiteTimestamp(value) {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    throw new TypeError("Codex notification emittedAtMs must be a non-negative finite number.");
  }
  return value;
}

function compareEvents(left, right) {
  const leftTime = left.occurredAt ?? left.receivedAt;
  const rightTime = right.occurredAt ?? right.receivedAt;
  return leftTime.localeCompare(rightTime);
}

function redactAuditValue(value, key = null) {
  if (Array.isArray(value)) return value.map((item) => redactAuditValue(item, key));
  if (value && typeof value === "object") {
    return Object.fromEntries(Object.entries(value).map(([childKey, child]) => [
      childKey,
      redactAuditValue(child, childKey),
    ]));
  }
  if (typeof value !== "string") return value;
  if (key === "prompt") {
    if (containsProhibitedAuditSecret(value)) {
      throw new TypeError("Codex prompt contains prohibited secret material; it was not journaled.");
    }
    if (value.length > 200_000) throw new TypeError("Codex prompt must be at most 200000 characters.");
    return value;
  }
  if (sensitiveKey(key) || containsProhibitedAuditSecret(value)) return "[REDACTED]";
  return value;
}

function sensitiveKey(key) {
  return typeof key === "string" && /^(authorization|cookie|set-cookie|password|passwd|secret|api[-_]?key|access[-_]?token|refresh[-_]?token|database[-_]?url)$/i.test(key);
}

function zeroUsage() {
  return Object.fromEntries(usageFields.map((field) => [field, 0]));
}

function addUsage(target, source) {
  for (const field of usageFields) target[field] += source[field];
}

function subtractUsage(left, right) {
  return Object.fromEntries(usageFields.map((field) => [field, left[field] - right[field]]));
}

function countersAtLeast(left, right) {
  return usageFields.every((field) => left[field] >= right[field]);
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

function cloneJson(value, label) {
  if (!value || typeof value !== "object") throw new TypeError(`${label} must be an object or array.`);
  return JSON.parse(stableJson(value));
}

function requiredText(value, label) {
  if (typeof value !== "string" || !value.trim()) throw new TypeError(`${label} is required.`);
  return value.trim();
}

function optionalText(value) {
  if (value == null) return null;
  return requiredText(value, "Optional text");
}

function nonNegative(value, label) {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) throw new TypeError(`${label} must be a non-negative number.`);
  return value;
}

function iso(value, label) {
  const milliseconds = Date.parse(value);
  if (!Number.isFinite(milliseconds)) throw new TypeError(`${label} must be an ISO timestamp.`);
  return new Date(milliseconds).toISOString();
}
