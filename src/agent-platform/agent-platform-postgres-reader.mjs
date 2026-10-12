import { createHash } from "node:crypto";
import { performance } from "node:perf_hooks";

const DEFAULT_MAX_ROWS = 50;
const HARD_MAX_ROWS = 100;
const MAX_SQL_BYTES = 100_000;
const MAX_PARAMETERS = 100;
const MAX_PARAMETER_BYTES = 100_000;
const MAX_ROW_BYTES = 16_384;
const MAX_RESPONSE_BYTES = 524_288;
const STATEMENT_TIMEOUT_MS = 10_000;
const LOCK_TIMEOUT_MS = 2_000;
const IDLE_TRANSACTION_TIMEOUT_MS = 10_000;
const persistentSessionLockPattern = /\bpg_(?:try_)?advisory_(?:xact_)?lock(?:_shared)?\b/i;

/**
 * A narrow host-owned interface for model-authored PostgreSQL reads. PostgreSQL
 * permissions remain authoritative; this module adds statement and output
 * bounds without exposing a connection or credential to the model.
 */
export function createAgentPostgresReaderFactory({
  issueReadContext = null,
  configureTransaction,
  validateReadContext,
  rowOmissionMarker = "$agent",
} = {}) {
  if (issueReadContext !== null && typeof issueReadContext !== "function") {
    throw new TypeError("Agent PostgreSQL read-context issuer must be callable or null.");
  }
  if (typeof configureTransaction !== "function") {
    throw new TypeError("Agent PostgreSQL transaction configuration must be callable.");
  }
  if (typeof validateReadContext !== "function") {
    throw new TypeError("Agent PostgreSQL read-context validation must be callable.");
  }
  if (typeof rowOmissionMarker !== "string" || !/^\$[A-Za-z][A-Za-z0-9_]{0,63}$/.test(rowOmissionMarker)) {
    throw new TypeError("Agent PostgreSQL row-omission marker must be a valid reserved key.");
  }
  const issueContext = issueReadContext;
  const configure = configureTransaction;
  const validate = validateReadContext;
  const omissionMarker = rowOmissionMarker;

  return function createAgentPostgresReader({
    database,
    maxRows = HARD_MAX_ROWS,
    maxRowBytes = MAX_ROW_BYTES,
    maxResponseBytes = MAX_RESPONSE_BYTES,
    statementTimeoutMs = STATEMENT_TIMEOUT_MS,
    lockTimeoutMs = LOCK_TIMEOUT_MS,
    idleTransactionTimeoutMs = IDLE_TRANSACTION_TIMEOUT_MS,
    clock = () => performance.now(),
  } = {}) {
    if (!database || typeof database.transaction !== "function") {
      throw new TypeError("AgentPostgresReader requires a transaction-capable database.");
    }
    const limits = Object.freeze({
      maxRows: boundedInteger(maxRows, "Agent PostgreSQL maximum rows", 1, HARD_MAX_ROWS),
      maxRowBytes: boundedInteger(maxRowBytes, "Agent PostgreSQL maximum row bytes", 256, MAX_ROW_BYTES),
      maxResponseBytes: boundedInteger(maxResponseBytes, "Agent PostgreSQL maximum response bytes", 1_024, MAX_RESPONSE_BYTES),
      statementTimeoutMs: boundedInteger(statementTimeoutMs, "Agent PostgreSQL statement timeout", 1, 60_000),
      lockTimeoutMs: boundedInteger(lockTimeoutMs, "Agent PostgreSQL lock timeout", 1, 60_000),
      idleTransactionTimeoutMs: boundedInteger(idleTransactionTimeoutMs, "Agent PostgreSQL idle transaction timeout", 1, 60_000),
    });

    return Object.freeze({
      limits,
      async query(input, { signal, runId } = {}) {
        signal?.throwIfAborted();
        const request = normalizeAgentPostgresQuery(input, { maximumRows: limits.maxRows });
        const started = clock();
        try {
          const readContext = issueContext ? await issueContext({ runId }) : null;
          const selected = await database.transaction(async (client) => {
            await client.query("SET TRANSACTION READ ONLY");
            await client.query(`SET LOCAL statement_timeout='${limits.statementTimeoutMs}ms'`);
            await client.query(`SET LOCAL lock_timeout='${limits.lockTimeoutMs}ms'`);
            await client.query(`SET LOCAL idle_in_transaction_session_timeout='${limits.idleTransactionTimeoutMs}ms'`);
            await configure(client, readContext);
            const wrapped = wrapReadQuery(request.sql, request.maxRows, limits.maxRowBytes, omissionMarker);
            return client.query(wrapped, request.parameters);
          }, { signal });
          signal?.throwIfAborted();
          if (issueContext) {
            const current = await issueContext({ runId });
            validate(current, readContext);
          }
          const normalized = normalizeRows(selected?.rows, request.maxRows, limits.maxResponseBytes, omissionMarker);
          const result = {
            schemaVersion: 1,
            querySha256: request.querySha256,
            rows: normalized.rows,
            returnedRowCount: normalized.rows.length,
            truncated: normalized.truncated,
            truncationReasons: normalized.truncationReasons,
            oversizedRowCount: normalized.oversizedRowCount,
            durationMs: elapsedMilliseconds(clock() - started),
          };
          return Object.freeze({
            ...result,
            resultSha256: hashJson({
              schemaVersion: result.schemaVersion,
              querySha256: result.querySha256,
              rows: result.rows,
              returnedRowCount: result.returnedRowCount,
              truncated: result.truncated,
              truncationReasons: result.truncationReasons,
              oversizedRowCount: result.oversizedRowCount,
            }),
          });
        } catch (error) {
          if (error instanceof AgentPostgresQueryError) throw error;
          throw databaseQueryError(error, request.querySha256, clock() - started);
        }
      },
    });
  };
}

export class AgentPostgresQueryError extends Error {
  constructor(message, { code = "AGENT_POSTGRES_QUERY_REJECTED", querySha256 = null, durationMs = 0 } = {}) {
    super(message);
    this.name = "AgentPostgresQueryError";
    this.code = code;
    this.querySha256 = querySha256;
    this.durationMs = durationMs;
  }
}

export function normalizeAgentPostgresQuery(input, { maximumRows = HARD_MAX_ROWS } = {}) {
  if (!input || typeof input !== "object" || Array.isArray(input)) {
    throw new AgentPostgresQueryError("A PostgreSQL query object is required.");
  }
  if (Object.keys(input).some((key) => !["sql", "parameters", "maxRows"].includes(key))) {
    throw new AgentPostgresQueryError("The PostgreSQL query contains unsupported fields.");
  }
  if (typeof input.sql !== "string" || !input.sql.trim()) {
    throw new AgentPostgresQueryError("PostgreSQL query text is required.");
  }
  let sql = input.sql.trim();
  if (Buffer.byteLength(sql) > MAX_SQL_BYTES) {
    throw new AgentPostgresQueryError(`PostgreSQL query text exceeds ${MAX_SQL_BYTES} bytes.`);
  }
  if (persistentSessionLockPattern.test(sql)) {
    throw new AgentPostgresQueryError("PostgreSQL advisory-lock functions are not available to agents.");
  }
  sql = sql.replace(/;\s*$/, "").trim();
  if (!sql) throw new AgentPostgresQueryError("PostgreSQL query text is required.");
  const parameters = input.parameters ?? [];
  if (!Array.isArray(parameters) || parameters.length > MAX_PARAMETERS) {
    throw new AgentPostgresQueryError(`PostgreSQL parameters must be an array with at most ${MAX_PARAMETERS} entries.`);
  }
  let encodedParameters;
  try {
    encodedParameters = JSON.stringify(parameters);
  } catch {
    throw new AgentPostgresQueryError("PostgreSQL parameters must be JSON-serializable.");
  }
  if (encodedParameters === undefined || Buffer.byteLength(encodedParameters) > MAX_PARAMETER_BYTES) {
    throw new AgentPostgresQueryError(`PostgreSQL parameters exceed ${MAX_PARAMETER_BYTES} bytes.`);
  }
  const normalizedParameters = JSON.parse(encodedParameters);
  const allowedMaximum = boundedInteger(maximumRows, "Agent PostgreSQL maximum rows", 1, HARD_MAX_ROWS);
  const maxRows = input.maxRows == null
    ? Math.min(DEFAULT_MAX_ROWS, allowedMaximum)
    : boundedInteger(input.maxRows, "PostgreSQL maxRows", 1, allowedMaximum);
  return Object.freeze({
    sql,
    parameters: Object.freeze(normalizedParameters),
    maxRows,
    querySha256: hashJson({ sql, parameters: normalizedParameters }),
  });
}

function wrapReadQuery(sql, maxRows, maxRowBytes, rowOmissionMarker) {
  return `
    SELECT CASE
      WHEN octet_length(encoded.row_data::text)<=${maxRowBytes} THEN encoded.row_data
      ELSE jsonb_build_object(
        '${rowOmissionMarker}','row-omitted',
        'reason','row-byte-limit',
        'bytes',octet_length(encoded.row_data::text)
      )
    END AS row_data
    FROM (
      SELECT to_jsonb(agent_row) AS row_data
      FROM (${sql}) AS agent_row
      LIMIT ${maxRows + 1}
    ) AS encoded
  `;
}

function normalizeRows(rows, maxRows, maxResponseBytes, rowOmissionMarker) {
  if (!Array.isArray(rows)) throw new Error("PostgreSQL returned an invalid row collection.");
  const output = [];
  const reasons = new Set();
  let responseBytes = 2;
  let oversizedRowCount = 0;
  if (rows.length > maxRows) reasons.add("row-limit");
  for (const row of rows.slice(0, maxRows)) {
    const value = row?.row_data;
    if (value === undefined) throw new Error("PostgreSQL returned an invalid bounded row.");
    if (value?.[rowOmissionMarker] === "row-omitted") oversizedRowCount += 1;
    const encoded = JSON.stringify(value);
    const bytes = Buffer.byteLength(encoded) + (output.length ? 1 : 0);
    if (responseBytes + bytes > maxResponseBytes) {
      reasons.add("response-byte-limit");
      break;
    }
    output.push(value);
    responseBytes += bytes;
  }
  if (oversizedRowCount) reasons.add("row-byte-limit");
  return {
    rows: Object.freeze(output),
    truncated: reasons.size > 0,
    truncationReasons: Object.freeze([...reasons].sort()),
    oversizedRowCount,
  };
}

function databaseQueryError(error, querySha256, elapsed) {
  const sqlState = typeof error?.code === "string" && /^[0-9A-Z]{5}$/.test(error.code) ? error.code : null;
  const raw = error instanceof Error ? error.message : String(error);
  const message = raw.replace(/[\r\n\t]+/g, " ").slice(0, 2_000) || "PostgreSQL rejected the agent query.";
  return new AgentPostgresQueryError(message, {
    code: sqlState ? `POSTGRES_${sqlState}` : "AGENT_POSTGRES_QUERY_FAILED",
    querySha256,
    durationMs: elapsedMilliseconds(elapsed),
  });
}

function boundedInteger(value, label, minimum, maximum) {
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw new AgentPostgresQueryError(`${label} must be an integer from ${minimum} through ${maximum}.`);
  }
  return value;
}

function elapsedMilliseconds(value) {
  return Math.max(0, Math.round(Number(value) * 1_000) / 1_000);
}

function hashJson(value) {
  return createHash("sha256").update(stableJson(value)).digest("hex");
}

function stableJson(value) {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stableJson(value[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}
