import { createHash } from "node:crypto";

const secretPatterns = Object.freeze([
  /\bBearer\s+\S+/i,
  /\bpostgres(?:ql)?:\/\/[^\s:/]+:[^\s@/]+@/i,
  /\bsk-[A-Za-z0-9_-]{16,}\b/,
]);

export function containsProhibitedAuditSecret(value) {
  return typeof value === "string" && secretPatterns.some((pattern) => pattern.test(value));
}

export function assertSafeAuditPrompt(value, { label = "Audit prompt", maximumLength = 200_000 } = {}) {
  if (typeof value !== "string" || !value.trim()) throw new TypeError(`${label} is required.`);
  if (value.length > maximumLength) throw new TypeError(`${label} must be at most ${maximumLength} characters.`);
  if (containsProhibitedAuditSecret(value)) {
    throw new TypeError(`${label} contains prohibited secret material; it was not journaled.`);
  }
  return value.trim();
}

/** Capture the bounded JSON actually disclosed, not an unbounded reconstruction. */
export function captureBoundedAuditValue(value, { maximumBytes = 550_000 } = {}) {
  const empty = { version: 1, status: "unavailable", value: null, sha256: null, byteLength: null };
  if (value === undefined) return empty;
  let encoded;
  try { encoded = JSON.stringify(value); } catch { return empty; }
  if (encoded === undefined) return empty;
  const byteLength = Buffer.byteLength(encoded);
  if (containsProhibitedAuditSecret(encoded)
    || /"(?:authorization|cookie|set-cookie|password|passwd|secret|api[-_]?key|access[-_]?token|refresh[-_]?token|database[-_]?url)"\s*:/i.test(encoded)) {
    return { ...empty, status: "redacted", byteLength };
  }
  const sha256 = createHash("sha256").update(encoded).digest("hex");
  if (byteLength > maximumBytes) return { ...empty, status: "partial", sha256, byteLength };
  return { version: 1, status: "complete", value: JSON.parse(encoded), sha256, byteLength };
}
