import { createHash } from "node:crypto";
import { lookup as dnsLookup } from "node:dns/promises";
import { request as httpsRequest } from "node:https";
import { isIP } from "node:net";
import { checkServerIdentity } from "node:tls";
import { TextDecoder } from "node:util";
import { createGunzip } from "node:zlib";

const hardLimits = Object.freeze({
  maxUrlBytes: 8_192,
  maxRedirects: 3,
  maxHeaderBytes: 16_384,
  maxEncodedBodyBytes: 1_048_576,
  maxDecodedBodyBytes: 2_097_152,
  timeoutMs: 15_000,
});
const maxDnsAnswers = 32;
const maxHeaderCount = 128;
const redirectStatuses = new Set([301, 302, 303, 307, 308]);
const errorMessages = Object.freeze({
  "invalid-argument": "HTTPS fetch arguments are invalid.",
  "invalid-policy": "HTTPS fetch host policy is invalid.",
  "invalid-url": "HTTPS fetch URL is invalid.",
  "destination-denied": "HTTPS fetch destination is outside host policy.",
  "dns-failed": "HTTPS fetch DNS resolution failed.",
  "unsafe-address": "HTTPS fetch destination address is prohibited.",
  "headers-too-large": "HTTPS fetch response headers exceed the limit.",
  "headers-invalid": "HTTPS fetch response headers are invalid.",
  "http-status": "HTTPS fetch response status is not successful.",
  "unsupported-media-type": "HTTPS fetch response is not supported UTF-8 text.",
  "unsupported-encoding": "HTTPS fetch response encoding is unsupported.",
  "invalid-utf8": "HTTPS fetch response contains invalid UTF-8.",
  "body-too-large": "HTTPS fetch response body exceeds the limit.",
  "body-truncated": "HTTPS fetch response body is incomplete.",
  "invalid-compression": "HTTPS fetch gzip entity is invalid.",
  "redirect-invalid": "HTTPS fetch redirect is invalid.",
  "redirect-limit": "HTTPS fetch redirect limit was exceeded.",
  "redirect-loop": "HTTPS fetch redirect loop was detected.",
  "transport-failed": "HTTPS fetch transport failed.",
  "tls-failed": "HTTPS fetch TLS verification failed.",
  aborted: "HTTPS fetch was aborted.",
  timeout: "HTTPS fetch deadline was exceeded.",
});

/** Errors deliberately retain neither remote error causes, URLs, nor body text. */
export class HttpsFetchError extends Error {
  constructor(code) {
    const safeCode = Object.hasOwn(errorMessages, code) ? code : "transport-failed";
    super(errorMessages[safeCode]);
    this.name = "HttpsFetchError";
    this.code = safeCode;
  }
}

const fail = (code) => { throw new HttpsFetchError(code); };

/**
 * GET a complete HTTPS entity; bodyBytes/hash identify decompressed bytes.
 * Accept text/* and application JSON/XML/JavaScript (including +json/+xml),
 * with no declared charset or UTF-8. HTML is returned without parsing.
 * Content-Encoding must be identity or gzip; concatenated gzip members are
 * decoded together under the same decoded-byte limit.
 */
export async function fetchHttpsText(input, hostPolicy) {
  return productionFetcher(input, hostPolicy);
}

// Internal fixture seam. Standalone package exports must expose fetchHttpsText
// only: application callers must never supply a resolver, connector, or clock.
export function createHttpsTextFetcher({
  lookupAll = (hostname) => dnsLookup(hostname, { all: true, verbatim: true }),
  request = httpsRequest,
  now = Date.now,
} = {}) {
  return async function fetchText(input, hostPolicy = {}) {
    let context;
    try {
      knownObject(input, new Set(["url", "signal"]), "invalid-argument");
      const policy = normalizePolicy(hostPolicy);
      const requested = parseUrl(input.url, policy, "invalid-url");
      if (input.signal !== undefined && !(input.signal instanceof AbortSignal)) fail("invalid-argument");
      if (input.signal?.aborted) fail("aborted");
      context = createContext(input.signal, policy.timeoutMs, now);
      const operation = acquire(requested, policy, context, { lookupAll, request, now });
      return await Promise.race([operation, context.failure]);
    } catch (error) {
      throw context?.error ?? safeError(error);
    } finally {
      context?.finish();
    }
  };
}

const productionFetcher = createHttpsTextFetcher();

function knownObject(value, keys, code) {
  if (!value || typeof value !== "object" || Array.isArray(value)
    || ![Object.prototype, null].includes(Object.getPrototypeOf(value))
    || Reflect.ownKeys(value).some((key) => !keys.has(key))) fail(code);
}

function normalizePolicy(value) {
  knownObject(value, new Set([...Object.keys(hardLimits), "allowedHosts"]), "invalid-policy");
  const policy = { ...hardLimits };
  for (const [key, ceiling] of Object.entries(hardLimits)) {
    if (!Object.hasOwn(value, key)) continue;
    const limit = value[key];
    if (!Number.isSafeInteger(limit) || limit < (key === "maxRedirects" ? 0 : 1)
      || limit > ceiling) fail("invalid-policy");
    policy[key] = limit;
  }
  if (Object.hasOwn(value, "allowedHosts")) {
    if (!Array.isArray(value.allowedHosts) || value.allowedHosts.length > 1_024) fail("invalid-policy");
    policy.allowedHosts = new Set(value.allowedHosts.map((host) => {
      if (typeof host !== "string" || host.length > 253 || host !== host.toLowerCase()
        || !validHostname(host) || new URL(`https://${host}/`).hostname !== host) fail("invalid-policy");
      return host;
    }));
  }
  return policy;
}

function validHostname(host) {
  return !isIP(host) && host.length <= 253 && host.split(".").every((label) =>
    label.length >= 1 && label.length <= 63 && /^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/.test(label));
}

function parseUrl(value, policy, code, base) {
  if (typeof value !== "string" || !value || /[\s\u0000-\u001f\u007f\\]/u.test(value)
    || Buffer.byteLength(value, "utf8") > policy.maxUrlBytes) fail(code);
  if ((!base && !/^https:\/\/[^/]/i.test(value))
    || (/^[a-z][a-z0-9+.-]*:/i.test(value) && !/^https:\/\/[^/]/i.test(value))
    || value.startsWith("///")) fail(code);
  let url;
  try { url = base ? new URL(value, base) : new URL(value); } catch { fail(code); }
  if (url.protocol !== "https:" || (url.port && url.port !== "443")
    || url.username || url.password || !validHostname(url.hostname)) fail(code);
  // A syntactically empty userinfo delimiter must also be rejected.
  if (/^(?:https:)?\/\/[^/?#]*@/i.test(value)) fail(code);
  url.hash = "";
  if (Buffer.byteLength(url.href, "utf8") > policy.maxUrlBytes) fail(code);
  if (policy.allowedHosts && !policy.allowedHosts.has(url.hostname)) fail("destination-denied");
  return url;
}

function createContext(signal, timeoutMs, now) {
  const resources = new Set();
  const startedAt = now();
  let rejectFailure;
  const context = {
    error: null,
    finished: false,
    failure: new Promise((_, reject) => { rejectFailure = reject; }),
    active() {
      if (context.error) throw context.error;
      if (context.finished) fail("transport-failed");
      if (now() - startedAt >= timeoutMs) context.stop(new HttpsFetchError("timeout"));
      if (context.error) throw context.error;
    },
    track(resource) {
      resources.add(resource);
      if (context.error || context.finished) resource.destroy();
      context.active();
      return resource;
    },
    release(resource) { resources.delete(resource); },
    stop(error) {
      if (context.error || context.finished) return;
      context.error = safeError(error);
      for (const resource of resources) resource.destroy();
      rejectFailure(context.error);
    },
    finish() {
      context.finished = true;
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
      for (const resource of resources) resource.destroy();
      resources.clear();
    },
  };
  const onAbort = () => context.stop(new HttpsFetchError("aborted"));
  const timer = setTimeout(() => context.stop(new HttpsFetchError("timeout")), timeoutMs);
  signal?.addEventListener("abort", onAbort, { once: true });
  if (signal?.aborted) onAbort();
  return context;
}

async function acquire(requested, policy, context, dependencies) {
  let current = requested;
  const redirects = [];
  const visited = new Set([current.href]);
  for (;;) {
    context.active();
    let answers;
    try { answers = await dependencies.lookupAll(current.hostname); }
    catch { context.active(); fail("dns-failed"); }
    context.active();
    if (!Array.isArray(answers) || !answers.length || answers.length > maxDnsAnswers) fail("dns-failed");
    // Reject the complete answer set if even one answer is unsafe. Never choose
    // a public answer from a mixed public/private set or retry another answer.
    for (const answer of answers) {
      if (!answer || typeof answer.address !== "string" || ![4, 6].includes(answer.family) || isIP(answer.address) !== answer.family
        || !publicAddress(answer.address, answer.family)) fail("unsafe-address");
    }
    const pinned = { address: answers[0].address, family: answers[0].family };
    const response = await requestEntity(current, pinned, policy, context, dependencies.request);
    context.active();
    if (response.redirect) {
      if (redirects.length >= policy.maxRedirects) fail("redirect-limit");
      const next = parseUrl(response.location, policy, "redirect-invalid", current);
      if (visited.has(next.href)) fail("redirect-loop");
      redirects.push({ fromUrl: current.href, toUrl: next.href, status: response.status });
      visited.add(next.href);
      current = next;
      continue;
    }
    const bodyBytes = response.contentEncoding === "gzip"
      ? await decodeGzip(response.encodedBytes, policy, context) : response.encodedBytes;
    if (bodyBytes.length > policy.maxDecodedBodyBytes) fail("body-too-large");
    let text;
    try { text = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(bodyBytes); }
    catch { fail("invalid-utf8"); }
    context.active();
    const bodySha256 = createHash("sha256").update(bodyBytes).digest("hex");
    const retrievedAt = new Date(dependencies.now()).toISOString();
    context.active();
    return {
      requestedUrl: requested.href,
      finalUrl: current.href,
      redirects,
      retrievedAt,
      status: response.status,
      mediaType: response.mediaType,
      contentEncoding: response.contentEncoding,
      bodyBytes,
      bodySha256,
      text,
      encodedByteLength: response.encodedBytes.length,
    };
  }
}

function requestEntity(url, pinned, policy, context, request) {
  return new Promise((resolve, reject) => {
    let settled = false;
    let verifiedSocket = false;
    let req;
    const rejectOnce = (error) => {
      if (settled) return;
      settled = true;
      reject(safeError(error));
      req?.destroy();
    };
    const resolveOnce = (value) => {
      if (settled) return;
      settled = true;
      resolve(value);
    };
    const lookup = (hostname, options, callback) => {
      if (typeof options === "function") { callback = options; options = {}; }
      if (hostname !== url.hostname || (options.family && options.family !== pinned.family)) {
        callback(new HttpsFetchError("unsafe-address"));
        return;
      }
      if (options.all) callback(null, [{ ...pinned }]);
      else callback(null, pinned.address, pinned.family);
    };
    const options = {
      protocol: "https:", hostname: url.hostname, port: 443,
      path: `${url.pathname}${url.search}`, method: "GET",
      servername: url.hostname, rejectUnauthorized: true,
      checkServerIdentity: (_hostname, certificate) => checkServerIdentity(url.hostname, certificate),
      agent: false, family: pinned.family, autoSelectFamily: false,
      lookup, maxHeaderSize: policy.maxHeaderBytes, insecureHTTPParser: false,
      headers: { Host: url.hostname, Accept: "text/*, application/json, application/xml", "Accept-Encoding": "gzip, identity", Connection: "close" },
    };
    try {
      req = request(options, (res) => {
        // Install before validation/destruction: rejected responses can emit
        // asynchronous stream errors even when their bodies are never read.
        res.on("error", () => rejectOnce(new HttpsFetchError("body-truncated")));
        try {
          context.track(res);
          context.active();
          if (!verifiedSocket) fail("tls-failed");
          const headers = responseHeaders(res, policy);
          const status = res.statusCode;
          if (!Number.isInteger(status) || status < 100 || status > 599) fail("headers-invalid");
          if (redirectStatuses.has(status)) {
            if (!headers.location) fail("redirect-invalid");
            resolveOnce({ redirect: true, location: headers.location, status });
            res.destroy();
            req.destroy();
            return;
          }
          if (status < 200 || status >= 300) fail("http-status");
          const mediaType = textMediaType(headers["content-type"]);
          const contentEncoding = (headers["content-encoding"] ?? "identity").toLowerCase();
          if (!["identity", "gzip"].includes(contentEncoding)) fail("unsupported-encoding");
          let contentLength = null;
          if (headers["content-length"] !== undefined) {
            if (!/^\d+$/.test(headers["content-length"])) fail("headers-invalid");
            contentLength = Number(headers["content-length"]);
            if (!Number.isSafeInteger(contentLength) || contentLength > policy.maxEncodedBodyBytes) fail("body-too-large");
          }
          const encodedBuffer = Buffer.alloc(policy.maxEncodedBodyBytes);
          let encodedLength = 0;
          let ended = false;
          res.on("data", (chunk) => {
            try {
              context.active();
              if (!Buffer.isBuffer(chunk)) fail("transport-failed");
              encodedLength += chunk.length;
              if (encodedLength > policy.maxEncodedBodyBytes
                || (contentEncoding === "identity" && encodedLength > policy.maxDecodedBodyBytes)) fail("body-too-large");
              chunk.copy(encodedBuffer, encodedLength - chunk.length);
            } catch (error) { res.destroy(); rejectOnce(error); }
          });
          res.once("end", () => {
            ended = true;
            try {
              context.active();
              if (!res.complete || (contentLength !== null && encodedLength !== contentLength)) fail("body-truncated");
              validateTrailers(res, policy);
              context.release(res);
              resolveOnce({ status, mediaType, contentEncoding, encodedBytes: encodedBuffer.subarray(0, encodedLength) });
            } catch (error) { rejectOnce(error); }
          });
          res.once("aborted", () => rejectOnce(new HttpsFetchError("body-truncated")));
          res.once("close", () => { if (!ended) rejectOnce(new HttpsFetchError("body-truncated")); });
        } catch (error) {
          res.destroy();
          rejectOnce(error);
        }
      });
      req.maxHeadersCount = maxHeaderCount + 1;
      req.on("error", (error) => {
        if (error?.code === "HPE_HEADER_OVERFLOW") rejectOnce(new HttpsFetchError("headers-too-large"));
        else if (error instanceof HttpsFetchError) rejectOnce(error);
        else if (/^(?:ERR_TLS_|ERR_SSL_|CERT_|DEPTH_|SELF_SIGNED_|UNABLE_TO_)/.test(error?.code ?? "")) {
          rejectOnce(new HttpsFetchError("tls-failed"));
        } else rejectOnce(new HttpsFetchError("transport-failed"));
      });
      req.on("socket", (socket) => {
        socket.once("secureConnect", () => {
          try {
            context.active();
            if (socket.authorized !== true) fail("tls-failed");
            if (socket.remotePort !== 443 || !sameAddress(socket.remoteAddress, pinned)) fail("unsafe-address");
            verifiedSocket = true;
          } catch (error) { socket.destroy(); rejectOnce(error); }
        });
      });
      req.on("information", () => rejectOnce(new HttpsFetchError("headers-invalid")));
      req.on("upgrade", (_res, socket) => { socket.destroy(); rejectOnce(new HttpsFetchError("http-status")); });
      context.track(req);
      req.end();
    } catch (error) { rejectOnce(error); }
  });
}

function responseHeaders(res, policy) {
  if (!Array.isArray(res.rawHeaders) || res.rawHeaders.length % 2) fail("headers-invalid");
  if (res.rawHeaders.length > maxHeaderCount * 2) fail("headers-too-large");
  const headers = Object.create(null);
  let byteLength = 32; // Conservative allowance for the HTTP status line.
  const uniqueHeaders = new Set(["location", "content-type", "content-encoding", "content-length", "transfer-encoding"]);
  for (let index = 0; index < res.rawHeaders.length; index += 2) {
    const name = res.rawHeaders[index];
    const value = res.rawHeaders[index + 1];
    if (typeof name !== "string" || typeof value !== "string"
      || !/^[!#$%&'*+.^_`|~0-9a-z-]+$/i.test(name) || /[\r\n\u0000]/.test(value)) fail("headers-invalid");
    byteLength += Buffer.byteLength(name, "latin1") + Buffer.byteLength(value, "latin1") + 4;
    if (byteLength > policy.maxHeaderBytes) fail("headers-too-large");
    const key = name.toLowerCase();
    if (uniqueHeaders.has(key)) {
      if (Object.hasOwn(headers, key)) fail("headers-invalid");
      headers[key] = value.trim();
    }
  }
  if (headers["content-length"] !== undefined && headers["transfer-encoding"] !== undefined) fail("headers-invalid");
  if (headers["transfer-encoding"] !== undefined && headers["transfer-encoding"].toLowerCase() !== "chunked") fail("unsupported-encoding");
  return headers;
}

function validateTrailers(res, policy) {
  const trailers = res.rawTrailers ?? [];
  if (!Array.isArray(trailers) || trailers.length % 2) fail("headers-invalid");
  const forbidden = new Set(["location", "content-type", "content-encoding", "content-length", "transfer-encoding"]);
  for (let index = 0; index < trailers.length; index += 2) {
    if (typeof trailers[index] !== "string" || forbidden.has(trailers[index].toLowerCase())) fail("headers-invalid");
  }
  // One header budget covers the initial fields and fields supplied after the
  // body. Trailer fields cannot reinterpret the entity captured above.
  responseHeaders({ rawHeaders: [...res.rawHeaders, ...trailers] }, policy);
}

function textMediaType(header) {
  if (typeof header !== "string") fail("unsupported-media-type");
  const parts = header.split(";");
  const mediaType = parts.shift().trim().toLowerCase();
  if (!/^[a-z0-9!#$&^_.+-]+\/[a-z0-9!#$&^_.+-]+$/.test(mediaType)
    || !(mediaType.startsWith("text/") || /^application\/(?:json|xml|javascript|ecmascript|[a-z0-9!#$&^_.+-]+\+(?:json|xml))$/.test(mediaType))) fail("unsupported-media-type");
  let charsetSeen = false;
  for (const part of parts) {
    const parameter = /^\s*([a-z0-9!#$&^_.+-]+)\s*=\s*(?:"([^"\\]*)"|([a-z0-9!#$&^_.+-]+))\s*$/i.exec(part);
    if (!parameter) fail("unsupported-media-type");
    if (parameter[1].toLowerCase() === "charset") {
      if (charsetSeen || !/^(?:utf-8|utf8)$/i.test(parameter[2] ?? parameter[3])) fail("unsupported-media-type");
      charsetSeen = true;
    }
  }
  return mediaType;
}

async function decodeGzip(encoded, policy, context) {
  const inflater = createGunzip();
  const body = await new Promise((resolve, reject) => {
    const decodedBuffer = Buffer.alloc(policy.maxDecodedBodyBytes);
    let size = 0;
    let ended = false;
    inflater.on("error", () => reject(new HttpsFetchError("invalid-compression")));
    inflater.on("data", (chunk) => {
      try {
        context.active();
        size += chunk.length;
        if (size > policy.maxDecodedBodyBytes) fail("body-too-large");
        chunk.copy(decodedBuffer, size - chunk.length);
      } catch (error) { reject(safeError(error)); inflater.destroy(); }
    });
    inflater.once("end", () => { ended = true; resolve(decodedBuffer.subarray(0, size)); });
    inflater.once("close", () => { if (!ended) reject(context.error ?? new HttpsFetchError("invalid-compression")); });
    context.track(inflater);
    inflater.end(encoded);
  });
  context.release(inflater);
  context.active();
  return body;
}

// Conservative exclusions from the IANA IPv4/IPv6 Special-Purpose Address
// Registries, including globally reachable special-purpose assignments. IPv6
// additionally requires the global-unicast range (2000::/3), excluding
// mapped/compatible, NAT64, local, multicast and space outside that range.
// Registry sources (review when updating this fixed transport policy):
// https://www.iana.org/assignments/iana-ipv4-special-registry/
// https://www.iana.org/assignments/iana-ipv6-special-registry/
const deniedV4 = [
  ["0.0.0.0", 8], ["10.0.0.0", 8], ["100.64.0.0", 10], ["127.0.0.0", 8],
  ["169.254.0.0", 16], ["172.16.0.0", 12], ["192.0.0.0", 24], ["192.0.2.0", 24],
  ["192.31.196.0", 24], ["192.52.193.0", 24], ["192.88.99.0", 24], ["192.168.0.0", 16],
  ["192.175.48.0", 24], ["198.18.0.0", 15], ["198.51.100.0", 24], ["203.0.113.0", 24],
  ["224.0.0.0", 4], ["240.0.0.0", 4],
].map(([address, bits]) => [ipv4Value(address), bits]);
const deniedV6 = [
  ["2001::", 23], // IETF protocol assignments: includes Teredo, ORCHID and benchmarking.
  ["2001:db8::", 32], ["2002::", 16], // Documentation and 6to4.
  ["2620:4f:8000::", 48], ["3fff::", 20], // AS112 and documentation.
].map(([address, bits]) => [ipv6Value(address), bits]);

function publicAddress(address, family) {
  if (family === 4) {
    const value = ipv4Value(address);
    return !deniedV4.some(([network, bits]) => Math.floor(value / 2 ** (32 - bits)) === Math.floor(network / 2 ** (32 - bits)));
  }
  const value = ipv6Value(address);
  return value !== null && value >> 125n === 1n
    && !deniedV6.some(([network, bits]) => value >> BigInt(128 - bits) === network >> BigInt(128 - bits));
}
function ipv4Value(address) {
  return address.split(".").reduce((value, part) => value * 256 + Number(part), 0);
}
function ipv6Value(address) {
  if (typeof address !== "string" || address.includes("%") || address.includes(".") || isIP(address) !== 6) return null;
  const halves = address.split("::");
  const left = halves[0] ? halves[0].split(":") : [];
  const right = halves.length === 2 && halves[1] ? halves[1].split(":") : [];
  const words = halves.length === 2 ? [...left, ...Array(8 - left.length - right.length).fill("0"), ...right] : left;
  return words.reduce((value, word) => (value << 16n) | BigInt(`0x${word}`), 0n);
}
function sameAddress(remote, pinned) {
  if (typeof remote !== "string") return false;
  if (pinned.family === 6) return isIP(remote) === 6 && ipv6Value(remote) === ipv6Value(pinned.address);
  if (isIP(remote) === 4) return remote === pinned.address;
  const mapped = /^::ffff:(\d+\.\d+\.\d+\.\d+)$/i.exec(remote);
  if (mapped) return mapped[1] === pinned.address;
  return isIP(remote) === 6 && ipv6Value(remote) === ((0xffffn << 32n) | BigInt(ipv4Value(pinned.address)));
}
function safeError(error) {
  // Reconstruct even typed errors so mutated messages, causes and arbitrary
  // properties from a dependency never cross the transport boundary.
  return new HttpsFetchError(error instanceof HttpsFetchError ? error.code : "transport-failed");
}
