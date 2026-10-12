import {createHash} from 'node:crypto';

const invocationFields = ['runId', 'attemptId', 'parentInvocationId', 'partId', 'toolId', 'bindingId', 'argumentsHash', 'intentHash'];
const descriptorFields = ['schemaVersion', 'mode', 'ownerId', 'contractId', 'contractVersion', 'lookupVersion', 'maximumResultBytes', 'retirement', 'replay'];
const outcomeFields = ['schemaVersion', 'descriptorHash', 'invocationId', 'invocationHash', 'kind', 'retired', 'retirementId', 'effectId', 'resultRef'];

function refuse() {
  return Object.assign(new Error('Caller tool outcome is unavailable.'), {code: 'CALLER_TOOL_OUTCOME_UNAVAILABLE', statusCode: 409});
}
function check(value) { if (!value) throw refuse(); }
function safe(work) { try { return work(); } catch { throw refuse(); } }
function freeze(value) {
  if (value && typeof value === 'object') {
    Object.values(value).forEach(freeze);
    Object.freeze(value);
  }
  return value;
}
function fields(value, expected) {
  check(value && Object.getPrototypeOf(value) === Object.prototype);
  const keys = Reflect.ownKeys(value);
  check(keys.length === expected.length && keys.every(key => typeof key === 'string' && expected.includes(key)));
  const descriptors = Object.getOwnPropertyDescriptors(value);
  const copy = {};
  for (const key of expected) {
    const entry = descriptors[key];
    check(entry && entry.enumerable && Object.hasOwn(entry, 'value'));
    copy[key] = entry.value;
  }
  return copy;
}
function id(value) {
  check(typeof value === 'string' && value.length > 0 && value.length <= 512
    && value.trim() === value && Buffer.from(value).toString('utf8') === value);
  return value;
}
function digest(value) { check(typeof value === 'string' && /^[a-f0-9]{64}$/.test(value)); return value; }
function version(value) { check(Number.isSafeInteger(value) && value >= 1 && value <= 1_000_000); return value; }

// Bounded canonical plain JSON hashing, shared by the host's descriptor,
// invocation and owner receipt. This does not authenticate any of them.
export function callerToolOutcomeHash(value) {
  return safe(() => {
    let nodes = 0;
    function canonical(item, depth = 0) {
      check(++nodes <= 4096 && depth <= 32);
      if (item === null || typeof item === 'boolean') return JSON.stringify(item);
      if (typeof item === 'number') { check(Number.isFinite(item)); return JSON.stringify(item); }
      if (typeof item === 'string') {
        check(Buffer.byteLength(item) <= 512_000 && Buffer.from(item).toString('utf8') === item);
        return JSON.stringify(item);
      }
      check(item && typeof item === 'object');
      if (Array.isArray(item)) {
        check(Object.getPrototypeOf(item) === Array.prototype && item.length <= 4096);
        const keys = Reflect.ownKeys(item);
        check(keys.length === item.length + 1 && keys.every(key => key === 'length'
          || typeof key === 'string' && /^(0|[1-9][0-9]*)$/.test(key) && Number(key) < item.length));
        const entries = Object.getOwnPropertyDescriptors(item);
        return '[' + Array.from({length: item.length}, (_, index) => {
          const entry = entries[index];
          check(entry && entry.enumerable && Object.hasOwn(entry, 'value'));
          return canonical(entry.value, depth + 1);
        }).join(',') + ']';
      }
      check(Object.getPrototypeOf(item) === Object.prototype);
      const keys = Reflect.ownKeys(item);
      check(keys.length <= 4096 && keys.every(key => typeof key === 'string'));
      const entries = Object.getOwnPropertyDescriptors(item);
      return '{' + keys.sort().map(key => {
        const entry = entries[key];
        check(entry.enumerable && Object.hasOwn(entry, 'value'));
        return JSON.stringify(key) + ':' + canonical(entry.value, depth + 1);
      }).join(',') + '}';
    }
    const text = canonical(value);
    check(Buffer.byteLength(text) <= 512_000);
    return createHash('sha256').update(text).digest('hex');
  });
}

function descriptor(value) {
  const result = fields(value, descriptorFields);
  check(result.schemaVersion === 1 && result.mode === 'caller-tool-outcome.v1'
    && result.retirement === 'owner-attested.v1' && result.replay === 'never.v1');
  id(result.ownerId); id(result.contractId); version(result.contractVersion); version(result.lookupVersion);
  check(Number.isSafeInteger(result.maximumResultBytes) && result.maximumResultBytes >= 1 && result.maximumResultBytes <= 128_000);
  return result;
}

export function callerToolOutcomeDescriptor(options) {
  return safe(() => freeze(descriptor(fields(options, ['policy']).policy)));
}

function invocation(value) {
  const result = fields(value, invocationFields);
  for (const key of ['runId', 'attemptId', 'parentInvocationId', 'partId', 'toolId']) id(result[key]);
  for (const key of ['bindingId', 'argumentsHash', 'intentHash']) digest(result[key]);
  const identityHash = callerToolOutcomeHash(result);
  return {...result, invocationId: 'caller-tool:' + identityHash, identityHash};
}

export function createCallerToolInvocationBinding(input) {
  return safe(() => freeze(invocation(input)));
}

function boundInvocation(value) {
  const supplied = fields(value, [...invocationFields, 'invocationId', 'identityHash']);
  const result = invocation(Object.fromEntries(invocationFields.map(key => [key, supplied[key]])));
  check(supplied.invocationId === result.invocationId && supplied.identityHash === result.identityHash);
  return result;
}

/** Pure structural validation only. The host must independently attest this
 * exact normalized snapshot and reauthorize disclosure on every adoption.
 * No outcome, including definitely-not-applied, authorizes invocation/replay.
 */
export function validateCallerToolOutcome(input, options) {
  return safe(() => {
    const settings = fields(options, ['descriptor', 'invocation']);
    const policy = descriptor(settings.descriptor), binding = boundInvocation(settings.invocation);
    const result = fields(input, outcomeFields);
    check(result.schemaVersion === 1 && result.descriptorHash === callerToolOutcomeHash(policy)
      && result.invocationId === binding.invocationId && result.invocationHash === binding.identityHash);
    check(typeof result.retired === 'boolean');
    if (result.retired) id(result.retirementId); else check(result.retirementId === null);
    if (result.kind === 'applied-with-exact-result') {
      check(result.retired); id(result.effectId);
      const ref = fields(result.resultRef, ['ownerId', 'contractId', 'referenceId', 'sha256', 'byteLength']);
      check(ref.ownerId === policy.ownerId && ref.contractId === policy.contractId);
      id(ref.referenceId); digest(ref.sha256);
      check(Number.isSafeInteger(ref.byteLength) && ref.byteLength >= 1 && ref.byteLength <= policy.maximumResultBytes);
      result.resultRef = ref;
    } else {
      check(result.kind === 'definitely-not-applied' || result.kind === 'unresolved');
      if (result.kind === 'definitely-not-applied') check(result.retired);
      check(result.effectId === null && result.resultRef === null);
    }
    return freeze(result);
  });
}
