import { createHash } from 'node:crypto';
import { validateValueSchema } from '../shared/value-schema.mjs';

const schemaDialect = 'value-schema.v1';

/**
 * Snapshot a trusted host's implementation/adapter declaration, not executable
 * code or authority. Digests must describe the host's reviewed loaded bindings;
 * this pure contract cannot attest that a handler actually matches them.
 */
export function createToolRegistration(input) {
  exact(input, ['schemaVersion', 'toolId', 'implementation', 'adapter', 'schemaDialect', 'inputSchema', 'resultSchema'],
    'Tool registration', ['bindingId']);
  version(input.schemaVersion, 'Tool registration');
  if (input.schemaDialect !== schemaDialect) throw new TypeError('Unsupported tool schema dialect.');
  const registration = {
    schemaVersion: 1,
    toolId: identity(input.toolId, 'Tool id'),
    implementation: implementationBinding(input.implementation, 'Tool implementation'),
    adapter: implementationBinding(input.adapter, 'Tool adapter'),
    schemaDialect,
    inputSchema: jsonSnapshot(input.inputSchema),
    resultSchema: jsonSnapshot(input.resultSchema),
  };
  validateValueSchema(registration.inputSchema, 'Tool input schema');
  validateValueSchema(registration.resultSchema, 'Tool result schema');
  registration.bindingId = hash(bindingRecord(registration));
  if (Object.hasOwn(input, 'bindingId') && input.bindingId !== registration.bindingId) {
    throw new TypeError('Tool registration binding identity mismatch.');
  }
  return freeze(registration);
}

/**
 * Resolve every host-validated executable part, including empty assignments.
 * References are { toolId, bindingId }; hostTools is an explicit host ceiling.
 * Part identities/topology and definitionHash are host-owned, never inferred.
 * No parent/sibling inheritance, model delegation, provider or handler execution.
 */
export function resolveToolAssignments(input) {
  exact(input, ['schemaVersion', 'definitionId', 'definitionHash', 'partIds', 'assignments', 'registrations', 'hostTools'],
    'Tool assignments');
  version(input.schemaVersion, 'Tool assignments');
  const definitionId = identity(input.definitionId, 'Definition id');
  const definitionHash = digest(input.definitionHash, 'Definition hash');
  const partIds = unique(array(input.partIds, 'Part ids').map(id => identity(id, 'Part id')), 'Part ids');
  if (partIds.length === 0) throw new TypeError('At least one executable part is required.');
  const registrations = new Map();
  for (const raw of array(input.registrations, 'Tool registrations')) {
    const registration = createToolRegistration(raw);
    if (registrations.has(registration.toolId)) throw new TypeError('Duplicate tool registration.');
    registrations.set(registration.toolId, registration);
  }
  function references(values, label) {
    const seen = new Set();
    return array(values, label).map(ref => {
      exact(ref, ['toolId', 'bindingId'], 'Tool reference');
      const toolId = identity(ref.toolId, 'Referenced tool id');
      const bindingId = digest(ref.bindingId, 'Referenced binding id');
      if (seen.has(toolId)) throw new TypeError('Duplicate tool reference.');
      seen.add(toolId);
      const registration = registrations.get(toolId);
      if (!registration) throw new TypeError('Unknown tool registration.');
      if (registration.bindingId !== bindingId) throw new TypeError('Tool reference binding identity mismatch.');
      return registration;
    });
  }
  const hostTools = new Map(references(input.hostTools, 'Host tools').map(tool => [tool.toolId, tool.bindingId]));
  const byPart = new Map();
  for (const assignment of array(input.assignments, 'Part assignments')) {
    exact(assignment, ['partId', 'tools'], 'Part assignment');
    const partId = identity(assignment.partId, 'Assigned part id');
    if (!partIds.includes(partId)) throw new TypeError('Unknown assignment part.');
    if (byPart.has(partId)) throw new TypeError('Duplicate part assignment.');
    const tools = references(assignment.tools, 'Assigned tools');
    if (tools.some(tool => hostTools.get(tool.toolId) !== tool.bindingId)) {
      throw new TypeError('Assigned tool is outside the host ceiling.');
    }
    byPart.set(partId, tools.map(tool => ({ ...bindingRecord(tool), bindingId: tool.bindingId }))
      .sort((left, right) => compare(left.toolId, right.toolId)));
  }
  if (byPart.size !== partIds.length) throw new TypeError('Every executable part requires an explicit assignment.');
  const record = {
    schemaVersion: 1,
    definitionId,
    definitionHash,
    parts: [...partIds].sort(compare).map(partId => ({ partId, tools: byPart.get(partId) })),
  };
  return freeze({ ...record, assignmentHash: hash(record) });
}

function bindingRecord(registration) {
  return {
    schemaVersion: registration.schemaVersion,
    toolId: registration.toolId,
    implementation: registration.implementation,
    adapter: registration.adapter,
    schemaDialect: registration.schemaDialect,
    inputSchemaHash: hash(registration.inputSchema),
    resultSchemaHash: hash(registration.resultSchema),
  };
}

function implementationBinding(input, label) {
  exact(input, ['id', 'version', 'digest'], label);
  return { id: identity(input.id, `${label} id`), version: identity(input.version, `${label} version`),
    digest: digest(input.digest, `${label} digest`) };
}

function version(value, label) {
  if (value !== 1) throw new TypeError(`${label} schemaVersion must equal 1.`);
}

function identity(value, label) {
  if (typeof value !== 'string' || !value.trim() || value !== value.trim() || value.length > 512) {
    throw new TypeError(`${label} must be non-empty, unpadded text of at most 512 characters.`);
  }
  return value;
}

function digest(value, label) {
  if (typeof value !== 'string' || !/^[0-9a-f]{64}$/.test(value)) throw new TypeError(`${label} must be a lowercase SHA-256.`);
  return value;
}

function array(value, label) {
  if (!Array.isArray(value) || value.length > 1_000 || !denseArray(value)) {
    throw new TypeError(`${label} must be a dense array of at most 1000 entries.`);
  }
  return value;
}

function denseArray(value) {
  if (!Array.isArray(value) || Object.getPrototypeOf(value) !== Array.prototype
    || Reflect.ownKeys(value).length !== value.length + 1) return false;
  for (let index = 0; index < value.length; index += 1) {
    const property = Object.getOwnPropertyDescriptor(value, index);
    if (!property?.enumerable || !Object.hasOwn(property, 'value')) return false;
  }
  return true;
}

function unique(values, label) {
  if (new Set(values).size !== values.length) throw new TypeError(`${label} must be unique.`);
  return values;
}

function exact(value, required, label, optional = []) {
  if (!value || Object.getPrototypeOf(value) !== Object.prototype
    || Reflect.ownKeys(value).some(key => typeof key !== 'string' || ![...required, ...optional].includes(key)
      || !Object.getOwnPropertyDescriptor(value, key)?.enumerable
      || !Object.hasOwn(Object.getOwnPropertyDescriptor(value, key), 'value'))
    || required.some(key => !Object.hasOwn(value, key))) {
    throw new TypeError(`${label} contains missing or unknown fields or is not a plain data object.`);
  }
}

// Avoid lossy JSON conversion (undefined, nonfinite numbers, custom objects,
// sparse arrays) before schema validation. These are contract resource bounds.
function jsonSnapshot(input, state = { nodes: 0 }, depth = 0) {
  state.nodes += 1;
  if (depth > 64 || state.nodes > 10_000) throw new TypeError('Tool schema exceeds JSON snapshot limits.');
  if (input === null || typeof input === 'string' || typeof input === 'boolean'
    || (typeof input === 'number' && Number.isFinite(input))) return input;
  if (Array.isArray(input)) {
    if (input.length > 10_000) throw new TypeError('Tool schema exceeds JSON snapshot limits.');
    if (!denseArray(input)) throw new TypeError('Tool schema requires dense JSON arrays.');
    return input.map(value => jsonSnapshot(value, state, depth + 1));
  }
  if (input && Object.getPrototypeOf(input) === Object.prototype) {
    const entries = Reflect.ownKeys(input).map(key => {
      const property = Object.getOwnPropertyDescriptor(input, key);
      if (typeof key !== 'string' || !property.enumerable || !Object.hasOwn(property, 'value')) {
        throw new TypeError('Tool schema requires plain JSON data.');
      }
      return [key, jsonSnapshot(property.value, state, depth + 1)];
    });
    return Object.fromEntries(entries);
  }
  throw new TypeError('Tool schema requires plain JSON data.');
}

function compare(left, right) { return left < right ? -1 : left > right ? 1 : 0; }
function hash(value) { return createHash('sha256').update(stableJson(value)).digest('hex'); }
function stableJson(value) {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(',')}]`;
  if (value && typeof value === 'object') {
    return `{${Object.keys(value).sort(compare).map(key => `${JSON.stringify(key)}:${stableJson(value[key])}`).join(',')}}`;
  }
  return JSON.stringify(value);
}
function freeze(value) {
  if (value && typeof value === 'object') {
    for (const child of Object.values(value)) freeze(child);
    Object.freeze(value);
  }
  return value;
}
