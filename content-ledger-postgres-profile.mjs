import { validateValueSchema } from "./value-schema.mjs";

export const CONTENT_LEDGER_POSTGRES_SCHEMA_PROFILE = "capture-v1";

export function assertContentLedgerSchemaProfile(schemaProfile) {
  if (schemaProfile !== undefined && schemaProfile !== CONTENT_LEDGER_POSTGRES_SCHEMA_PROFILE) {
    throw new TypeError(`Unsupported content ledger schema profile: ${schemaProfile}.`);
  }
}

/** The clean PostgreSQL schema implements capture-v1, which excludes format. */
export function validateContentLedgerProfileSchema(schema, schemaProfile, label = "Content extension schema") {
  assertContentLedgerSchemaProfile(schemaProfile);
  if (schemaProfile === undefined) return validateValueSchema(schema, label);
  assertProfileJsonValue(schema, label);
  validateValueSchema(schema, label);
  rejectFormats(schema, label);
  return schema;
}

export function assertProfileJsonValue(value, label = "Content value", seen = new WeakSet()) {
  if (value === null || typeof value === "boolean") return;
  if (typeof value === "string") {
    assertPostgresString(value, label);
    return;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new TypeError(`${label} must contain finite JSON numbers.`);
    return;
  }
  if (typeof value !== "object" || seen.has(value)
    || (!Array.isArray(value) && Object.getPrototypeOf(value) !== Object.prototype
      && Object.getPrototypeOf(value) !== null)) {
    throw new TypeError(`${label} must contain only acyclic JSON values.`);
  }
  seen.add(value);
  if (Array.isArray(value)) {
    for (const item of value) assertProfileJsonValue(item, label, seen);
  } else {
    for (const [key, item] of Object.entries(value)) {
      assertPostgresString(key, label);
      assertProfileJsonValue(item, label, seen);
    }
  }
  seen.delete(value);
}

function assertPostgresString(value, label) {
  for (let index = 0; index < value.length; index += 1) {
    const unit = value.charCodeAt(index);
    if (unit === 0) throw new TypeError(`${label} contains a PostgreSQL-incompatible null character.`);
    if (unit >= 0xd800 && unit <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (!(next >= 0xdc00 && next <= 0xdfff)) {
        throw new TypeError(`${label} contains an unpaired Unicode surrogate.`);
      }
      index += 1;
    } else if (unit >= 0xdc00 && unit <= 0xdfff) {
      throw new TypeError(`${label} contains an unpaired Unicode surrogate.`);
    }
  }
}

function rejectFormats(schema, label) {
  if (Object.hasOwn(schema, "format")) throw new TypeError(`${label}.format is unsupported by capture-v1.`);
  if (schema.type === "array") rejectFormats(schema.items, `${label}.items`);
  if (schema.type === "object") {
    for (const [key, child] of Object.entries(schema.properties ?? {})) {
      rejectFormats(child, `${label}.properties.${key}`);
    }
    if (schema.additionalProperties && typeof schema.additionalProperties === "object") {
      rejectFormats(schema.additionalProperties, `${label}.additionalProperties`);
    }
  }
}
