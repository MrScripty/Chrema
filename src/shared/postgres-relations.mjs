const identifierPattern = /^[A-Za-z_][A-Za-z0-9_]*$/;

/** Resolve an exact, caller-supplied set of PostgreSQL schema and table names. */
export function resolvePostgresRelations(relations, requiredKeys, label, options = {}) {
  if (!Array.isArray(requiredKeys) || !requiredKeys.length
    || new Set(requiredKeys).size !== requiredKeys.length
    || requiredKeys.some((key) => typeof key !== "string" || !key)) {
    throw new TypeError("PostgreSQL relation keys are required.");
  }
  const missingRelations = options?.missingRelations ?? requiredKeys.join(", ");
  if (typeof missingRelations !== "string") {
    throw new TypeError("PostgreSQL relation requirement description must be text.");
  }
  if (!relations || typeof relations !== "object" || Array.isArray(relations)
    || Object.keys(relations).length !== requiredKeys.length
    || requiredKeys.some((key) => !Object.hasOwn(relations, key))) {
    throw new TypeError(`${label} relations must provide ${missingRelations}.`);
  }
  return Object.freeze(Object.fromEntries(requiredKeys.map((key) => {
    const relation = relations[key];
    if (!relation || typeof relation !== "object" || Array.isArray(relation)
      || Object.keys(relation).length !== 2
      || !Object.hasOwn(relation, "schema") || !Object.hasOwn(relation, "name")) {
      throw new TypeError(`${label} ${key} relation must provide schema and name.`);
    }
    return [key, `${quotePostgresIdentifier(relation.schema, `${label} ${key} schema`)}.${quotePostgresIdentifier(relation.name, `${label} ${key} name`)}`];
  })));
}

export function quotePostgresIdentifier(value, label) {
  if (typeof value !== "string" || !identifierPattern.test(value) || Buffer.byteLength(value, "utf8") > 63) {
    throw new TypeError(`${label} is not a valid PostgreSQL identifier.`);
  }
  return `"${value}"`;
}
