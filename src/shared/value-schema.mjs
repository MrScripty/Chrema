const allowedTypes = new Set(["null", "boolean", "number", "integer", "string", "array", "object"]);
const commonKeywords = new Set(["type", "enum", "const", "title", "description"]);
const keywordsByType = Object.freeze({
  null: new Set(),
  boolean: new Set(),
  number: new Set(["minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum"]),
  integer: new Set(["minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum"]),
  string: new Set(["minLength", "maxLength", "format"]),
  array: new Set(["items", "minItems", "maxItems", "uniqueItems"]),
  object: new Set(["properties", "required", "additionalProperties", "minProperties", "maxProperties"]),
});
const supportedFormats = new Set(["date", "date-time", "uri"]);
const allSupportedKeywords = new Set([
  ...commonKeywords,
  ...Object.values(keywordsByType).flatMap((keywords) => [...keywords]),
]);
const maximumSchemaDepth = 12;
const maximumSchemaNodes = 500;

export function validateValueSchema(schema, label = "valueSchema") {
  const state = { nodes: 0 };
  validateSchemaNode(schema, label, state, 0);
  return schema;
}

export function valueSchemaIssues(value, schema, path = "value") {
  validateValueSchema(schema);
  const issues = [];
  validateValue(value, schema, path, issues);
  return issues;
}

function validateSchemaNode(schema, label, state, depth) {
  if (!record(schema)) throw new TypeError(`${label} must be a JSON object.`);
  state.nodes += 1;
  if (depth > maximumSchemaDepth || state.nodes > maximumSchemaNodes) {
    throw new TypeError(`${label} exceeds the supported JSON Schema complexity limit.`);
  }
  for (const keyword of Object.keys(schema)) {
    if (!allSupportedKeywords.has(keyword)) {
      throw new TypeError(`${label} contains unsupported JSON Schema keyword ${keyword}.`);
    }
  }
  const type = schema.type;
  if (typeof type !== "string" || !allowedTypes.has(type)) {
    throw new TypeError(`${label}.type must be one supported JSON type.`);
  }
  const allowed = new Set([...commonKeywords, ...keywordsByType[type]]);
  for (const keyword of Object.keys(schema)) {
    if (!allowed.has(keyword)) throw new TypeError(`${label} contains unsupported JSON Schema keyword ${keyword}.`);
  }
  if (schema.title !== undefined) boundedText(schema.title, `${label}.title`, 200);
  if (schema.description !== undefined) boundedText(schema.description, `${label}.description`, 2_000);
  if (schema.enum !== undefined) {
    if (!Array.isArray(schema.enum) || schema.enum.length < 1 || schema.enum.length > 100) {
      throw new TypeError(`${label}.enum must contain between 1 and 100 values.`);
    }
    const identities = schema.enum.map(canonicalJson);
    if (new Set(identities).size !== identities.length) throw new TypeError(`${label}.enum values must be unique.`);
  }
  if (schema.const !== undefined) canonicalJson(schema.const);
  if (schema.enum !== undefined && schema.const !== undefined) {
    throw new TypeError(`${label} cannot combine enum and const.`);
  }
  if (type === "string") {
    optionalInteger(schema.minLength, `${label}.minLength`, 0, 1_000_000);
    optionalInteger(schema.maxLength, `${label}.maxLength`, 0, 1_000_000);
    if (schema.minLength !== undefined && schema.maxLength !== undefined && schema.minLength > schema.maxLength) {
      throw new TypeError(`${label}.minLength cannot exceed maxLength.`);
    }
    if (schema.format !== undefined && !supportedFormats.has(schema.format)) {
      throw new TypeError(`${label}.format is unsupported.`);
    }
  }
  if (type === "number" || type === "integer") validateNumericBounds(schema, label);
  if (type === "array") {
    if (schema.items === undefined) throw new TypeError(`${label}.items is required for array schemas.`);
    validateSchemaNode(schema.items, `${label}.items`, state, depth + 1);
    optionalInteger(schema.minItems, `${label}.minItems`, 0, 100_000);
    optionalInteger(schema.maxItems, `${label}.maxItems`, 0, 100_000);
    if (schema.minItems !== undefined && schema.maxItems !== undefined && schema.minItems > schema.maxItems) {
      throw new TypeError(`${label}.minItems cannot exceed maxItems.`);
    }
    if (schema.uniqueItems !== undefined && typeof schema.uniqueItems !== "boolean") {
      throw new TypeError(`${label}.uniqueItems must be boolean.`);
    }
  }
  if (type === "object") {
    if (schema.properties !== undefined && !record(schema.properties)) {
      throw new TypeError(`${label}.properties must be an object.`);
    }
    for (const [name, child] of Object.entries(schema.properties ?? {})) {
      if (!name || name.length > 200) throw new TypeError(`${label}.properties contains an invalid property name.`);
      validateSchemaNode(child, `${label}.properties.${name}`, state, depth + 1);
    }
    if (schema.required !== undefined) {
      if (!Array.isArray(schema.required) || schema.required.some((name) => typeof name !== "string" || !name)) {
        throw new TypeError(`${label}.required must be an array of property names.`);
      }
      if (new Set(schema.required).size !== schema.required.length) throw new TypeError(`${label}.required values must be unique.`);
      for (const name of schema.required) {
        if (!Object.hasOwn(schema.properties ?? {}, name)) {
          throw new TypeError(`${label}.required refers to undeclared property ${name}.`);
        }
      }
    }
    if (schema.additionalProperties !== undefined && typeof schema.additionalProperties !== "boolean") {
      validateSchemaNode(schema.additionalProperties, `${label}.additionalProperties`, state, depth + 1);
    }
    optionalInteger(schema.minProperties, `${label}.minProperties`, 0, 100_000);
    optionalInteger(schema.maxProperties, `${label}.maxProperties`, 0, 100_000);
    if (schema.minProperties !== undefined && schema.maxProperties !== undefined
      && schema.minProperties > schema.maxProperties) {
      throw new TypeError(`${label}.minProperties cannot exceed maxProperties.`);
    }
  }
}

function validateNumericBounds(schema, label) {
  for (const keyword of ["minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum"]) {
    if (schema[keyword] !== undefined && !Number.isFinite(schema[keyword])) {
      throw new TypeError(`${label}.${keyword} must be a finite number.`);
    }
  }
  const lower = schema.exclusiveMinimum ?? schema.minimum;
  const upper = schema.exclusiveMaximum ?? schema.maximum;
  if (lower !== undefined && upper !== undefined && lower > upper) {
    throw new TypeError(`${label} has contradictory numeric bounds.`);
  }
}

function validateValue(value, schema, path, issues) {
  if (!matchesType(value, schema.type)) {
    issues.push({ code: "VALUE_SCHEMA_TYPE_MISMATCH", path, expected: schema.type, actual: jsonType(value) });
    return;
  }
  if (schema.enum !== undefined && !schema.enum.some((candidate) => canonicalJson(candidate) === canonicalJson(value))) {
    issues.push({ code: "VALUE_SCHEMA_ENUM_MISMATCH", path, expected: schema.enum, actual: value });
  }
  if (schema.const !== undefined && canonicalJson(schema.const) !== canonicalJson(value)) {
    issues.push({ code: "VALUE_SCHEMA_CONST_MISMATCH", path, expected: schema.const, actual: value });
  }
  if (schema.type === "string") validateString(value, schema, path, issues);
  if (schema.type === "number" || schema.type === "integer") validateNumber(value, schema, path, issues);
  if (schema.type === "array") validateArray(value, schema, path, issues);
  if (schema.type === "object") validateObject(value, schema, path, issues);
}

function validateString(value, schema, path, issues) {
  if (schema.minLength !== undefined && [...value].length < schema.minLength) {
    issues.push({ code: "VALUE_SCHEMA_MIN_LENGTH", path, expected: schema.minLength, actual: [...value].length });
  }
  if (schema.maxLength !== undefined && [...value].length > schema.maxLength) {
    issues.push({ code: "VALUE_SCHEMA_MAX_LENGTH", path, expected: schema.maxLength, actual: [...value].length });
  }
  if (schema.format === "date" && !validDate(value)) {
    issues.push({ code: "VALUE_SCHEMA_FORMAT", path, expected: "date", actual: value });
  }
  if (schema.format === "date-time" && !validDateTime(value)) {
    issues.push({ code: "VALUE_SCHEMA_FORMAT", path, expected: "date-time", actual: value });
  }
  if (schema.format === "uri" && !validUri(value)) {
    issues.push({ code: "VALUE_SCHEMA_FORMAT", path, expected: "uri", actual: value });
  }
}

function validateNumber(value, schema, path, issues) {
  for (const [keyword, compare] of [
    ["minimum", (actual, expected) => actual >= expected],
    ["maximum", (actual, expected) => actual <= expected],
    ["exclusiveMinimum", (actual, expected) => actual > expected],
    ["exclusiveMaximum", (actual, expected) => actual < expected],
  ]) {
    if (schema[keyword] !== undefined && !compare(value, schema[keyword])) {
      issues.push({ code: `VALUE_SCHEMA_${keyword.replace(/[A-Z]/g, (letter) => `_${letter}`).toUpperCase()}`, path,
        expected: schema[keyword], actual: value });
    }
  }
}

function validateArray(value, schema, path, issues) {
  if (schema.minItems !== undefined && value.length < schema.minItems) {
    issues.push({ code: "VALUE_SCHEMA_MIN_ITEMS", path, expected: schema.minItems, actual: value.length });
  }
  if (schema.maxItems !== undefined && value.length > schema.maxItems) {
    issues.push({ code: "VALUE_SCHEMA_MAX_ITEMS", path, expected: schema.maxItems, actual: value.length });
  }
  if (schema.uniqueItems && new Set(value.map(canonicalJson)).size !== value.length) {
    issues.push({ code: "VALUE_SCHEMA_UNIQUE_ITEMS", path, expected: "unique items", actual: value });
  }
  value.forEach((item, index) => validateValue(item, schema.items, `${path}.${index}`, issues));
}

function validateObject(value, schema, path, issues) {
  const keys = Object.keys(value);
  if (schema.minProperties !== undefined && keys.length < schema.minProperties) {
    issues.push({ code: "VALUE_SCHEMA_MIN_PROPERTIES", path, expected: schema.minProperties, actual: keys.length });
  }
  if (schema.maxProperties !== undefined && keys.length > schema.maxProperties) {
    issues.push({ code: "VALUE_SCHEMA_MAX_PROPERTIES", path, expected: schema.maxProperties, actual: keys.length });
  }
  for (const name of schema.required ?? []) {
    if (!Object.hasOwn(value, name)) issues.push({ code: "VALUE_SCHEMA_REQUIRED", path: `${path}.${name}`, expected: "present", actual: "missing" });
  }
  for (const [name, child] of Object.entries(value)) {
    if (Object.hasOwn(schema.properties ?? {}, name)) {
      validateValue(child, schema.properties[name], `${path}.${name}`, issues);
    } else if (schema.additionalProperties === false) {
      issues.push({ code: "VALUE_SCHEMA_ADDITIONAL_PROPERTY", path: `${path}.${name}`, expected: "declared property", actual: name });
    } else if (record(schema.additionalProperties)) {
      validateValue(child, schema.additionalProperties, `${path}.${name}`, issues);
    }
  }
}

function matchesType(value, type) {
  if (type === "null") return value === null;
  if (type === "boolean") return typeof value === "boolean";
  if (type === "number") return typeof value === "number" && Number.isFinite(value);
  if (type === "integer") return Number.isInteger(value);
  if (type === "string") return typeof value === "string";
  if (type === "array") return Array.isArray(value);
  return record(value);
}

function jsonType(value) {
  if (value === null) return "null";
  if (Array.isArray(value)) return "array";
  if (typeof value === "number" && Number.isInteger(value)) return "integer";
  return typeof value;
}

function validDate(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(value)
    && Number.isFinite(Date.parse(`${value}T00:00:00.000Z`))
    && new Date(`${value}T00:00:00.000Z`).toISOString().slice(0, 10) === value;
}

function validDateTime(value) {
  return /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?Z$/.test(value)
    && Number.isFinite(Date.parse(value));
}

function validUri(value) {
  try {
    const url = new URL(value);
    return Boolean(url.protocol);
  } catch {
    return false;
  }
}

function optionalInteger(value, label, minimum, maximum) {
  if (value === undefined) return;
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw new TypeError(`${label} must be an integer from ${minimum} through ${maximum}.`);
  }
}

function boundedText(value, label, maximum) {
  if (typeof value !== "string" || !value.trim() || value.length > maximum) {
    throw new TypeError(`${label} must be non-empty text no longer than ${maximum} characters.`);
  }
}

function record(value) {
  return value != null && typeof value === "object" && !Array.isArray(value);
}

function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (record(value)) return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`).join(",")}}`;
  const encoded = JSON.stringify(value);
  if (encoded === undefined) throw new TypeError("JSON Schema values must be JSON serializable.");
  return encoded;
}
