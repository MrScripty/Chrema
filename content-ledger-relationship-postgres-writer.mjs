import { assertNormalizedContentRelationship } from "./content-ledger-relationship-contract.mjs";
import { assertContentLedgerSchemaProfile } from "./content-ledger-postgres-profile.mjs";
import { resolvePostgresRelations } from "./postgres-relations.mjs";

const MAX_RELATIONSHIPS_PER_BATCH = 1000;

/**
 * Bind relationship writes to one explicit table. Each write issues multiple
 * statements; callers own the transaction and supply its client for atomicity.
 */
export function createPostgresContentRelationshipWriter({ relations, schemaProfile } = {}) {
  assertContentLedgerSchemaProfile(schemaProfile);
  const resolved = resolvePostgresRelations(relations, ["relationships"], "Content relationship", {
    missingRelations: "relationships",
  });
  return Object.freeze({
    insertContentRelationship: (client, value) => insertContentRelationship(client, value, resolved, schemaProfile),
    insertContentRelationships: (client, values) => insertContentRelationships(client, values, resolved, schemaProfile),
  });
}

async function insertContentRelationships(client, values, relations, schemaProfile) {
  if (!Array.isArray(values)) throw new TypeError("Content relationships must be an array.");
  if (values.length === 0) return [];
  // Snapshot this call's data before the first await; callers retain their input objects.
  const snapshots = values.map(value => structuredClone(value));
  const unique = new Map();
  for (const value of snapshots) {
    assertNormalizedContentRelationship(value, { schemaProfile });
    const previous = unique.get(value.relationshipId);
    if (previous && previous.identityHash !== value.identityHash) {
      throw relationshipConflict(value.relationshipId);
    }
    if (!previous) unique.set(value.relationshipId, value);
  }

  const distinct = [...unique.values()];
  for (let offset = 0; offset < distinct.length; offset += MAX_RELATIONSHIPS_PER_BATCH) {
    const chunk = distinct.slice(offset, offset + MAX_RELATIONSHIPS_PER_BATCH);
    const parameters = [];
    const placeholders = chunk.map((value, index) => {
      const start = index * 13;
      parameters.push(value.relationshipId, value.applicationNamespace, value.relationshipType,
        value.relationshipSchemaVersion, value.fromVersionId, value.toVersionId,
        value.sourceReference.kind, value.sourceReference.id, value.extensionDefinitionId,
        value.extensionDefinitionVersion, JSON.stringify(value.extensions), value.identityHash,
        value.recordedAt);
      return `(${Array.from({ length: 13 }, (_, column) =>
        `$${start + column + 1}${column === 10 ? "::jsonb" : ""}`).join(",")})`;
    });
    const inserted = await client.query(`
      INSERT INTO ${relations.relationships} (
        id,application_namespace,relationship_type,relationship_schema_version,
        from_version_id,to_version_id,source_reference_kind,source_reference_id,
        extension_definition_id,extension_definition_version,extensions,identity_sha256,recorded_at
      ) VALUES ${placeholders.join(",")}
      ON CONFLICT DO NOTHING
      RETURNING id,identity_sha256
    `, parameters);

    const expected = new Map(chunk.map(value => [value.relationshipId, value.identityHash]));
    const completeResult = Number.isInteger(inserted?.rowCount) && inserted.rowCount >= 0
      && Array.isArray(inserted.rows) && inserted.rows.length === inserted.rowCount
      && inserted.rows.every(row => row !== null && typeof row === "object"
        && typeof row.id === "string" && typeof row.identity_sha256 === "string");
    const unresolved = new Set(expected.keys());
    if (completeResult) {
      for (const row of inserted.rows) {
        if (!expected.has(row.id) || expected.get(row.id) !== row.identity_sha256
          || !unresolved.delete(row.id)) {
          throw relationshipConflict(row.id);
        }
      }
    }
    if (unresolved.size) {
      const stored = await client.query(`
        SELECT id,identity_sha256 FROM ${relations.relationships} WHERE id = ANY($1::text[])
      `, [[...unresolved]]);
      if (!Array.isArray(stored?.rows)) throw relationshipConflict([...unresolved][0]);
      for (const row of stored.rows) {
        if (row === null || typeof row !== "object" || !unresolved.has(row.id)
          || expected.get(row.id) !== row.identity_sha256) {
          throw relationshipConflict(row?.id);
        }
        unresolved.delete(row.id);
      }
      if (unresolved.size) throw relationshipConflict([...unresolved][0]);
    }
  }
  return snapshots.map(value => ({ relationshipId: value.relationshipId }));
}

function relationshipConflict(relationshipId) {
  return Object.assign(new Error(`Content relationship ${relationshipId} already has different identity.`),
    { code: "CONFLICT" });
}

async function insertContentRelationship(client, value, relations, schemaProfile) {
  assertNormalizedContentRelationship(value, { schemaProfile });
  const inserted = await client.query(`
    INSERT INTO ${relations.relationships} (
      id,application_namespace,relationship_type,relationship_schema_version,
      from_version_id,to_version_id,source_reference_kind,source_reference_id,
      extension_definition_id,extension_definition_version,extensions,identity_sha256,recorded_at
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12,$13)
    ON CONFLICT DO NOTHING
    RETURNING id,identity_sha256
  `, [value.relationshipId,value.applicationNamespace,value.relationshipType,
    value.relationshipSchemaVersion,value.fromVersionId,value.toVersionId,
    value.sourceReference.kind,value.sourceReference.id,value.extensionDefinitionId,
    value.extensionDefinitionVersion,JSON.stringify(value.extensions),value.identityHash,value.recordedAt]);
  if (inserted?.rowCount === 1 && Array.isArray(inserted.rows) && inserted.rows.length === 1
    && inserted.rows[0] !== null && typeof inserted.rows[0] === "object") {
    if (inserted.rows[0].id !== value.relationshipId
      || inserted.rows[0].identity_sha256 !== value.identityHash) {
      throw Object.assign(new Error(`Content relationship ${value.relationshipId} already has different identity.`),
        { code: "CONFLICT" });
    }
  } else {
    const stored = await client.query(`SELECT identity_sha256 FROM ${relations.relationships} WHERE id=$1`,
      [value.relationshipId]);
    if (stored.rows[0]?.identity_sha256 !== value.identityHash) {
      throw Object.assign(new Error(`Content relationship ${value.relationshipId} already has different identity.`),
        { code: "CONFLICT" });
    }
  }
  return { relationshipId: value.relationshipId };
}
