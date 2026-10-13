import { readFile } from "node:fs/promises";

import { quotePostgresIdentifier, resolvePostgresRelations } from "../shared/postgres-relations.mjs";

const schemaSqlUrl = new URL("../../schemas/content-ledger/transfer-schema.sql", import.meta.url);
const deliveryExtensionMarker = "-- CONTENT TRANSFER DELIVERY EXTENSION --";
const relationNames = Object.freeze({
  policyDefinitions: "content_transfer_policy_definitions",
  assessmentDefinitions: "content_transfer_assessment_definitions",
  assessments: "content_transfer_assessments",
  requests: "content_transfer_requests",
  decisions: "content_transfer_decisions",
  deliveryOutcomes: "content_transfer_delivery_outcomes",
});
const externalColumns = ["application_namespace", "id", "content_id", "content_type",
  "capture_status", "capture_source", "content_sha256", "hash_encoding", "byte_length", "recorded_at"];
const deliverySourceColumns = ["source_kind", "source_record_id", "source_field"];

/**
 * Install six immutable transfer relations in a new schema and bind them to an
 * existing content-version table. The bootstrap caller must own that table and
 * use its owner connection. Policy and assessment definitions must be registered
 * through that owner before runtime writes dependent records. Runtime and reader
 * roles must already have SELECT and schema USAGE on the supplied table.
 * The runtime also receives column-level UPDATE(id) on requests solely so
 * SELECT ... FOR UPDATE OF requests can lock a row. An unconditional immutable
 * trigger rejects UPDATE and DELETE even when issued by the table owner;
 * runtime has no table-level UPDATE, DELETE, or TRUNCATE grant.
 * Foreign keys require session_replication_role to stay out of replica mode:
 * neither persistence role may have SET on that parameter, and the host must
 * not grant it after bootstrap.
 * Runtime grants trust that role; they do not provide tenant isolation. The host
 * controls login membership. Decisions always leave delivery disabled.
 * Assessment IDs remain an array: the database cannot enforce membership or
 * assessment eligibility through a foreign key; the decision service checks it.
 */
export async function bootstrapContentTransferPostgres({
  database, schema, runtimeRole, readerRole, contentVersions,
} = {}) {
  return bootstrapTransfer({ database, schema, runtimeRole, readerRole, contentVersions }, false);
}

/** Install transfer records and durable delivery claims together in one new schema. */
export async function bootstrapContentTransferDeliveryPostgres({
  database, schema, runtimeRole, readerRole, contentVersions,
} = {}) {
  return bootstrapTransfer({ database, schema, runtimeRole, readerRole, contentVersions }, true);
}

async function bootstrapTransfer({ database, schema, runtimeRole, readerRole, contentVersions }, includeDelivery) {
  if (typeof database?.transaction !== "function") {
    throw new TypeError("Content transfer bootstrap requires a transaction-capable database.");
  }
  const quotedSchema = quotePostgresIdentifier(schema, "Content transfer schema");
  const quotedRuntime = quotePostgresIdentifier(runtimeRole, "Content transfer runtime role");
  const quotedReader = quotePostgresIdentifier(readerRole, "Content transfer reader role");
  if (runtimeRole === readerRole) throw new TypeError("Content transfer runtime and reader roles must differ.");
  const contentRelation = resolvePostgresRelations(
    { contentVersions }, ["contentVersions"], "Content transfer",
  ).contentVersions;
  if (schema === contentVersions.schema) {
    throw new TypeError("Content transfer schema must differ from the content-version schema.");
  }
  const relations = Object.freeze({
    ...Object.fromEntries(Object.entries(relationNames).map(([key, name]) => [
      key, Object.freeze({ schema, name }),
    ])),
    contentVersions: Object.freeze({ schema: contentVersions.schema, name: contentVersions.name }),
  });
  const schemaTemplate = await readFile(schemaSqlUrl, "utf8");
  const markerOffset = schemaTemplate.indexOf(deliveryExtensionMarker);
  if (markerOffset < 0 || schemaTemplate.indexOf(deliveryExtensionMarker, markerOffset + 1) >= 0) {
    throw new Error("Content transfer SQL must contain one delivery extension marker.");
  }
  const ddl = (includeDelivery ? schemaTemplate : schemaTemplate.slice(0, markerOffset))
    .replaceAll("__SCHEMA__", quotedSchema)
    .replaceAll("__CONTENT_VERSIONS__", contentRelation);

  await database.transaction(async (client) => {
    const ownerResult = await client.query("SELECT current_user AS schema_owner");
    const schemaOwner = ownerResult.rows[0]?.schema_owner;
    if (typeof schemaOwner !== "string" || !schemaOwner) {
      throw new Error("Content transfer bootstrap could not identify the schema owner.");
    }
    if (runtimeRole === schemaOwner || readerRole === schemaOwner) {
      throw new TypeError("Content transfer persistence roles must differ from the schema owner.");
    }
    await assertSafeRoles(client, runtimeRole, readerRole);

    const initial = await inspectContentVersion(client, contentVersions, runtimeRole, readerRole, includeDelivery);
    if (initial.owner !== schemaOwner) {
      throw new TypeError("Content transfer bootstrap owner must own the supplied content-version table.");
    }
    await client.query(`LOCK TABLE ${contentRelation} IN SHARE ROW EXCLUSIVE MODE`);
    const locked = await inspectContentVersion(client, contentVersions, runtimeRole, readerRole, includeDelivery);
    if (locked.identity !== initial.identity || locked.owner !== schemaOwner) {
      throw new TypeError("Content transfer content-version table changed during bootstrap.");
    }

    const existing = await client.query(
      "SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname=$1", [schema],
    );
    if (existing.rows.length) throw new TypeError(`Content transfer schema ${schema} already exists.`);

    await client.query(`CREATE SCHEMA ${quotedSchema}`);
    await client.query(ddl);

    const grantees = `PUBLIC, ${quotedRuntime}, ${quotedReader}`;
    await client.query(`REVOKE ALL PRIVILEGES ON SCHEMA ${quotedSchema} FROM ${grantees}`);
    await client.query(`REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA ${quotedSchema} FROM ${grantees}`);
    await client.query(`REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA ${quotedSchema} FROM ${grantees}`);
    await client.query(`REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA ${quotedSchema} FROM ${grantees}`);
    await client.query(`GRANT USAGE ON SCHEMA ${quotedSchema} TO ${quotedRuntime}, ${quotedReader}`);
    await client.query(`GRANT SELECT ON ALL TABLES IN SCHEMA ${quotedSchema} TO ${quotedRuntime}, ${quotedReader}`);
    await client.query(`GRANT INSERT ON TABLE ${quotedSchema}.content_transfer_assessments,
      ${quotedSchema}.content_transfer_requests, ${quotedSchema}.content_transfer_decisions,
      ${quotedSchema}.content_transfer_delivery_outcomes TO ${quotedRuntime}`);
    await client.query(`GRANT UPDATE (id) ON TABLE ${quotedSchema}.content_transfer_requests TO ${quotedRuntime}`);
    if (includeDelivery) {
      await client.query(`GRANT INSERT ON TABLE ${quotedSchema}.content_transfer_delivery_attempts TO ${quotedRuntime}`);
      await client.query(`GRANT UPDATE (id) ON TABLE ${quotedSchema}.content_transfer_delivery_attempts TO ${quotedRuntime}`);
    }
    await assertExpectedAcl(client, schema, schemaOwner, runtimeRole, readerRole, includeDelivery);
  });
  if (!includeDelivery) return relations;
  return Object.freeze({
    transferRelations: relations,
    deliveryAttempts: Object.freeze({ schema, name: "content_transfer_delivery_attempts" }),
  });
}

async function assertSafeRoles(client, runtimeRole, readerRole) {
  const result = await client.query(`
    SELECT r.rolname,r.rolsuper,r.rolcreatedb,r.rolcreaterole,r.rolreplication,r.rolbypassrls,
      pg_catalog.has_parameter_privilege(r.oid,'session_replication_role','SET') AS can_disable_foreign_keys,
      EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m WHERE m.member=r.oid) AS has_outgoing_membership
    FROM pg_catalog.pg_roles r WHERE r.rolname=ANY($1::text[])
  `, [[runtimeRole, readerRole]]);
  const roles = new Map(result.rows.map((row) => [row.rolname, row]));
  for (const name of [runtimeRole, readerRole]) {
    const role = roles.get(name);
    if (!role) throw new TypeError(`Content transfer persistence role ${name} does not exist.`);
    if (role.rolsuper || role.rolcreatedb || role.rolcreaterole || role.rolreplication
      || role.rolbypassrls || role.has_outgoing_membership || role.can_disable_foreign_keys) {
      throw new TypeError(`Content transfer persistence role ${name} has unsafe attributes or memberships.`);
    }
  }
}

async function inspectContentVersion(client, relation, runtimeRole, readerRole, includeDelivery) {
  const result = await client.query(`
    SELECT c.oid::text AS oid,c.relkind,c.relpersistence,
      pg_catalog.pg_get_userbyid(c.relowner) AS owner,
      pg_catalog.has_schema_privilege($3::name,n.oid,'USAGE') AS runtime_schema_usage,
      pg_catalog.has_schema_privilege($4::name,n.oid,'USAGE') AS reader_schema_usage,
      pg_catalog.has_table_privilege($3::name,c.oid,'SELECT') AS runtime_select,
      pg_catalog.has_table_privilege($4::name,c.oid,'SELECT') AS reader_select
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname=$1 AND c.relname=$2
  `, [relation.schema, relation.name, runtimeRole, readerRole]);
  const table = result.rows[0];
  if (!table || !["r", "p"].includes(table.relkind) || table.relpersistence !== "p") {
    throw new TypeError("Content transfer contentVersions must be an existing permanent PostgreSQL table.");
  }
  if (!table.runtime_schema_usage || !table.reader_schema_usage
    || !table.runtime_select || !table.reader_select) {
    throw new TypeError("Content transfer persistence roles require existing schema USAGE and content-version SELECT grants.");
  }

  const requiredColumns = includeDelivery ? [...externalColumns, ...deliverySourceColumns] : externalColumns;
  const columns = await client.query(`
    SELECT attname,atttypid::text AS type_oid,attnotnull,attnum
    FROM pg_catalog.pg_attribute
    WHERE attrelid=$1::oid AND attnum>0 AND NOT attisdropped AND attname=ANY($2::text[])
  `, [table.oid, requiredColumns]);
  const byName = new Map(columns.rows.map((column) => [column.attname, column]));
  if (externalColumns.some((name) => !byName.has(name))) {
    throw new TypeError("Content transfer contentVersions lacks columns required by persistence.");
  }
  if (includeDelivery && deliverySourceColumns.some((name) => !byName.has(name))) {
    throw new TypeError("Content transfer delivery requires non-null text source columns on contentVersions.");
  }
  const textOid = await client.query("SELECT 'pg_catalog.text'::regtype::oid::text AS oid");
  const contentText = textOid.rows[0]?.oid;
  if (["application_namespace", "id"].some((name) =>
    !byName.get(name).attnotnull || byName.get(name).type_oid !== contentText)) {
    throw new TypeError("Content transfer contentVersions needs required text application_namespace and id columns.");
  }
  if (includeDelivery && deliverySourceColumns.some((name) =>
    !byName.get(name).attnotnull || byName.get(name).type_oid !== contentText)) {
    throw new TypeError("Content transfer delivery requires non-null text source columns on contentVersions.");
  }
  const key = await client.query(`
    SELECT con.oid::text AS oid,con.condeferrable,con.convalidated,
      idx.indisunique,idx.indisvalid,idx.indisready,idx.indislive,
      idx.indpred IS NULL AS unpredicated,idx.indexprs IS NULL AS unexpressed,
      ARRAY(SELECT a.attname::text
        FROM unnest(con.conkey) WITH ORDINALITY AS k(num,ordinal)
        JOIN pg_catalog.pg_attribute a ON a.attrelid=con.conrelid AND a.attnum=k.num
        ORDER BY k.ordinal) AS columns
    FROM pg_catalog.pg_constraint con
    JOIN pg_catalog.pg_index idx ON idx.indexrelid=con.conindid
    WHERE con.conrelid=$1::oid AND con.contype='u'
  `, [table.oid]);
  const exact = key.rows.filter((row) => sameColumns(row.columns, ["application_namespace", "id"])
    && !row.condeferrable && row.convalidated && row.indisunique && row.indisvalid
    && row.indisready && row.indislive && row.unpredicated && row.unexpressed);
  if (exact.length !== 1) {
    throw new TypeError("Content transfer contentVersions needs one valid exact UNIQUE(application_namespace,id) constraint.");
  }
  return { owner: table.owner, identity: [table.oid, table.owner, exact[0].oid,
    ...requiredColumns.map((name) => `${name}:${byName.get(name).attnum}:${byName.get(name).type_oid}`),
    ...(includeDelivery ? deliverySourceColumns.map((name) => `${name}:notnull:${byName.get(name).attnotnull}`) : [])].join(":") };
}

async function assertExpectedAcl(client, schema, owner, runtimeRole, readerRole, includeDelivery) {
  const unexpected = await client.query(`
    WITH target_schema AS (
      SELECT oid,nspname,nspowner,nspacl FROM pg_catalog.pg_namespace WHERE nspname=$1
    ), allowed AS (
      SELECT oid FROM pg_catalog.pg_roles WHERE rolname=ANY($2::text[])
    ), acl AS (
      SELECT 'schema' AS kind,n.nspname AS name,a.grantee,a.privilege_type
      FROM target_schema n,
        LATERAL pg_catalog.aclexplode(coalesce(n.nspacl,pg_catalog.acldefault('n',n.nspowner))) a
      UNION ALL
      SELECT 'relation',c.relname,a.grantee,a.privilege_type
      FROM pg_catalog.pg_class c JOIN target_schema n ON n.oid=c.relnamespace,
        LATERAL pg_catalog.aclexplode(coalesce(c.relacl,pg_catalog.acldefault(
          (CASE WHEN c.relkind='S' THEN 'S' ELSE 'r' END)::"char",c.relowner))) a
      WHERE c.relkind IN ('r','p','S')
      UNION ALL
      SELECT 'function',p.proname,a.grantee,a.privilege_type
      FROM pg_catalog.pg_proc p JOIN target_schema n ON n.oid=p.pronamespace,
        LATERAL pg_catalog.aclexplode(coalesce(p.proacl,pg_catalog.acldefault('f',p.proowner))) a
      UNION ALL
      SELECT 'column',c.relname || '.' || attr.attname,a.grantee,a.privilege_type
      FROM pg_catalog.pg_class c JOIN target_schema n ON n.oid=c.relnamespace
      JOIN pg_catalog.pg_attribute attr ON attr.attrelid=c.oid
        AND attr.attnum>0 AND NOT attr.attisdropped,
        LATERAL pg_catalog.aclexplode(attr.attacl) a
      WHERE c.relkind IN ('r','p')
    )
    SELECT kind,name,privilege_type FROM acl
    WHERE grantee NOT IN (SELECT oid FROM allowed)
      OR (grantee=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=$3)
        AND ((kind='schema' AND privilege_type <> 'USAGE')
          OR (kind='function')
          OR (kind='relation' AND privilege_type <> 'SELECT'
            AND NOT (privilege_type='INSERT' AND name IN (
              'content_transfer_assessments','content_transfer_requests',
              'content_transfer_decisions','content_transfer_delivery_outcomes')
              OR ($5::boolean AND name='content_transfer_delivery_attempts')))
          OR (kind='column' AND (privilege_type <> 'UPDATE'
            OR NOT (name='content_transfer_requests.id'
              OR ($5::boolean AND name='content_transfer_delivery_attempts.id'))))))
      OR (grantee=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=$4)
        AND ((kind='schema' AND privilege_type <> 'USAGE')
          OR (kind='function')
          OR (kind='relation' AND privilege_type <> 'SELECT')
          OR kind='column'))
    LIMIT 1
  `, [schema, [owner, runtimeRole, readerRole], runtimeRole, readerRole, includeDelivery]);
  if (unexpected.rows.length) {
    const { kind, name } = unexpected.rows[0];
    throw new Error(`Content transfer ${kind} ${name} has an unexpected privilege grant.`);
  }
}

function sameColumns(actual, expected) {
  return Array.isArray(actual) && actual.length === expected.length
    && actual.every((column, index) => column === expected[index]);
}
