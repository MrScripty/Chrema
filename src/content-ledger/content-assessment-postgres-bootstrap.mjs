import { readFile } from "node:fs/promises";

import { quotePostgresIdentifier, resolvePostgresRelations } from "../shared/postgres-relations.mjs";

const validationSqlUrl = new URL("../../schemas/content-ledger/capture-v1-validation.sql", import.meta.url);
const schemaSqlUrl = new URL("../../schemas/content-ledger/assessment-schema.sql", import.meta.url);

/** Install a separate immutable assessment ledger bound to exact content versions.
 * Runtime grants trust that role; they do not provide tenant isolation. The host
 * controls later role memberships, grants, and ownership of the supplied ledger.
 */
export async function bootstrapContentAssessmentPostgres({
  database, schema, runtimeRole, readerRole, contentVersions,
} = {}) {
  if (typeof database?.transaction !== "function") {
    throw new TypeError("Content assessment bootstrap requires a transaction-capable database.");
  }
  const quotedSchema = quotePostgresIdentifier(schema, "Content assessment schema");
  const quotedRuntime = quotePostgresIdentifier(runtimeRole, "Content assessment runtime role");
  const quotedReader = quotePostgresIdentifier(readerRole, "Content assessment reader role");
  if (runtimeRole === readerRole) throw new TypeError("Content assessment runtime and reader roles must differ.");
  const contentRelation = resolvePostgresRelations(
    { contentVersions }, ["contentVersions"], "Content assessment",
  ).contentVersions;
  if (schema === contentVersions.schema) {
    throw new TypeError("Content assessment schema must differ from the content-version schema.");
  }
  const relations = Object.freeze({
    definitions: Object.freeze({ schema, name: "content_assessment_definitions" }),
    assessments: Object.freeze({ schema, name: "content_assessments" }),
  });
  const [validationTemplate, schemaTemplate] = await Promise.all([
    readFile(validationSqlUrl, "utf8"), readFile(schemaSqlUrl, "utf8"),
  ]);
  const validationDdl = validationTemplate.replaceAll("__SCHEMA__", quotedSchema);
  const ddl = schemaTemplate.replaceAll("__SCHEMA__", quotedSchema)
    .replaceAll("__CONTENT_VERSIONS__", contentRelation);

  await database.transaction(async (client) => {
    const ownerResult = await client.query("SELECT current_user AS schema_owner");
    const schemaOwner = ownerResult.rows[0]?.schema_owner;
    if (typeof schemaOwner !== "string" || !schemaOwner) {
      throw new Error("Content assessment bootstrap could not identify the schema owner.");
    }
    if (runtimeRole === schemaOwner || readerRole === schemaOwner) {
      throw new TypeError("Content assessment persistence roles must differ from the schema owner.");
    }
    await assertSafeRoles(client, runtimeRole, readerRole);

    const initial = await inspectContentVersion(client, contentVersions, runtimeRole, readerRole);
    if (initial.owner !== schemaOwner) {
      throw new TypeError("Content assessment bootstrap owner must own the supplied content-version table.");
    }
    await client.query(`LOCK TABLE ${contentRelation} IN SHARE ROW EXCLUSIVE MODE`);
    const locked = await inspectContentVersion(client, contentVersions, runtimeRole, readerRole);
    if (locked.identity !== initial.identity) {
      throw new TypeError("Content assessment content-version relation changed during bootstrap.");
    }
    const existing = await client.query(
      "SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname=$1", [schema],
    );
    if (existing.rows.length) throw new TypeError(`Content assessment schema ${schema} already exists.`);

    await client.query(`CREATE SCHEMA ${quotedSchema}`);
    await client.query(validationDdl);
    await client.query(ddl);

    const grantees = `PUBLIC, ${quotedRuntime}, ${quotedReader}`;
    await client.query(`REVOKE ALL PRIVILEGES ON SCHEMA ${quotedSchema} FROM ${grantees}`);
    await client.query(`REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA ${quotedSchema} FROM ${grantees}`);
    await client.query(`REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA ${quotedSchema} FROM ${grantees}`);
    await client.query(`REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA ${quotedSchema} FROM ${grantees}`);
    await client.query(`GRANT USAGE ON SCHEMA ${quotedSchema} TO ${quotedRuntime}, ${quotedReader}`);
    await client.query(`GRANT SELECT ON ALL TABLES IN SCHEMA ${quotedSchema} TO ${quotedRuntime}, ${quotedReader}`);
    await client.query(`GRANT INSERT ON TABLE ${quotedSchema}.content_assessments TO ${quotedRuntime}`);
    // Runtime assessment inserts execute these two local, invoker-rights validators.
    await client.query(`GRANT EXECUTE ON FUNCTION ${quotedSchema}.capture_js_json_safe(jsonb),
      ${quotedSchema}.capture_value_matches(jsonb,jsonb) TO ${quotedRuntime}`);
    await assertExpectedAcl(client, schema, schemaOwner, runtimeRole, readerRole);
  });
  return relations;
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
    if (!role) throw new TypeError(`Content assessment persistence role ${name} does not exist.`);
    if (role.rolsuper || role.rolcreatedb || role.rolcreaterole || role.rolreplication
      || role.rolbypassrls || role.has_outgoing_membership || role.can_disable_foreign_keys) {
      throw new TypeError(`Content assessment persistence role ${name} has unsafe attributes or memberships.`);
    }
  }
}

async function inspectContentVersion(client, relation, runtimeRole, readerRole) {
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
    throw new TypeError("Content assessment contentVersions must be an existing permanent PostgreSQL table.");
  }
  if (!table.runtime_schema_usage || !table.reader_schema_usage
    || !table.runtime_select || !table.reader_select) {
    throw new TypeError("Content assessment roles require existing content-version schema USAGE and SELECT grants.");
  }
  const columns = await client.query(`
    SELECT attname,atttypid::text AS type_oid,attnotnull,attnum
    FROM pg_catalog.pg_attribute
    WHERE attrelid=$1::oid AND attnum>0 AND NOT attisdropped
      AND attname=ANY($2::text[])
  `, [table.oid, ["application_namespace", "id"]]);
  const byName = new Map(columns.rows.map((row) => [row.attname, row]));
  const textOid = (await client.query("SELECT 'pg_catalog.text'::regtype::oid::text AS oid")).rows[0]?.oid;
  if (["application_namespace", "id"].some((name) =>
    !byName.has(name) || !byName.get(name).attnotnull || byName.get(name).type_oid !== textOid)) {
    throw new TypeError("Content assessment contentVersions needs required text application_namespace and id columns.");
  }
  const constraints = await client.query(`
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
  const exact = constraints.rows.filter((row) => sameColumns(row.columns, ["application_namespace", "id"])
    && !row.condeferrable && row.convalidated && row.indisunique && row.indisvalid
    && row.indisready && row.indislive && row.unpredicated && row.unexpressed);
  if (exact.length !== 1) {
    throw new TypeError("Content assessment contentVersions needs one valid exact UNIQUE(application_namespace,id) constraint.");
  }
  return { owner: table.owner, identity: [table.oid,table.owner,exact[0].oid,
    ...["application_namespace", "id"].map((name) =>
      `${name}:${byName.get(name).attnum}:${byName.get(name).type_oid}:${byName.get(name).attnotnull}`)].join(":") };
}

async function assertExpectedAcl(client, schema, owner, runtimeRole, readerRole) {
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
        AND ((kind='schema' AND privilege_type<>'USAGE')
          OR (kind='relation' AND NOT (privilege_type='SELECT'
            OR (name='content_assessments' AND privilege_type='INSERT')))
          OR (kind='function' AND NOT (privilege_type='EXECUTE'
            AND name IN ('capture_js_json_safe','capture_value_matches')))
          OR kind='column'))
      OR (grantee=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=$4)
        AND ((kind='schema' AND privilege_type<>'USAGE')
          OR (kind='relation' AND privilege_type<>'SELECT')
          OR kind IN ('function','column')))
    LIMIT 1
  `, [schema, [owner, runtimeRole, readerRole], runtimeRole, readerRole]);
  if (unexpected.rows.length) {
    const { kind, name } = unexpected.rows[0];
    throw new Error(`Content assessment ${kind} ${name} has an unexpected privilege grant.`);
  }
}

function sameColumns(actual, expected) {
  return Array.isArray(actual) && actual.length === expected.length
    && actual.every((value, index) => value === expected[index]);
}
