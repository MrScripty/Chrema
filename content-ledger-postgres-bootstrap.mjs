import { readFile } from "node:fs/promises";

import { quotePostgresIdentifier, resolvePostgresRelations } from "./postgres-relations.mjs";

const schemaSqlUrl = new URL("./content-ledger-sql/capture-schema.sql", import.meta.url);
const validationSqlUrl = new URL("./content-ledger-sql/capture-v1-validation.sql", import.meta.url);
const journalKeys = ["runs", "attempts"];
const relationNames = Object.freeze({
  extensionDefinitions: "content_extension_definitions",
  contentVersions: "content_versions",
  observedUsage: "observed_content_usage",
  relationships: "content_relationships",
});

/**
 * Install immutable capture, observed-usage, and relationship tables beside an explicit
 * journal. Runtime can record observed content, but observed usage does not
 * prove semantic influence, factual support, or disclosure safety. The
 * capture-v1 profile excludes JSON Schema format. Extension definitions are
 * registered only by the schema owner. Runtime grants trust the runtime and
 * do not provide tenant isolation. The host controls login memberships of the
 * runtime and reader roles. A privileged journal owner can alter its lineage
 * constraints after bootstrap; the host must control that authority.
 * The runtime role must not gain SET on session_replication_role after bootstrap;
 * schema-local triggers reject writes whenever that setting is replica.
 */
export async function bootstrapContentLedgerPostgres({
  database, schema, runtimeRole, readerRole, journalRelations,
} = {}) {
  if (typeof database?.transaction !== "function") {
    throw new TypeError("Content ledger bootstrap requires a transaction-capable database.");
  }
  const quotedSchema = quotePostgresIdentifier(schema, "Content ledger schema");
  const quotedRuntime = quotePostgresIdentifier(runtimeRole, "Content ledger runtime role");
  const quotedReader = quotePostgresIdentifier(readerRole, "Content ledger reader role");
  if (runtimeRole === readerRole) throw new TypeError("Content ledger runtime and reader roles must differ.");
  const journal = resolvePostgresRelations(journalRelations, journalKeys, "Content ledger journal");

  const [validationTemplate, template] = await Promise.all([
    readFile(validationSqlUrl, "utf8"), readFile(schemaSqlUrl, "utf8"),
  ]);
  const validationDdl = validationTemplate.replaceAll("__SCHEMA__", quotedSchema);
  const ddl = template.replaceAll("__SCHEMA__", quotedSchema)
    .replaceAll("__JOURNAL_RUNS__", journal.runs)
    .replaceAll("__JOURNAL_ATTEMPTS__", journal.attempts);
  const relations = Object.freeze(Object.fromEntries(Object.entries(relationNames).map(([key, name]) => [
    key, Object.freeze({ schema, name }),
  ])));

  await database.transaction(async (client) => {
    const ownerResult = await client.query("SELECT current_user AS schema_owner");
    const schemaOwner = ownerResult.rows[0]?.schema_owner;
    if (typeof schemaOwner !== "string" || !schemaOwner) {
      throw new Error("Content ledger bootstrap could not identify the schema owner.");
    }
    if (runtimeRole === schemaOwner || readerRole === schemaOwner) {
      throw new TypeError("Content ledger persistence roles must differ from the schema owner.");
    }

    const roleResult = await client.query(`
      SELECT r.rolname,r.rolsuper,r.rolcreatedb,r.rolcreaterole,
             r.rolreplication,r.rolbypassrls,
             pg_catalog.has_parameter_privilege(
               r.oid,'session_replication_role','SET'
             ) AS can_set_replication_role,
             EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members membership
                     WHERE membership.member=r.oid) AS has_outgoing_membership
      FROM pg_catalog.pg_roles r WHERE r.rolname=ANY($1::text[])
    `, [[runtimeRole, readerRole]]);
    const roles = new Map(roleResult.rows.map((row) => [row.rolname, row]));
    for (const name of [runtimeRole, readerRole]) {
      const role = roles.get(name);
      if (!role) throw new TypeError(`Content ledger persistence role ${name} does not exist.`);
      if (role.rolsuper || role.rolcreatedb || role.rolcreaterole || role.rolreplication
        || role.rolbypassrls || role.has_outgoing_membership
        || (name === runtimeRole && role.can_set_replication_role)) {
        throw new TypeError(`Content ledger persistence role ${name} has unsafe attributes or memberships.`);
      }
    }

    const initialLineage = await inspectJournalLineage(client, journal);
    if (initialLineage.owner !== schemaOwner) {
      throw new TypeError("Content ledger bootstrap owner must own the supplied journal lineage.");
    }
    if (initialLineage.owner === runtimeRole || initialLineage.owner === readerRole) {
      throw new TypeError("Content ledger persistence roles must differ from journal table owners.");
    }
    const lockedRelations = [journal.runs, journal.attempts, initialLineage.assignmentsName].sort();
    await client.query(`LOCK TABLE ${lockedRelations.join(", ")} IN SHARE ROW EXCLUSIVE MODE`);
    const lockedLineage = await inspectJournalLineage(client, journal);
    if (lockedLineage.identity !== initialLineage.identity) {
      throw new TypeError("Content ledger journal lineage changed during bootstrap.");
    }

    const existing = await client.query(
      "SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname=$1", [schema],
    );
    if (existing.rows.length) throw new TypeError(`Content ledger schema ${schema} already exists.`);

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
    await client.query(`GRANT INSERT ON TABLE ${quotedSchema}.content_versions, ${quotedSchema}.observed_content_usage, ${quotedSchema}.content_relationships TO ${quotedRuntime}`);
    // Insert triggers invoke these pure recursive validators as the runtime role.
    await client.query(`GRANT EXECUTE ON FUNCTION ${quotedSchema}.capture_js_json_safe(jsonb), ${quotedSchema}.capture_value_matches(jsonb, jsonb) TO ${quotedRuntime}`);

    // Global default privileges can grant unrelated roles direct access.
    // Fail the transaction when those ACL entries survive our explicit revokes.
    const unexpectedAcl = await client.query(`
      WITH target_schema AS (
        SELECT oid,nspname,nspowner,nspacl FROM pg_catalog.pg_namespace WHERE nspname=$1
      ), allowed AS (
        SELECT oid FROM pg_catalog.pg_roles WHERE rolname=ANY($2::text[])
      ), acl AS (
        SELECT 'schema' AS kind,n.nspname AS name,a.grantee
        FROM target_schema n,
          LATERAL pg_catalog.aclexplode(coalesce(n.nspacl,pg_catalog.acldefault('n',n.nspowner))) a
        UNION ALL
        SELECT 'relation',c.relname,a.grantee
        FROM pg_catalog.pg_class c JOIN target_schema n ON n.oid=c.relnamespace,
          LATERAL pg_catalog.aclexplode(coalesce(c.relacl,pg_catalog.acldefault(
            (CASE WHEN c.relkind='S' THEN 'S' ELSE 'r' END)::"char",c.relowner))) a
        WHERE c.relkind IN ('r','S')
        UNION ALL
        SELECT 'function',p.proname,a.grantee
        FROM pg_catalog.pg_proc p JOIN target_schema n ON n.oid=p.pronamespace,
          LATERAL pg_catalog.aclexplode(coalesce(p.proacl,pg_catalog.acldefault('f',p.proowner))) a
      )
      SELECT kind,name FROM acl WHERE grantee NOT IN (SELECT oid FROM allowed) LIMIT 1
    `, [schema, [schemaOwner, runtimeRole, readerRole]]);
    if (unexpectedAcl.rows.length) {
      const { kind, name } = unexpectedAcl.rows[0];
      throw new Error(`Content ledger ${kind} ${name} has an unexpected privilege grant.`);
    }
  });
  return relations;
}

async function inspectJournalLineage(client, journal) {
  if (journal.runs === journal.attempts) {
    throw new TypeError("Content ledger journal runs and attempts must be distinct tables.");
  }
  const tables = await client.query(`
    SELECT c.oid::text AS oid,c.relkind,c.relowner::text AS owner_oid,
           pg_catalog.pg_get_userbyid(c.relowner) AS owner_name,
           n.nspname,c.relname
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
    WHERE c.oid=ANY(ARRAY[pg_catalog.to_regclass($1)::oid,pg_catalog.to_regclass($2)::oid])
  `, [journal.runs, journal.attempts]);
  if (tables.rows.length !== 2 || tables.rows.some((row) => !["r", "p"].includes(row.relkind))) {
    throw new TypeError("Content ledger journal runs and attempts must be existing PostgreSQL tables.");
  }
  const runsOid = await relationOid(client, journal.runs);
  const attemptsOid = await relationOid(client, journal.attempts);
  const runs = tables.rows.find((row) => row.oid === runsOid);
  const attempts = tables.rows.find((row) => row.oid === attemptsOid);
  if (!runs || !attempts || runs.owner_oid !== attempts.owner_oid) {
    throw new TypeError("Content ledger journal tables must have one owner.");
  }
  const attemptCandidates = (await foreignKeys(client, attempts.oid)).filter((key) =>
    sameColumns(key.from_columns, ["run_id", "assignment_id"])
    && sameColumns(key.to_columns, ["run_id", "id"]));
  if (attemptCandidates.length !== 1) {
    throw new TypeError("Content ledger attempts must have one exact assignment lineage foreign key.");
  }
  const attemptsAssignment = attemptCandidates[0];
  const assignmentResult = await client.query(`
    SELECT c.oid::text AS oid,c.relkind,c.relowner::text AS owner_oid,
           n.nspname,c.relname
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
    WHERE c.oid=$1::oid
  `, [attemptsAssignment.parent_oid]);
  const assignment = assignmentResult.rows[0];
  if (!assignment || !["r", "p"].includes(assignment.relkind)
    || assignment.owner_oid !== runs.owner_oid
    || assignment.oid === runs.oid || assignment.oid === attempts.oid) {
    throw new TypeError("Content ledger assignment lineage must belong to the supplied journal owner.");
  }
  const runCandidates = (await foreignKeys(client, assignment.oid)).filter((key) =>
    key.parent_oid === runs.oid && sameColumns(key.from_columns, ["run_id"])
    && sameColumns(key.to_columns, ["id"]));
  if (runCandidates.length !== 1) {
    throw new TypeError("Content ledger assignments must have one exact run lineage foreign key.");
  }
  const assignmentsRun = runCandidates[0];
  for (const key of [attemptsAssignment, assignmentsRun]) {
    if (!key.convalidated || Number(key.trigger_count) < 1 || !key.triggers_enabled) {
      throw new TypeError("Content ledger journal lineage foreign keys must be validated and enforced.");
    }
  }
  const requiredColumns = [
    [runs.oid, "id"], [assignment.oid, "id"], [assignment.oid, "run_id"],
    [attempts.oid, "run_id"], [attempts.oid, "assignment_id"],
  ];
  const columns = await client.query(`
    SELECT attrelid::text AS relation_oid,attname,attnotnull
    FROM pg_catalog.pg_attribute
    WHERE attrelid=ANY($1::oid[]) AND attname=ANY($2::text[])
      AND attnum>0 AND NOT attisdropped
  `, [[runs.oid, assignment.oid, attempts.oid], ["id", "run_id", "assignment_id"]]);
  if (requiredColumns.some(([oid, name]) =>
    !columns.rows.some((column) => column.relation_oid === oid
      && column.attname === name && column.attnotnull))) {
    throw new TypeError("Content ledger journal lineage columns must be required.");
  }
  const assignmentsName = `${quoteCatalogIdentifier(assignment.nspname)}.${quoteCatalogIdentifier(assignment.relname)}`;
  return { owner: runs.owner_name, assignmentsName,
    identity: [runs.oid, attempts.oid, assignment.oid, attemptsAssignment.oid, assignmentsRun.oid,
      runs.owner_oid, assignmentsName].join(":"),
  };
}

async function relationOid(client, qualifiedName) {
  const result = await client.query("SELECT pg_catalog.to_regclass($1)::oid::text AS oid", [qualifiedName]);
  return result.rows[0]?.oid ?? null;
}

async function foreignKeys(client, relationOidValue) {
  const result = await client.query(`
    SELECT con.oid::text AS oid,con.confrelid::text AS parent_oid,con.convalidated,
      ARRAY(SELECT a.attname::text FROM unnest(con.conkey) WITH ORDINALITY AS k(num,ordinal)
        JOIN pg_catalog.pg_attribute a ON a.attrelid=con.conrelid AND a.attnum=k.num
        ORDER BY k.ordinal) AS from_columns,
      ARRAY(SELECT a.attname::text FROM unnest(con.confkey) WITH ORDINALITY AS k(num,ordinal)
        JOIN pg_catalog.pg_attribute a ON a.attrelid=con.confrelid AND a.attnum=k.num
        ORDER BY k.ordinal) AS to_columns,
      (SELECT count(*) FROM pg_catalog.pg_trigger t WHERE t.tgconstraint=con.oid) AS trigger_count,
      (SELECT bool_and(t.tgenabled IN ('O','A') AND t.tgisinternal)
       FROM pg_catalog.pg_trigger t WHERE t.tgconstraint=con.oid) AS triggers_enabled
    FROM pg_catalog.pg_constraint con
    WHERE con.contype='f' AND con.conrelid=$1::oid
  `, [relationOidValue]);
  return result.rows;
}

function sameColumns(actual, expected) {
  return Array.isArray(actual) && actual.length === expected.length
    && actual.every((column, index) => column === expected[index]);
}

function quoteCatalogIdentifier(value) { return `"${value.replaceAll('"', '""')}"`; }
