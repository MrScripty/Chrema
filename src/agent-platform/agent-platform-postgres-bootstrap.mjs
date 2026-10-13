import { readFile } from "node:fs/promises";

import { quotePostgresIdentifier } from "../shared/postgres-relations.mjs";

const schemaSqlUrl = new URL("../../schemas/agent-platform/journal-schema.sql", import.meta.url);
const relationNames = Object.freeze({
  versions: "agent_workflow_versions",
  prices: "agent_model_price_versions",
  runs: "agent_workflow_runs",
  assignments: "agent_workflow_assignments",
  assignmentTargets: "agent_workflow_assignment_targets",
  attempts: "agent_workflow_attempts",
  events: "agent_workflow_events",
  stagedCommands: "agent_workflow_staged_commands",
  validations: "agent_workflow_validations",
  admissions: "agent_workflow_admissions",
});
const runFamilyRelationNames = Object.freeze({
  families: "agent_workflow_run_families",
  members: "agent_workflow_run_family_members",
});

/**
 * Install the generic journal in a new schema and grant trusted persistence
 * roles. The runtime can record observations and validation/admission evidence;
 * those writes do not prove semantic influence, factual support, or disclosure
 * safety. These grants do not provide tenant isolation. The host controls which
 * login roles are members of the runtime and reader roles after bootstrap.
 * Database completion checks only completed_at, final_summary, and the absence
 * of an admitted manifest hash. The host assertCompletion hook and application
 * policy must enforce assignment and result lineage; this schema does not
 * implement any application-specific bounded-analysis completion guard.
 * The runtime role must not gain SET on session_replication_role after bootstrap;
 * schema-local triggers reject writes whenever that setting is replica.
 */
export async function bootstrapAgentPlatformPostgres({ database, schema, runtimeRole, readerRole } = {}) {
  if (typeof database?.transaction !== "function") {
    throw new TypeError("Agent platform bootstrap requires a transaction-capable database.");
  }
  const quotedSchema = quotePostgresIdentifier(schema, "Agent platform schema");
  const quotedRuntime = quotePostgresIdentifier(runtimeRole, "Agent platform runtime role");
  const quotedReader = quotePostgresIdentifier(readerRole, "Agent platform reader role");
  if (runtimeRole === readerRole) throw new TypeError("Agent platform runtime and reader roles must differ.");

  const template = await readFile(schemaSqlUrl, "utf8");
  const ddl = template.replaceAll("__SCHEMA__", quotedSchema);
  const tables = Object.freeze(Object.fromEntries(Object.entries(relationNames).map(([key, name]) => [
    key, Object.freeze({ schema, name }),
  ])));

  await database.transaction(async (client) => {
    const ownerResult = await client.query("SELECT current_user AS schema_owner");
    const schemaOwner = ownerResult.rows[0]?.schema_owner;
    if (typeof schemaOwner !== "string" || !schemaOwner) {
      throw new Error("Agent platform bootstrap could not identify the schema owner.");
    }
    if (runtimeRole === schemaOwner || readerRole === schemaOwner) {
      throw new TypeError("Agent platform persistence roles must differ from the schema owner.");
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
      if (!role) throw new TypeError(`Agent platform persistence role ${name} does not exist.`);
      if (role.rolsuper || role.rolcreatedb || role.rolcreaterole || role.rolreplication
        || role.rolbypassrls || role.has_outgoing_membership
        || (name === runtimeRole && role.can_set_replication_role)) {
        throw new TypeError(`Agent platform persistence role ${name} has unsafe attributes or memberships.`);
      }
    }

    const existing = await client.query(
      "SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname=$1", [schema],
    );
    if (existing.rows.length) throw new TypeError(`Agent platform schema ${schema} already exists.`);

    await client.query(`CREATE SCHEMA ${quotedSchema}`);
    await client.query(ddl);

    // Clear PUBLIC and any default direct grants before installing the exact
    // runtime/reader ACL. Identity sequences need USAGE for runtime inserts.
    const grantees = `PUBLIC, ${quotedRuntime}, ${quotedReader}`;
    await client.query(`REVOKE ALL PRIVILEGES ON SCHEMA ${quotedSchema} FROM ${grantees}`);
    await client.query(`REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA ${quotedSchema} FROM ${grantees}`);
    await client.query(`REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA ${quotedSchema} FROM ${grantees}`);
    await client.query(`REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA ${quotedSchema} FROM ${grantees}`);
    await client.query(`GRANT USAGE ON SCHEMA ${quotedSchema} TO ${quotedRuntime}, ${quotedReader}`);
    await client.query(`GRANT SELECT ON ALL TABLES IN SCHEMA ${quotedSchema} TO ${quotedRuntime}, ${quotedReader}`);
    await client.query(`GRANT INSERT ON ALL TABLES IN SCHEMA ${quotedSchema} TO ${quotedRuntime}`);
    await client.query(`GRANT UPDATE ON TABLE ${quotedSchema}.agent_workflow_runs,
      ${quotedSchema}.agent_workflow_assignments, ${quotedSchema}.agent_workflow_attempts,
      ${quotedSchema}.agent_workflow_staged_commands TO ${quotedRuntime}`);
    await client.query(`GRANT USAGE ON ALL SEQUENCES IN SCHEMA ${quotedSchema} TO ${quotedRuntime}`);
    // Narrow definer helpers lock an immutable family row without granting
    // runtime UPDATE on the family or membership tables.
    await client.query(`GRANT EXECUTE ON FUNCTION ${quotedSchema}.lock_agent_workflow_run_family(text) TO ${quotedRuntime}`);
    await client.query(`GRANT EXECUTE ON FUNCTION ${quotedSchema}.try_lock_agent_workflow_run_family(text) TO ${quotedRuntime}`);

    // Global default privileges can grant new objects to unrelated roles.
    // Reject such grants rather than committing a schema with broader access.
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
      throw new Error(`Agent platform ${kind} ${name} has an unexpected privilege grant.`);
    }
  });
  return tables;
}

/** Return the unchanged journal map and separate clean-schema family bindings. */
export async function bootstrapAgentPlatformPostgresWithRunFamilies(options = {}) {
  const postgresRelations = await bootstrapAgentPlatformPostgres(options);
  const schema = postgresRelations.runs.schema;
  const postgresRunFamilyRelations = Object.freeze(Object.fromEntries(
    Object.entries(runFamilyRelationNames).map(([key, name]) => [key, Object.freeze({ schema, name })]),
  ));
  return Object.freeze({ postgresRelations, postgresRunFamilyRelations });
}
