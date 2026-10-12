#!/usr/bin/env node
import assert from "node:assert/strict";
import { writeSync } from "node:fs";
import { lstat, readFile, readdir, realpath } from "node:fs/promises";
import { isAbsolute, join, relative, resolve, sep } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { runSyntheticArchiveApplication } from "../standalone/synthetic-archive/application.mjs";
import { runSyntheticToolHost } from '../standalone/synthetic-tool-host/application.mjs';
import { auditSource } from "./audit.mjs";
import { barrelBytes, loadManifest, packageJsonBytes, repositoryRoot } from "../tools/build-standalone.mjs";

function parseOptions(args) {
  const options = {};
  for (let index = 0; index < args.length; index += 2) {
    const key = args[index];
    if (!["--packages", "--pg-client-module", "--postgres-url-env"].includes(key)
      || !args[index + 1] || options[key]) throw new Error("Invalid verify options.");
    options[key] = args[index + 1];
  }
  if (!options["--packages"] || Boolean(options["--pg-client-module"]) !== Boolean(options["--postgres-url-env"])) {
    throw new Error("Expected --packages <consumer-root> and paired --pg-client-module/--postgres-url-env.");
  }
  if (options["--postgres-url-env"] && options["--postgres-url-env"] !== "STANDALONE_POSTGRES_URL") {
    throw new Error("PostgreSQL URL must use the generic STANDALONE_POSTGRES_URL bridge.");
  }
  return options;
}

function disposableUrl(value) {
  const url = new URL(value);
  const socket = url.searchParams.get("host");
  const queryKeys = [...url.searchParams.keys()];
  if (!["postgres:", "postgresql:"].includes(url.protocol) || url.host || url.hash || url.password || url.username
    || queryKeys.length !== 3 || new Set(queryKeys).size !== 3
    || queryKeys.sort().join("|") !== "host|port|user"
    || !/^\/tmp\/[A-Za-z0-9_.-]+\/socket$/.test(socket ?? "")
    || !/^\/[A-Za-z0-9_]+$/.test(url.pathname)
    || !/^[0-9]{4,5}$/.test(url.searchParams.get("port") ?? "")
    || !/^[A-Za-z_][A-Za-z0-9_]*$/.test(url.searchParams.get("user") ?? "")) {
    throw new Error("Expected an explicit disposable local PostgreSQL socket URL.");
  }
  return value;
}

async function installedNamespaces(consumerRoot) {
  const manifest = JSON.parse(await readFile(new URL("../standalone/manifest.json", import.meta.url), "utf8"));
  const result = { "agent-platform": {}, "content-ledger": {} };
  for (const [name, packageSpec] of Object.entries(manifest.packages)) {
    const packageRoot = join(consumerRoot, "node_modules", name);
    const metadata = JSON.parse(await readFile(join(packageRoot, "package.json"), "utf8"));
    assert.equal(metadata.name, name);
    assert.deepEqual(metadata.dependencies, {});
    for (const [subpath, entries] of Object.entries(packageSpec.exports)) {
      const modulePath = resolve(packageRoot, metadata.exports[subpath]);
      const within = relative(packageRoot, modulePath);
      if (!within || within === ".." || within.startsWith(`..${sep}`) || isAbsolute(within)) {
        throw new Error(`Installed export escapes its package: ${name}${subpath}`);
      }
      const stat = await lstat(modulePath);
      if (!stat.isFile() || stat.isSymbolicLink()) throw new Error(`Invalid installed export: ${name}${subpath}`);
      const namespace = await import(pathToFileURL(modulePath).href);
      assert.deepEqual(Object.keys(namespace).sort(), entries.flatMap(entry => entry.names).sort());
      result[name][subpath] = namespace;
    }
  }
  const select = (name, subpath) => result[name][subpath];
  return {
    agentPlatform: {
      journal: select("agent-platform", "./journal"),
      bootstrap: select("agent-platform", "./bootstrap"),
      postgresReader: select("agent-platform", "./postgres-reader"),
      revisionController: select("agent-platform", "./revision-controller"),
      assignmentLifecycle: select("agent-platform", "./assignment-lifecycle"),
      boundedAnalysisWorker: select("agent-platform", "./bounded-analysis-worker"),
      toolAssignment: select('agent-platform', './tool-assignment-v1'),
      assignedExecution: select('agent-platform', './assigned-execution-v1'),
    },
    contentLedger: {
      capture: select("content-ledger", "./capture"),
      relationships: select("content-ledger", "./relationships"),
      assessments: select("content-ledger", "./assessments"),
      transfers: select("content-ledger", "./transfers"),
      delivery: select("content-ledger", "./delivery"),
    },
  };
}

async function regularTree(root, prefix = "") {
  const files = [];
  const directories = [];
  for (const entry of await readdir(join(root, prefix), { withFileTypes: true })) {
    const path = prefix ? `${prefix}/${entry.name}` : entry.name;
    const stat = await lstat(join(root, path));
    if (stat.isSymbolicLink() || (!stat.isDirectory() && !stat.isFile())) {
      throw new Error(`Installed package has a symlink or special entry: ${path}`);
    }
    if (stat.isDirectory()) {
      directories.push(path);
      const nested = await regularTree(root, path);
      directories.push(...nested.directories);
      files.push(...nested.files);
    } else files.push(path);
  }
  return { files: files.sort(), directories: directories.sort() };
}

async function assertInstalledPackages(consumerRoot) {
  const modulesRoot = join(consumerRoot, "node_modules");
  const modulesStat = await lstat(modulesRoot);
  if (!modulesStat.isDirectory() || modulesStat.isSymbolicLink()) {
    throw new Error("Consumer node_modules must be a real directory.");
  }
  const packageManifest = JSON.parse(await readFile(new URL("../standalone/manifest.json", import.meta.url), "utf8"));
  const loaded = await loadManifest(join(repositoryRoot, "standalone/manifest.json"));
  for (const name of ["agent-platform", "content-ledger"]) {
    const packageRoot = join(modulesRoot, name);
    const stat = await lstat(packageRoot);
    if (!stat.isDirectory() || stat.isSymbolicLink()) throw new Error(`Invalid installed package: ${name}`);
    const spec = loaded[name];
    const expected = new Map(spec.files.map(file => [file.destination, file.bytes]));
    expected.set("package.json", packageJsonBytes(spec.metadata, packageManifest.packages[name].exports));
    for (const [subpath, entries] of Object.entries(packageManifest.packages[name].exports)) {
      expected.set(`exports/${subpath.slice(2)}.mjs`, barrelBytes(entries));
    }
    const actual = await regularTree(packageRoot);
    if (actual.files.length !== expected.size || actual.files.some(path => !expected.has(path))) {
      throw new Error(`Installed package has missing or unexpected files: ${name}`);
    }
    const expectedDirectories = new Set();
    for (const path of expected.keys()) {
      const parts = path.split("/");
      for (let index = 1; index < parts.length; index++) {
        expectedDirectories.add(parts.slice(0, index).join("/"));
      }
    }
    if (actual.directories.length !== expectedDirectories.size
      || actual.directories.some(path => !expectedDirectories.has(path))) {
      throw new Error(`Installed package has missing or unexpected directories: ${name}`);
    }
    for (const [path, bytes] of expected) {
      if (!(await readFile(join(packageRoot, path))).equals(bytes)) {
        throw new Error(`Installed package bytes differ: ${name}/${path}`);
      }
    }
  }
}

async function postgresAdapter(modulePath, connectionString) {
  if (!isAbsolute(modulePath)) throw new Error("Host PostgreSQL client module path must be absolute.");
  const stat = await lstat(modulePath);
  if (!stat.isFile() || stat.isSymbolicLink()) throw new Error("Host PostgreSQL client must be a regular file.");
  const module = await import(pathToFileURL(modulePath).href);
  const Pool = module.Pool ?? module.default?.Pool;
  if (typeof Pool !== "function") throw new Error("Host PostgreSQL client must export Pool.");
  const quoteIdentifier = value => {
    if (!/^[a-z_][a-z0-9_]*$/.test(value)) throw new Error("Invalid disposable role name.");
    return `"${value}"`;
  };
  return {
    Pool,
    connectionString,
    databaseAdapter: pool => ({
      query: pool.query.bind(pool),
      async transaction(work) {
        const client = await pool.connect();
        let begun = false;
        try {
          await client.query("BEGIN");
          begun = true;
          const result = await work(client);
          await client.query("COMMIT");
          begun = false;
          return result;
        } catch (error) {
          if (begun) await client.query("ROLLBACK").catch(() => undefined);
          throw error;
        } finally {
          client.release();
        }
      },
    }),
    async createRole(pool, role) {
      await pool.query(`CREATE ROLE ${quoteIdentifier(role)} LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS`);
    },
    openRolePool(role, applicationName) {
      const url = new URL(connectionString);
      url.searchParams.set("user", role);
      return new Pool({ connectionString: url.toString(), max: 6, application_name: applicationName });
    },
  };
}

export async function verifyInstalled({ packages, pgClientModule = null, postgresUrl = null }) {
  const consumerRoot = await realpath(resolve(packages));
  const sourceRoot = await realpath(repositoryRoot);
  if (consumerRoot === sourceRoot || consumerRoot.startsWith(`${sourceRoot}${sep}`)) {
    throw new Error("Installed consumer must be outside the source candidate.");
  }
  await auditSource();
  await assertInstalledPackages(consumerRoot);
  const namespaces = await installedNamespaces(consumerRoot);
  const pg = postgresUrl ? await postgresAdapter(pgClientModule, disposableUrl(postgresUrl)) : null;
  const result = await runSyntheticArchiveApplication({ ...namespaces, pg });
  const assignedTools = await runSyntheticToolHost({ modules: {
    contract: namespaces.agentPlatform.toolAssignment, execution: namespaces.agentPlatform.assignedExecution,
    journal: namespaces.agentPlatform.journal, reader: namespaces.agentPlatform.postgresReader,
  } });
  assert.equal(result.mode, pg ? "postgres" : "memory-only");
  return Object.freeze({ ...result, assignedTools });
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const options = parseOptions(process.argv.slice(2));
  const postgresUrl = options["--postgres-url-env"] ? process.env.STANDALONE_POSTGRES_URL : null;
  if (options["--pg-client-module"] && !postgresUrl) throw new Error("Missing disposable PostgreSQL URL.");
  const result = await verifyInstalled({
    packages: options["--packages"],
    pgClientModule: options["--pg-client-module"],
    postgresUrl,
  });
  writeSync(1, `${JSON.stringify(result)}\n`);
}
