import assert from "node:assert/strict";
import { execFile as execFileCallback } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, relative } from "node:path";
import { promisify } from "node:util";
import test from "node:test";
import { auditSource } from "../scripts/audit.mjs";
import { buildStandalone, repositoryRoot } from "../scripts/lib/build-packages.mjs";
import { verifyInstalled } from "./acceptance.mjs";

const execFile = promisify(execFileCallback);
const json = async path => JSON.parse(await readFile(path, "utf8"));

test("the README memory example retains original input, ordered results and an observable trace", async () => {
  const { stdout } = await execFile(process.execPath, ["examples/memory-workflow.mjs"], { cwd: repositoryRoot });
  const result = JSON.parse(stdout);
  assert.equal(result.status, "completed");
  assert.equal(result.originalInput.text, "  Keep   useful information.  ");
  assert.deepEqual(result.results.map(step => step.step), ["normalize", "summarize"]);
  assert.deepEqual(result.results[1].output, { text: "Keep useful information.", wordCount: 3 });
  assert.equal(result.trace[0].input, result.originalInput.text);
  assert.equal(result.trace[1].input, result.results[0].output);
  assert.equal(result.metrics.completedSteps, 2);
});

test("nested packages preserve public imports, SQL assets, isolated installs and memory behavior", { timeout: 180_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), "chrema-package-test-"));
  try {
    const home = join(root, "home"), archives = join(root, "archives");
    await mkdir(home); await mkdir(archives);
    const userConfig = join(root, "user.npmrc"), globalConfig = join(root, "global.npmrc");
    await writeFile(userConfig, ""); await writeFile(globalConfig, "");
    const env = { PATH: process.env.PATH, HOME: home, LANG: "C", LC_ALL: "C",
      NPM_CONFIG_USERCONFIG: userConfig, NPM_CONFIG_GLOBALCONFIG: globalConfig,
      NPM_CONFIG_UPDATE_NOTIFIER: "false", NPM_CONFIG_FUND: "false", NPM_CONFIG_AUDIT: "false" };
    const run = (command, args, cwd = root) => execFile(command, args,
      { cwd, env, timeout: 120_000, maxBuffer: 8 * 1024 * 1024 });
    const baseline = await json(join(repositoryRoot, "tests/fixtures/public-exports.json"));
    const sqlHashes = await json(join(repositoryRoot, "tests/fixtures/schema-hashes.json"));
    const manifest = await json(join(repositoryRoot, "packaging/manifest.json"));
    const built = join(root, "packages");
    await buildStandalone({ manifest: join(repositoryRoot, "packaging/manifest.json"), out: built });
    await auditSource({ packages: built });
    const tarballs = {};
    for (const name of Object.keys(baseline)) {
      const spec = manifest.packages[name];
      assert.deepEqual(Object.fromEntries(Object.entries(spec.exports).map(([subpath, entries]) =>
        [subpath, entries.flatMap(entry => entry.names).sort()])), baseline[name]);
      const packed = JSON.parse((await run("npm", ["pack", "--offline", "--ignore-scripts", "--json",
        "--cache", join(root, "pack-cache"), "--pack-destination", archives, join(built, name)])).stdout)[0];
      tarballs[name] = join(archives, packed.filename);
      assert.ok(packed.files.some(file => file.path.startsWith("src/")));
      assert.ok(packed.files.some(file => file.path.startsWith("schemas/")));
      assert.ok(!packed.files.some(file => /^(tests|examples|packaging)\//.test(file.path)));
    }
    for (const names of [["agent-platform"], ["content-ledger"], ["agent-platform", "content-ledger"]]) {
      const consumer = join(root, `consumer-${names.join("-")}`);
      await mkdir(consumer);
      await writeFile(join(consumer, "package.json"), JSON.stringify({ name: "chrema-example-consumer",
        private: true, type: "module", dependencies: Object.fromEntries(names.map(name =>
          [name, `file:${relative(consumer, tarballs[name])}`])) }));
      const cache = join(consumer, "npm-cache");
      await run("npm", ["install", "--offline", "--ignore-scripts", "--no-audit", "--no-fund", "--cache", cache], consumer);
      await run("npm", ["ci", "--offline", "--ignore-scripts", "--no-audit", "--no-fund", "--cache", cache], consumer);
      const check = `
        import assert from 'node:assert/strict';
        import { readFile } from 'node:fs/promises';
        import { createHash } from 'node:crypto';
        import { resolve } from 'node:path';
        import { fileURLToPath } from 'node:url';
        const baseline = ${JSON.stringify(baseline)};
        const hashes = ${JSON.stringify(sqlHashes)};
        const names = ${JSON.stringify(names)};
        const digest = bytes => createHash('sha256').update(bytes).digest('hex');
        for (const name of names) {
          const metadata = JSON.parse(await readFile('node_modules/' + name + '/package.json'));
          assert.deepEqual(Object.keys(metadata.exports).sort(), Object.keys(baseline[name]).sort());
          assert.deepEqual(metadata.dependencies, {});
          assert.equal(metadata.license, 'Apache-2.0');
          const namespaces = {};
          for (const [subpath, symbols] of Object.entries(baseline[name])) {
            const namespace = await import(name + '/' + subpath.slice(2));
            assert.deepEqual(Object.keys(namespace).sort(), symbols);
            namespaces[subpath] = namespace;
          }
          const denied = name + '/' + (name === 'agent-platform'
            ? 'src/agent-platform/agent-platform-journal.mjs' : 'src/content-ledger/content-ledger-contract.mjs');
          await assert.rejects(import(denied), { code: 'ERR_PACKAGE_PATH_NOT_EXPORTED' });
          for (const [path, hash] of Object.entries(hashes)) {
            if (path.startsWith('schemas/' + name + '/')) {
              assert.equal(digest(await readFile('node_modules/' + name + '/' + path)), hash);
            }
          }
          if (name === 'agent-platform') {
            for (const [subpath, namespace] of Object.entries(namespaces)) {
              for (const [symbol, getter] of Object.entries(namespace)) {
                if (!/^get.*SchemaUrl$/.test(symbol)) continue;
                const options = symbol === 'getSavedRootInitializationSchemaUrl'
                  ? [{ mode: 'saved-root-initialization.v1' }, { mode: 'saved-root-initialization.v2' }] : [undefined];
                for (const option of options) {
                  const url = getter(option), pathname = fileURLToPath(url);
                  const schemaRoot = resolve('node_modules/' + name + '/schemas/') + '/';
                  assert.ok(pathname.startsWith(schemaRoot), subpath + ':' + symbol);
                  const key = 'schemas/' + pathname.slice(schemaRoot.length);
                  assert.equal(digest(await readFile(url)), hashes[key]);
                }
              }
            }
          }
        }
      `;
      await run(process.execPath, ["--input-type=module", "--eval", check], consumer);
      if (names.length === 2) {
        const acceptance = await verifyInstalled({ packages: consumer });
        assert.equal(acceptance.mode, "memory-only");
        assert.equal(acceptance.boundedAnalysis.status, "completed");
        assert.equal(acceptance.assignedTools.realModelCalls, 0);
      }
    }
    await writeFile(join(built, "agent-platform", "unexpected.txt"), "Unexpected package file");
    await assert.rejects(auditSource({ packages: built }), /unexpected/i);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("public documentation links resolve within the repository", async () => {
  for (const file of ["README.md", "docs/api.md", "docs/storage.md", "docs/development.md"]) {
    const text = await readFile(join(repositoryRoot, file), "utf8");
    for (const [, target] of text.matchAll(/\]\(([^)]+)\)/g)) {
      if (/^(https?:|#)/.test(target)) continue;
      const path = new URL(target.split("#")[0], new URL(file, `file://${repositoryRoot}/`));
      await readFile(path);
    }
  }
});
