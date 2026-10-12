#!/usr/bin/env node
import { execFile as execFileCallback } from "node:child_process";
import { createHash } from "node:crypto";
import { lstat, mkdtemp, readFile, readdir, rm } from "node:fs/promises";
import { dirname, join, relative, resolve, sep } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { barrelBytes, loadManifest, packageJsonBytes, repositoryRoot, safeRelative } from "../tools/build-standalone.mjs";

const execFile = promisify(execFileCallback);
const digest = bytes => createHash("sha256").update(bytes).digest("hex");
const secretPatterns = [
  /-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/,
  /\bAKIA[0-9A-Z]{16}\b/,
  /\bgh[opsu]_[A-Za-z0-9_]{30,}\b/,
  /\bsk-[A-Za-z0-9_-]{16,}\b/,
  /\bpostgres(?:ql)?:\/\/[^\s:/]+:[^\s@/]+@/i,
  /\bBearer\s+\S+/i,
  /\b(?:password|secret|api[_-]?key|access[_-]?token)\s*[:=]\s*["']?[^\s"',;}]{12,}/i,
];

async function walk(root, prefix = "") {
  const rootStat = await lstat(join(root, prefix));
  if (!rootStat.isDirectory() || rootStat.isSymbolicLink()) {
    throw new Error(`Source or package root is not a real directory: ${prefix || root}`);
  }
  const files = [];
  const directories = [];
  for (const entry of await readdir(join(root, prefix), { withFileTypes: true })) {
    // Git metadata belongs only at the source root; payload and package trees stay exact.
    if (root === repositoryRoot && prefix === "" && entry.name === ".git") {
      const gitStat = await lstat(join(root, entry.name));
      if (!gitStat.isDirectory() || gitStat.isSymbolicLink()) throw new Error("Invalid source Git directory.");
      continue;
    }
    const path = prefix ? `${prefix}/${entry.name}` : entry.name;
    const stat = await lstat(join(root, path));
    if (stat.isSymbolicLink() || (!stat.isFile() && !stat.isDirectory())) {
      throw new Error(`Non-regular or symlink source entry: ${path}`);
    }
    if (stat.isDirectory()) {
      directories.push(path);
      const nested = await walk(root, path);
      directories.push(...nested.directories);
      files.push(...nested.files);
    } else files.push(path);
  }
  return { files: files.sort(), directories: directories.sort() };
}

function requiredDirectories(files) {
  const directories = new Set();
  for (const path of files) {
    const parts = path.split("/");
    for (let index = 1; index < parts.length; index++) {
      directories.add(parts.slice(0, index).join("/"));
    }
  }
  return directories;
}

function exactPaths(actual, expected, label) {
  if (actual.length !== expected.size || actual.some(path => !expected.has(path))) {
    throw new Error(`${label} contains missing or unexpected entries.`);
  }
}

function assertSecrets(path, bytes) {
  const value = bytes.toString("utf8");
  if (secretPatterns.some(pattern => pattern.test(value))) throw new Error(`Secret pattern in ${path}`);
}

function assertStaticClosure(paths, destination, bytes) {
  if (!destination.endsWith(".mjs") || destination === "tools/build-standalone.mjs") return;
  const source = bytes.toString("utf8");
  const imports = [...source.matchAll(/^\s*(?:import|export)\s+(?:[^;\n]*?\s+from\s+)?["']([^"']+)["']/gm)]
    .map(match => match[1]);
  for (const specifier of imports) {
    if (specifier.startsWith("node:")) continue;
    if (!specifier.startsWith("./") && !specifier.startsWith("../")) {
      throw new Error(`External source import in ${destination}: ${specifier}`);
    }
    const target = resolve(repositoryRoot, dirname(destination), specifier);
    const rel = relative(repositoryRoot, target).split(sep).join("/");
    if (!rel || rel.startsWith("../") || !paths.has(rel)) {
      throw new Error(`Source import escapes allowlist in ${destination}: ${specifier}`);
    }
  }
}

async function assertPacked(packageRoot, name, expected) {
  const cache = await mkdtemp(join(tmpdir(), "standalone-candidate-pack-"));
  try {
    const metadata = JSON.parse(await readFile(join(packageRoot, "package.json"), "utf8"));
    await execFile("npm", ["pack", "--ignore-scripts", "--offline", "--cache", cache, "--pack-destination", cache],
      { cwd: packageRoot, maxBuffer: 10 * 1024 * 1024 });
    const archive = join(cache, `${metadata.name}-${metadata.version}.tgz`);
    const { stdout } = await execFile("tar", ["-tzf", archive], { maxBuffer: 10 * 1024 * 1024 });
    const packedPaths = stdout.trimEnd().split("\n");
    if (packedPaths.length !== expected.size || new Set(packedPaths).size !== expected.size) {
      throw new Error(`Packed file count mismatch: ${name}`);
    }
    for (const entry of packedPaths) {
      if (!entry.startsWith("package/") || !expected.has(entry.slice("package/".length))) {
        throw new Error(`Unexpected packed path: ${name}/${entry}`);
      }
      const path = entry.slice("package/".length);
      const { stdout: bytes } = await execFile("tar", ["-xOzf", archive, entry],
        { encoding: "buffer", maxBuffer: 10 * 1024 * 1024 });
      if (digest(bytes) !== digest(expected.get(path))) throw new Error(`Packed byte mismatch: ${name}/${path}`);
      assertSecrets(`${name}.tgz/${path}`, bytes);
    }
  } finally {
    await rm(cache, { recursive: true, force: true });
  }
}

export async function auditSource({ packages = null } = {}) {
  const sourceManifest = JSON.parse(await readFile(join(repositoryRoot, "SOURCE-MANIFEST.json"), "utf8"));
  if (sourceManifest.version !== 1 || !Array.isArray(sourceManifest.files)) {
    throw new Error("Unsupported source manifest.");
  }
  const expected = new Map();
  for (const entry of sourceManifest.files) {
    if (Object.keys(entry).sort().join("|") !== "destination|sha256|source"
      || !/^[0-9a-f]{64}$/.test(entry.sha256)) throw new Error("Malformed source entry.");
    const destination = safeRelative(entry.destination, "Destination");
    safeRelative(entry.source, "Source");
    if (expected.has(destination)) throw new Error(`Duplicate source destination: ${destination}`);
    expected.set(destination, entry.sha256);
  }
  const sourceTree = await walk(repositoryRoot);
  const paths = sourceTree.files;
  const allowed = new Set([...expected.keys(), "SOURCE-MANIFEST.json"]);
  exactPaths(paths, allowed, "Source candidate");
  exactPaths(sourceTree.directories, requiredDirectories(allowed), "Source candidate directories");
  for (const [destination, sha256] of expected) {
    const bytes = await readFile(join(repositoryRoot, destination));
    if (digest(bytes) !== sha256) throw new Error(`Source hash mismatch: ${destination}`);
    assertSecrets(destination, bytes);
    assertStaticClosure(allowed, destination, bytes);
  }
  assertSecrets("SOURCE-MANIFEST.json", await readFile(join(repositoryRoot, "SOURCE-MANIFEST.json")));
  const packageManifest = JSON.parse(await readFile(join(repositoryRoot, "standalone/manifest.json"), "utf8"));
  const loaded = await loadManifest(join(repositoryRoot, "standalone/manifest.json"));
  if (packages) {
    const packageRoot = resolve(packages);
    if (packageRoot === repositoryRoot || packageRoot.startsWith(`${repositoryRoot}${sep}`)) {
      throw new Error("Generated packages must be outside the source candidate.");
    }
    const names = ["agent-platform", "content-ledger"];
    await walk(packageRoot);
    const top = await readdir(packageRoot);
    exactPaths(top, new Set(names), "Package root entries");
    for (const name of names) {
      const spec = loaded[name];
      const expectedPackage = new Map(spec.files.map(file => [file.destination, file.bytes]));
      expectedPackage.set("package.json", packageJsonBytes(spec.metadata, packageManifest.packages[name].exports));
      for (const [subpath, entries] of Object.entries(packageManifest.packages[name].exports)) {
        expectedPackage.set(`exports/${subpath.slice(2)}.mjs`, barrelBytes(entries));
      }
      const actualTree = await walk(join(packageRoot, name));
      exactPaths(actualTree.files, new Set(expectedPackage.keys()), `Generated package ${name}`);
      exactPaths(actualTree.directories, requiredDirectories(expectedPackage.keys()),
        `Generated package ${name} directories`);
      for (const [path, bytes] of expectedPackage) {
        const copied = await readFile(join(packageRoot, name, path));
        if (digest(copied) !== digest(bytes)) throw new Error(`Changed generated package: ${name}/${path}`);
        assertSecrets(`${name}/${path}`, copied);
      }
      await assertPacked(join(packageRoot, name), name, expectedPackage);
    }
  }
  return Object.freeze({ sourceFiles: paths.length, packages: packages ? 2 : 0 });
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const args = process.argv.slice(2);
  if (args.length && (args.length !== 2 || args[0] !== "--packages" || !args[1])) {
    throw new Error("Expected optional --packages <generated-package-root>.");
  }
  console.log(JSON.stringify(await auditSource({ packages: args[1] ?? null })));
}
