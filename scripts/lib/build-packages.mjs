#!/usr/bin/env node
import { createHash } from "node:crypto";
import { lstat, mkdir, readFile, realpath, writeFile } from "node:fs/promises";
import { basename, dirname, join, relative, resolve, sep, posix } from "node:path";
import { fileURLToPath } from "node:url";

export const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const packageNames = ["agent-platform", "content-ledger"];
const builtinNames = new Set([
  "crypto", "dns/promises", "fs/promises", "http", "https", "net", "perf_hooks", "tls", "util", "zlib",
]);
const sourceImport = /\b(?:import|export)\s+(?:(?:[\s\S]*?)\s+from\s*)?["']([^"']+)["']/g;
const dynamicImportCall = /\bimport\b(?:(?:\s+)|(?:\/\*[\s\S]*?\*\/)|(?:\/\/[^\r\n\u2028\u2029]*(?:\r\n?|\n|\u2028|\u2029|$)))*\(([^)]*)\)/g;
const sqlAsset = /new\s+URL\s*\(\s*["'](\.{1,2}\/[^"']+)["']\s*,\s*import\.meta\.url\s*\)/g;

export function sha256(bytes) { return createHash("sha256").update(bytes).digest("hex"); }

export function safeRelative(value, label) {
  if (typeof value !== "string" || !value || value.includes("\\") || value.startsWith("/")
    || value.split("/").some(part => !part || part === "." || part === "..")
    || value.includes("\0")) throw new Error(`${label} must be a clean relative POSIX path.`);
  return value;
}

async function regularFileWithoutSymlink(root, path) {
  const parts = safeRelative(path, "File path").split("/");
  let cursor = root;
  for (let index = 0; index < parts.length; index++) {
    cursor = join(cursor, parts[index]);
    const stat = await lstat(cursor);
    if (stat.isSymbolicLink() || (index < parts.length - 1 && !stat.isDirectory())
      || (index === parts.length - 1 && !stat.isFile())) {
      throw new Error(`Expected a regular file without symlinks: ${path}`);
    }
  }
  return readFile(cursor);
}

function assertKeys(value, keys, label) {
  if (!value || typeof value !== "object" || Array.isArray(value)
    || Object.keys(value).sort().join("|") !== [...keys].sort().join("|")) {
    throw new Error(`${label} has unexpected fields.`);
  }
}

function sourceSymbols(source) {
  const symbols = new Set();
  for (const match of source.matchAll(/\bexport\s+(?:(?:async|const|let|var|function|class)\s+)+(\w+)/g)) symbols.add(match[1]);
  for (const match of source.matchAll(/\bexport\s*\{([^}]+)\}/g)) {
    for (const item of match[1].split(",")) {
      const name = item.trim().split(/\s+as\s+/).at(-1)?.trim();
      if (name) symbols.add(name);
    }
  }
  return symbols;
}

function referencedPaths(source, label) {
  if (/\bcreateRequire\b|\brequire\s*(?:\?\.|\.)?\s*\(|\brequire\s*\.\s*(?:resolve|cache|extensions|main)\b|\b(?:module|globalThis|process\s*\.\s*mainModule)\s*(?:\.\s*require|\[\s*["']require["']\s*\])/.test(source)) {
    throw new Error(`${label} contains a CommonJS loader escape.`);
  }
  const imports = [];
  for (const match of source.matchAll(sourceImport)) imports.push(match[1]);
  for (const match of source.matchAll(dynamicImportCall)) {
    const literal = /^\s*(["'])([^"'\\]*)\1\s*$/.exec(match[1]);
    if (!literal) throw new Error(`${label} has a nonliteral dynamic import.`);
    imports.push(literal[2]);
  }
  const assets = [...source.matchAll(sqlAsset)].map(match => match[1]);
  return { imports, assets };
}

function assertClosure(packageName, files, exports) {
  const byDestination = new Map(files.map(file => [file.destination, file]));
  const edges = new Map();
  for (const file of files) {
    if (!file.destination.endsWith(".mjs")) continue;
    const source = file.bytes.toString("utf8");
    const { imports, assets } = referencedPaths(source, file.source);
    const dependencies = new Set();
    for (const dependency of imports) {
      if (dependency.startsWith("node:")) {
        if (!builtinNames.has(dependency.slice(5))) throw new Error(`Unknown built-in ${dependency} in ${file.source}`);
      } else if (dependency.startsWith("./") || dependency.startsWith("../")) {
        const target = posix.normalize(posix.join(posix.dirname(file.destination), dependency));
        if (target.startsWith("../") || !byDestination.has(target)) throw new Error(`Undeclared import ${dependency} in ${file.source}`);
        dependencies.add(target);
      } else throw new Error(`External dependency ${dependency} in ${file.source}`);
    }
    for (const asset of assets) {
      const target = posix.normalize(posix.join(posix.dirname(file.destination), asset));
      if (!byDestination.has(target)) throw new Error(`Undeclared asset ${asset} in ${file.source}`);
      dependencies.add(target);
    }
    edges.set(file.destination, dependencies);
  }
  const roots = new Set();
  for (const [subpath, entries] of Object.entries(exports)) {
    if (!/^\.\/[a-z][a-z0-9-]*$/.test(subpath) || !Array.isArray(entries) || !entries.length) {
      throw new Error(`Invalid export ${packageName}:${subpath}`);
    }
    const names = new Set();
    for (const entry of entries) {
      assertKeys(entry, ["module", "names"], `${subpath} entry`);
      const module = byDestination.get(safeRelative(entry.module, "Export module"));
      if (!module?.destination.endsWith(".mjs") || !Array.isArray(entry.names) || !entry.names.length) {
        throw new Error(`Invalid export module ${entry.module}`);
      }
      roots.add(module.destination);
      const available = sourceSymbols(module.bytes.toString("utf8"));
      for (const name of entry.names) {
        if (typeof name !== "string" || !/^[A-Za-z_$][\w$]*$/.test(name)
          || !available.has(name) || names.has(name)) throw new Error(`Invalid or duplicate export ${name} in ${subpath}`);
        names.add(name);
      }
    }
  }
  const reachable = new Set();
  const pending = [...roots];
  while (pending.length) {
    const path = pending.pop();
    if (reachable.has(path)) continue;
    reachable.add(path);
    for (const dependency of edges.get(path) ?? []) pending.push(dependency);
  }
  for (const { destination } of files) {
    if (destination !== "README.md" && destination !== "LICENSE" && !reachable.has(destination)) {
      throw new Error(`Unreachable package file ${packageName}:${destination}`);
    }
  }
}

export function barrelBytes(entries) {
  return Buffer.from(entries.map(({ module, names }) =>
    `export { ${names.join(", ")} } from "../${module}";\n`).join(""), "utf8");
}

export function packageJsonBytes(metadata, exports) {
  const paths = Object.keys(exports).sort();
  return Buffer.from(`${JSON.stringify({
    ...metadata,
    exports: Object.fromEntries(paths.map(path => [path, `./exports/${path.slice(2)}.mjs`])),
    files: ["src/", "schemas/", "exports/", "README.md", "LICENSE"],
  }, null, 2)}\n`, "utf8");
}

export async function loadManifest(manifestPath) {
  const absolute = resolve(manifestPath);
  if (relative(repositoryRoot, absolute).startsWith("..") || !(await realpath(absolute)).startsWith(`${repositoryRoot}${sep}`)) {
    throw new Error("Manifest must be inside the repository.");
  }
  const manifest = JSON.parse(await regularFileWithoutSymlink(repositoryRoot,
    safeRelative(relative(repositoryRoot, absolute).split(sep).join("/"), "Manifest path")));
  assertKeys(manifest, ["version", "packages"], "Manifest");
  if (manifest.version !== 1 || Object.keys(manifest.packages ?? {}).sort().join("|") !== packageNames.join("|")) {
    throw new Error("Unsupported standalone manifest.");
  }
  const loaded = {};
  for (const packageName of packageNames) {
    const spec = manifest.packages[packageName];
    assertKeys(spec, ["metadata", "files", "exports"], packageName);
    assertKeys(spec.metadata, ["path", "sha256"], `${packageName} metadata reference`);
    const metadataPath = safeRelative(spec.metadata.path, "Metadata path");
    const metadataBytes = await regularFileWithoutSymlink(repositoryRoot, metadataPath);
    if (sha256(metadataBytes) !== spec.metadata.sha256) throw new Error(`Metadata hash mismatch: ${metadataPath}`);
    const metadata = JSON.parse(metadataBytes);
    assertKeys(metadata, ["name", "version", "description", "type", "private", "license", "engines", "dependencies"], `${packageName} metadata`);
    if (metadata.name !== packageName || metadata.type !== "module" || metadata.private !== true
      || metadata.license !== "Apache-2.0" || Object.keys(metadata.dependencies ?? {}).length
      || typeof metadata.engines?.node !== "string") throw new Error(`Unsafe metadata for ${packageName}`);
    if (!Array.isArray(spec.files) || !spec.files.length) throw new Error(`Empty file list for ${packageName}`);
    const seenSources = new Set(); const seenDestinations = new Set();
    const files = [];
    for (const entry of spec.files) {
      assertKeys(entry, ["source", "destination", "sha256"], `${packageName} file`);
      const source = safeRelative(entry.source, "Source path");
      const destination = safeRelative(entry.destination, "Destination path");
      if (destination === "package.json" || destination.startsWith("exports/")
        || seenSources.has(source) || seenDestinations.has(destination)) throw new Error(`Duplicate or reserved path: ${destination}`);
      seenSources.add(source); seenDestinations.add(destination);
      const bytes = await regularFileWithoutSymlink(repositoryRoot, source);
      if (sha256(bytes) !== entry.sha256) throw new Error(`Source hash mismatch: ${source}`);
      files.push({ source, destination, bytes });
    }
    assertClosure(packageName, files, spec.exports);
    loaded[packageName] = { metadata, files, exports: spec.exports };
  }
  return loaded;
}

export async function buildStandalone({ manifest, out }) {
  const loaded = await loadManifest(manifest);
  const output = resolve(out);
  const sourceRoot = await realpath(repositoryRoot);
  const outputParent = await realpath(dirname(output));
  const actualOutput = resolve(outputParent, basename(output));
  if (actualOutput === sourceRoot || actualOutput.startsWith(`${sourceRoot}${sep}`)) {
    throw new Error("Output must be outside the source repository.");
  }
  await mkdir(output, { recursive: false });
  for (const packageName of packageNames) {
    const spec = loaded[packageName];
    const root = join(output, packageName);
    await mkdir(root);
    for (const { destination, bytes } of spec.files) {
      await mkdir(dirname(join(root, destination)), { recursive: true });
      await writeFile(join(root, destination), bytes, { flag: "wx" });
    }
    await mkdir(join(root, "exports"));
    for (const [subpath, entries] of Object.entries(spec.exports)) {
      await writeFile(join(root, "exports", `${subpath.slice(2)}.mjs`), barrelBytes(entries), { flag: "wx" });
    }
    await writeFile(join(root, "package.json"), packageJsonBytes(spec.metadata, spec.exports), { flag: "wx" });
  }
  return output;
}

export function parseOptions(argv, required) {
  const options = {};
  for (let index = 0; index < argv.length; index += 2) {
    const key = argv[index];
    if (!key?.startsWith("--") || !required.includes(key.slice(2)) || !argv[index + 1] || options[key.slice(2)]) {
      throw new Error(`Expected ${required.map(name => `--${name} <path>`).join(" ")}.`);
    }
    options[key.slice(2)] = argv[index + 1];
  }
  if (Object.keys(options).length !== required.length) throw new Error(`Expected ${required.map(name => `--${name} <path>`).join(" ")}.`);
  return options;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const { manifest, out } = parseOptions(process.argv.slice(2), ["manifest", "out"]);
    console.log(await buildStandalone({ manifest, out }));
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
