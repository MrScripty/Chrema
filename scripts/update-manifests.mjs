#!/usr/bin/env node
import { lstat, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { repositoryRoot, safeRelative, sha256 } from "./lib/build-packages.mjs";

// Refresh reviewed entries only. New files must be named explicitly with --add.
const args = process.argv.slice(2);
if (args.length && (args[0] !== "--add" || args.length === 1)) {
  throw new Error("Usage: node scripts/update-manifests.mjs [--add <file> ...]");
}
async function bytes(path) {
  const parts = safeRelative(path, "Manifest path").split("/");
  for (let index = 1; index <= parts.length; index++) {
    const stat = await lstat(join(repositoryRoot, ...parts.slice(0, index)));
    if (stat.isSymbolicLink() || (index < parts.length ? !stat.isDirectory() : !stat.isFile())) {
      throw new Error(`Expected a regular reviewed file: ${path}`);
    }
  }
  return readFile(join(repositoryRoot, path));
}
const packagePath = "packaging/manifest.json";
const sourcePath = "packaging/source-manifest.json";
const packages = JSON.parse(await bytes(packagePath));
const source = JSON.parse(await bytes(sourcePath));
for (const path of args.slice(1)) {
  safeRelative(path, "Added source");
  if (path === sourcePath || path.startsWith(".git/") || path.split("/").includes("node_modules")) {
    throw new Error(`Not a source inventory entry: ${path}`);
  }
  if (source.files.some(entry => entry.destination === path)) throw new Error(`Already listed: ${path}`);
  source.files.push({ source: path, destination: path, sha256: "" });
}
for (const spec of Object.values(packages.packages)) {
  spec.metadata.sha256 = sha256(await bytes(spec.metadata.path));
  for (const entry of spec.files) entry.sha256 = sha256(await bytes(entry.source));
}
const encodedPackages = Buffer.from(`${JSON.stringify(packages, null, 2)}\n`);
for (const entry of source.files) {
  if (entry.source !== entry.destination) throw new Error("Source inventory paths must match.");
  entry.sha256 = sha256(entry.destination === packagePath ? encodedPackages : await bytes(entry.destination));
}
source.files.sort((a, b) => a.destination.localeCompare(b.destination));
// Resolve every input before replacing either manifest. Auditing remains a separate step.
await writeFile(join(repositoryRoot, packagePath), encodedPackages);
await writeFile(join(repositoryRoot, sourcePath), `${JSON.stringify(source, null, 2)}\n`);
console.log(`Refreshed ${source.files.length} reviewed source entries and both package manifests.`);
