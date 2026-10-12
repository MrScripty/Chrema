#!/usr/bin/env node
import { resolve } from "node:path";
import { buildStandalone, repositoryRoot } from "../tools/build-standalone.mjs";

const args = process.argv.slice(2);
if (args.length !== 2 || args[0] !== "--out" || !args[1]) {
  throw new Error("Expected --out <fresh-directory-outside-this-tree>.");
}
const out = resolve(args[1]);
console.log(await buildStandalone({ manifest: resolve(repositoryRoot, "standalone/manifest.json"), out }));
