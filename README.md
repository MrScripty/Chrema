# Chrema

Chrema contains two independently bounded ESM packages: `agent-platform` for
agent workflows and `content-ledger` for PostgreSQL content records, assessments,
transfers and delivery. It also includes an independent synthetic archive application. The package
manifest and source manifest pin every copied source and SQL file by SHA-256.

The following commands use fresh directories outside this source tree:

```sh
standalone_source=/absolute/path/to/exported-source
standalone_built=/absolute/path/to/fresh-built-root
standalone_workspace=/absolute/path/to/fresh-install-workspace
standalone_archives="$standalone_workspace/archives"
standalone_consumer="$standalone_workspace/consumer"
standalone_cache=/absolute/path/to/fresh-npm-cache
mkdir -p "$standalone_archives" "$standalone_consumer" "$standalone_cache"
node "$standalone_source/scripts/audit.mjs"
node "$standalone_source/scripts/build.mjs" --out "$standalone_built"
node "$standalone_source/scripts/audit.mjs" --packages "$standalone_built"
npm pack --offline --ignore-scripts --cache "$standalone_cache" --pack-destination "$standalone_archives" "$standalone_built/agent-platform"
npm pack --offline --ignore-scripts --cache "$standalone_cache" --pack-destination "$standalone_archives" "$standalone_built/content-ledger"
printf '%s\n' '{"name":"synthetic-archive-consumer","private":true,"type":"module","dependencies":{"agent-platform":"file:../archives/agent-platform-0.0.0.tgz","content-ledger":"file:../archives/content-ledger-0.0.0.tgz"}}' > "$standalone_consumer/package.json"
cd "$standalone_consumer"
npm install --offline --ignore-scripts --no-audit --no-fund --cache "$standalone_cache"
npm ls --depth=0
npm ci --offline --ignore-scripts --no-audit --no-fund --cache "$standalone_cache"
node "$standalone_source/scripts/verify.mjs" --packages "$standalone_consumer"
```

Keep the archive and consumer directories as siblings so the relative `file:`
dependencies work from either location. The lockfile records those archives;
keep them available when replaying it with `npm ci`.

PostgreSQL acceptance uses the same verifier with
`--pg-client-module <absolute-host-driver-entry-file>`
`--postgres-url-env STANDALONE_POSTGRES_URL` and a validated, short-lived disposable local
socket URL in `STANDALONE_POSTGRES_URL`. The host supplies the driver and the
disposable database; the source candidate carries neither.

The package runtime dependencies are empty. The PostgreSQL driver belongs to
the host running the disposable acceptance database. The source and both packages use the Apache License, Version 2.0; see `LICENSE`.
Package metadata retains `private: true` because no npm publication is configured.
The packages have no runtime dependency on each other. Ledger capture schemas
require host-compatible journal relations; applications own that composition.
