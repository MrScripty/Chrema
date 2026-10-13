# Development

Use Node.js >=22.12.0, npm and `tar`. The repository has no development or runtime
npm dependencies to install for its memory example and package tests.

```sh
node examples/memory-workflow.mjs
node scripts/audit.mjs
node --test tests/*.test.mjs
```

The package test builds in a fresh temporary directory, packs both archives,
installs each alone and together with offline caches, checks the original public
API contract, reads every optional schema URL, and runs memory acceptance. It
also verifies that unexpected files and imports are rejected. Temporary output
is removed after the test.

## Working on source and packages

Implementation modules live in `src/agent-platform`, `src/content-ledger` and
`src/shared`. The two packages copy their own complete dependency closures;
sharing a source helper does not introduce a dependency between the packages.
SQL definitions live in `schemas/agent-platform` and `schemas/content-ledger`.
The source and built packages use the same relative module/asset layout.

`packaging/manifest.json` declares the source files, metadata and exported symbols
for each package. `packaging/source-manifest.json` records the reviewed source
inventory. The builder copies source bytes and generates only package metadata
and export barrels. Audits check exact files, imports, SQL assets, hashes and
packed bytes. Build output belongs outside the source checkout.

After editing an already listed file, refresh its hashes explicitly:

```sh
node scripts/update-manifests.mjs
node scripts/audit.mjs
```

For a new source, documentation or test file, name it explicitly with
`node scripts/update-manifests.mjs --add path/to/file`. This does not add the file
to either package. Update the package manifest separately when a package needs a
new module or asset. When moving a file, update its explicit manifest paths and
relative imports before refreshing hashes. The refresh command never discovers
or admits arbitrary files, and the audit never refreshes hashes itself.

`tests/fixtures/public-exports.json` preserves the original 31 public entry points
and their exported names. `tests/fixtures/schema-hashes.json` preserves the 11
schema contents through this layout change. Update these contracts deliberately
when an API or schema change is intended; internal moves alone should not change
them. Direct imports into package internals are unsupported.

## PostgreSQL acceptance

A database is not needed for the commands above. For persistence acceptance,
supply the installed `pg` driver from a separate host environment and a disposable
local PostgreSQL instance. The driver and database are not bundled with Chrema.
The test creates synthetic schemas and roles and removes them afterward. The
supplied instance must permit that isolated setup; never point it at a production
database.

Build and install both archives into an external consumer directory, as in the
[root README](../README.md#use-the-packages-in-an-application). Then run:

```sh
export STANDALONE_POSTGRES_URL='postgresql:///chrema_test?host=/tmp/chrema-test/socket&port=5432&user=chrema_test_owner'
node tests/acceptance.mjs --packages /absolute/path/to/consumer \
  --pg-client-module /absolute/path/to/pg/lib/index.js \
  --postgres-url-env STANDALONE_POSTGRES_URL
```

This exact URL shape is a test guard: a local `/tmp/<name>/socket` Unix socket,
database name, port and role, with no network host or embedded credentials. The
host must start that disposable instance and provide the named role first.
The same verifier can run memory-only with just `--packages <consumer-root>`.
PostgreSQL 16.9 is the version used for the current acceptance checks; this is
not a claim that every PostgreSQL version is qualified.
