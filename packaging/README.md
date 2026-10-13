# Chrema packages

Chrema provides reusable components for composing agent workflows, observing
their execution and retaining the information they produce or use.

- `agent-platform` provides workflow journals, assignment and worker lifecycles,
  tool binding, validation, review and admission components. Applications supply
  workflow definitions, executors and policy.
- `content-ledger` provides content versions, observed usage, relationships,
  assessments and controlled transfer/delivery records. Its persistence uses
  host-supplied PostgreSQL and compatible journal relations.

The agent journal can run in memory without a database. Durable persistence and
optional checkpoint/continuation protocols require PostgreSQL. Neither package
contains a database server, driver, provider credentials or application data.
The packages have no external runtime dependencies or dependency on each other.

Use the named exports, for example `agent-platform/journal`,
`agent-platform/assigned-execution-v1` or `content-ledger/capture`.
Internal `src/` and `schemas/` paths are implementation details; use exported
schema URL getters and bootstrap functions when provided.

For installation, runnable examples, public entry points and storage requirements,
see the [Chrema repository](https://github.com/MrScripty/Chrema).
Requires Node.js >=22.12.0. Licensed under Apache-2.0; see the included `LICENSE`.
These local packages retain `private: true`; no npm registry release is configured.
