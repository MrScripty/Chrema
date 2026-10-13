# Memory and PostgreSQL

Chrema ships storage interfaces and SQL definitions. It does not include a
database server, database driver, database dump or application data. Installing
either package does not connect to a database or apply a migration.

| Component | Without a database | With host-supplied PostgreSQL |
| --- | --- | --- |
| Agent workflow journal | Runs, assignments, events and results remain in process memory | Durable journal and transaction-bound application hooks |
| Optional checkpoint/continuation protocols | Not supported by the memory journal | Explicitly installed, versioned protocol tables |
| Content ledger | Pure contracts, validation and identity helpers | Content versions, usage, relationships, assessments, transfers and delivery records |

The journal factory selects memory when no `database` is supplied. Memory state
does not survive a restart and is not shared between processes. A PostgreSQL
journal expects a host adapter exposing `query` and `transaction`, validated
relation names, and host-owned policy callbacks. Neither package bundles `pg`.

The content ledger's persistence implementation requires PostgreSQL. Capture
schemas reference compatible journal relations supplied by the host; that host
can use the agent platform's journal. The packages do not import each other.
Your application decides what information to retain, who can read it, and which
assessments or policies permit a transfer. A captured record is not permission
to disclose or deliver it.

## Schema files

`schemas/agent-platform/` contains the base journal schema and optional checkpoint,
answer-finalization, continuation and initialization protocols.
`saved-root-initialization-v2.sql` is an explicit constraint upgrade from v1.
`schemas/content-ledger/` contains capture, validation, assessment and transfer
definitions. These files define tables, indexes, functions, constraints and
triggers; they contain no retained records.

Use the exported bootstrap functions for the base schemas. Optional protocol
modules expose schema URL getters for their versioned assets. SQL files retain
validated identifier placeholders; applications choose their schema names,
runtime/reader roles and installation sequence. Do not execute the templates
without that identifier validation. See [API contracts](api.md) for each seam.

The acceptance suite uses a disposable PostgreSQL instance, synthetic records
and temporary roles. It verifies cleanup as well as journal/ledger behavior.
See [development](development.md#postgresql-acceptance) for the host driver and
local socket requirements. No production database is needed for the quickstart.
