# Chrema

Chrema is a set of reusable components for **composing agent workflows, observing
their execution, and retaining the information they produce or use**. Combine
workflow steps, tools and host-provided executors while keeping a record of what
ran, which inputs it used, and how results were reviewed or admitted.

Your application supplies the workflow definitions, model/provider adapters,
prompts and policies. Chrema supplies the journals, lifecycle and validation
components around them; it is not a ready-made chatbot or a hosted service.

## Two packages, one repository

| Package | What it provides |
| --- | --- |
| `agent-platform` | Workflow versions, runs, assignments, attempts and events; worker and tool lifecycles; staging, validation, review and admission |
| `content-ledger` | Content versions and observed usage; relationships and assessments; policy-driven transfer decisions and delivery records |

Use either package independently, or compose them in your application. They have
no external npm runtime dependencies and do not import each other. The ledger's
capture schema expects compatible journal relations supplied by the host.

## Try it without a database

Requires **Node.js 22.12 or later**. From a source checkout:

```sh
git clone https://github.com/MrScripty/Chrema.git
cd Chrema
node examples/memory-workflow.mjs
```

The example composes two deterministic text-processing components, records their
assignments and input/output events, and prints the completed run with its trace.
It needs no API key, model provider, package installation or database. It retains
the original input and intermediate results in memory for inspection; that state
disappears when the process exits. Read the [example](examples/memory-workflow.mjs)
to see where your own components would plug in.

## When is PostgreSQL needed?

The agent journal supports memory and PostgreSQL. Durable storage and optional
checkpoint/continuation protocols use PostgreSQL. The content ledger's
persistence implementation also uses PostgreSQL; its pure validation and identity
helpers can be used without it. Applications supply the database, driver and
access policy.

The SQL under `schemas/` defines tables, validation functions and explicit
upgrades. It is not a database dump. Downloading or installing Chrema does not
start a database or run those files. See [storage requirements](docs/storage.md).

## Use the packages in an application

There is no npm registry release yet. Build local packages from this checkout:

```sh
package_workspace=$(mktemp -d)
node scripts/build.mjs --out "$package_workspace/packages"
node scripts/audit.mjs --packages "$package_workspace/packages"
npm pack --offline --ignore-scripts --pack-destination "$package_workspace" "$package_workspace/packages/agent-platform"
npm pack --offline --ignore-scripts --pack-destination "$package_workspace" "$package_workspace/packages/content-ledger"
```

Then, from your application's directory, install either or both generated
archives with `npm install /path/to/agent-platform-0.0.0.tgz` and
`npm install /path/to/content-ledger-0.0.0.tgz`. The commands above print their
locations. Import the package entry points, for example:

```js
import { createAgentWorkflowJournalFactory } from 'agent-platform/journal';
import { normalizeContentVersion } from 'content-ledger/capture';
```

See the [API guide](docs/api.md) for host contracts and all 31 public entry points,
and [development](docs/development.md) for tests, package verification and
PostgreSQL acceptance.

## Repository layout

```text
src/          Agent platform, content ledger and shared implementation modules
schemas/      PostgreSQL definitions grouped by the two components
examples/     Small runnable examples
tests/        Package compatibility tests and synthetic acceptance fixtures
docs/         API, storage and contributor documentation
packaging/    Package metadata and explicit source/asset inventories
scripts/      Build, audit and inventory maintenance tools
```

Public package imports stay stable when internal files move. Apache-2.0; see
[LICENSE](LICENSE).
