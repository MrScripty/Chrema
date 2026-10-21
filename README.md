# Chrema

Compose agent workflows without losing the history behind their results.

Chrema provides two reusable JavaScript packages: **agent-platform** records workflows, assignments, tool activity and review; **content-ledger** tracks content versions, relationships, usage and delivery decisions. Use either independently, or combine them in your application.

You supply the workflow steps, executors, model adapters and policies. Chrema supplies the lifecycle, validation and records around them.

## Try it in a minute

Requires Node.js 22.12 or later:

```sh
git clone https://github.com/MrScripty/Chrema.git
cd Chrema
node examples/memory-workflow.mjs
```

The example runs two deterministic text-processing steps and prints their completed run, original input and trace. It needs no package installation, database, API key or model. Its in-memory state disappears when the process exits.

## Use the packages in an application

There is no npm registry release yet. Build and pack local archives from the checkout:

```sh
package_workspace=$(mktemp -d)
node scripts/build.mjs --out "$package_workspace/packages"
node scripts/audit.mjs --packages "$package_workspace/packages"
npm pack --offline --ignore-scripts --pack-destination "$package_workspace" "$package_workspace/packages/agent-platform"
npm pack --offline --ignore-scripts --pack-destination "$package_workspace" "$package_workspace/packages/content-ledger"
```

Install the archive you need from your application's directory:

```sh
npm install /path/to/agent-platform-0.0.0.tgz
# Or: npm install /path/to/content-ledger-0.0.0.tgz
```

For example:

```js
import { createAgentWorkflowJournalFactory } from 'agent-platform/journal';
import { normalizeContentVersion } from 'content-ledger/capture';
```

Both packages have no external npm runtime dependencies and do not import each other. See the [API guide](docs/api.md) for entry points and host contracts.

## Add durable storage when you need it

The workflow journal supports memory and PostgreSQL. Durable checkpoints and the content ledger's persistence implementation use PostgreSQL; applications supply the database, driver and access policy. Installing Chrema does not start a database or apply SQL. Read [storage requirements](docs/storage.md) before enabling persistence.

## Develop

```sh
node scripts/audit.mjs
node --test tests/*.test.mjs
```

[Development guide](docs/development.md) covers packaging, explicit source inventories and disposable PostgreSQL acceptance.

[Apache-2.0 License](LICENSE).

