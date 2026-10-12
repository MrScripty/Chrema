# Package API and host contracts

Applications supply workflow definitions, model/provider adapters, access policies
and database clients. The public entry points below are stable across the internal
layout change. See [getting started](../README.md), [storage](storage.md) and
[development](development.md) before using the detailed contracts.

## Public entry points

### agent-platform

- `agent-platform/composed-workflow-draft` — `createComposedWorkflowDraftEditorFactory`
- `agent-platform/journal` — `agentWorkflowRunStatuses`, `createAgentWorkflowJournalFactory`, `normalizeModelSettings`, `normalizeUsage`, `versionIdentity`, `normalizeVersion`, `normalizePriceVersion`, `normalizeAttemptPrice`, `normalizeMoney`
- `agent-platform/bootstrap` — `bootstrapAgentPlatformPostgres`, `bootstrapAgentPlatformPostgresWithRunFamilies`
- `agent-platform/postgres-reader` — `createAgentPostgresReaderFactory`, `AgentPostgresQueryError`, `normalizeAgentPostgresQuery`
- `agent-platform/staging` — `createWorkflowStagingFactory`, `stagedValueHash`, `canonicalStagedJson`
- `agent-platform/revision-controller` — `createWorkflowRevisionControllerFactory`
- `agent-platform/manifest` — `createWorkflowManifestFactory`, `orderManifestCommands`
- `agent-platform/validation` — `createWorkflowValidationRecorder`, `describeWorkflowValidationGate`, `assertWorkflowResultIdentity`, `validateWorkflowValidationGate`
- `agent-platform/workflow-review` — `createWorkflowReviewFactory`
- `agent-platform/admission` — `createWorkflowAdmissionCommitter`, `retrySerializationConflicts`, `describeWorkflowAdmissionReceipt`, `validateWorkflowAdmissionReceipt`
- `agent-platform/assignment-lifecycle` — `createWorkflowAssignmentLifecycle`
- `agent-platform/bounded-analysis-worker` — `createBoundedAnalysisWorkerFactory`
- `agent-platform/worker-runtime` — `createWorkerRuntimeFactory`
- `agent-platform/https-fetch` — `fetchHttpsText`
- `agent-platform/tool-assignment-v1` — `createToolRegistration`, `resolveToolAssignments`
- `agent-platform/assigned-execution-v1` — `createAssignedToolAdapter`, `validateWorkflowToolAssignments`, `resolveWorkflowToolAssignment`, `assignedProviderQualificationRequest`, `assignedProviderCapabilityRequestV2`, `validateAssignedProviderQualification`, `renderPrompt`, `validatePromptStructure`, `workflowDefinitionHash`, `createStructuredWorkflowAgent`, `finalizeAcceptedStructuredWorkflowAttempt`
- `agent-platform/composition-checkpoint-records-v1` — `createCompositionCheckpointRecords`, `compositionCheckpointRecordHash`, `getCompositionCheckpointRecordsSchemaUrl`
- `agent-platform/reserved-answer-finalization-v1` — `createReservedAnswerFinalization`, `reservedAnswerFinalizationDescriptor`, `getReservedAnswerFinalizationSchemaUrl`, `completeWorkflowRunInTransaction`
- `agent-platform/checkpoint-status-v1` — `createCheckpointStatusReader`, `createAssignedCheckpointStatusTool`
- `agent-platform/caller-tool-outcome-v1` — `callerToolOutcomeDescriptor`, `createCallerToolInvocationBinding`, `validateCallerToolOutcome`, `callerToolOutcomeHash`
- `agent-platform/restored-child-owned-tool-v1` — `restoredChildToolDescriptor`, `createRestoredChildToolContinuation`, `getRestoredChildToolSchemaUrl`, `getRestoredChildBoundedToolsSchemaUrl`
- `agent-platform/composition-decisions-v1` — `assignedCompositionDecision`, `advanceCompositionToolObservation`
- `agent-platform/restored-child-bounded-tools-v2` — `restoredChildToolDescriptor`, `createRestoredChildToolContinuation`, `getRestoredChildBoundedToolsSchemaUrl`
- `agent-platform/saved-root-initialization-v1` — `savedRootInitializationDescriptor`, `createSavedRootInitialization`, `getSavedRootInitializationSchemaUrl`
- `agent-platform/root-cycle-progress-v1` — `ROOT_CYCLE_PROGRESS_EVENT`, `rootCycleSnapshotHash`, `rootCycleRecoveryDescriptor`, `inspectRootCycleProgress`, `createRootCycleProgress`
- `agent-platform/part-cycle-progress-v1` — `PART_CYCLE_PROGRESS_EVENT`, `partCycleSnapshotHash`, `partCycleRecoveryDescriptor`, `inspectPartCycleProgress`, `createPartCycleProgress`

### content-ledger

- `content-ledger/capture` — `normalizeContentExtensionDefinition`, `normalizeContentVersion`, `normalizeObservedUsage`, `normalizeObservedContent`, `contentVersionIdentity`, `observedUsageIdentity`, `hashValue`, `CONTENT_LEDGER_POSTGRES_SCHEMA_PROFILE`, `assertContentLedgerSchemaProfile`, `validateContentLedgerProfileSchema`, `assertProfileJsonValue`, `bootstrapContentLedgerPostgres`, `createPostgresContentLedgerWriter`
- `content-ledger/relationships` — `normalizeContentRelationship`, `contentRelationshipIdentity`, `assertNormalizedContentRelationship`, `createPostgresContentRelationshipWriter`
- `content-ledger/assessments` — `normalizeAssessmentDefinition`, `normalizeContentAssessment`, `contentAssessmentIdentity`, `assertNormalizedAssessmentDefinition`, `assertNormalizedContentAssessment`, `bootstrapContentAssessmentPostgres`, `createPostgresContentAssessmentWriter`
- `content-ledger/transfers` — `normalizeTransferPolicyDefinition`, `normalizeTransferAssessmentDefinition`, `normalizeTransferAssessment`, `normalizeTransferRequest`, `normalizeTransferDecision`, `normalizeTransferDeliveryOutcome`, `expectedPolicyIdentity`, `expectedAssessmentDefinitionIdentity`, `assertRequestIdentity`, `assertAssessmentIdentity`, `assertDecisionIdentity`, `assertDeliveryOutcomeIdentity`, `bootstrapContentTransferPostgres`, `bootstrapContentTransferDeliveryPostgres`, `createPostgresContentTransferPersistence`, `createContentTransferDecisionService`
- `content-ledger/delivery` — `createContentTransferDeliveryCoordinator`, `createPostgresContentTransferDeliveryPersistence`

## Host integration contracts

The `agent-platform` exports are `./journal`, `./bootstrap`, `./postgres-reader`, `./staging`,
`./revision-controller`,
`./manifest`, `./validation`, `./workflow-review`, `./admission`,
`./assignment-lifecycle`, `./bounded-analysis-worker`, and `./worker-runtime`.
The additive versioned exports include `./composition-checkpoint-records-v1`,
which provides PostgreSQL-only opt-in checkpoint record transitions. Its explicit
run descriptor, fresh host retirement/output-reference verification and exact
claim bind child start, atomic accepted-child/cursor/successor reservation and
one-shot successor start. It stores opaque references, exposes no body loader,
and does not wire an executor or implement continuation. The caller installs
`schemas/agent-platform/composition-checkpoint-records-v1.sql` only in its owned
journal schema and supplies every relation and host policy. Existing journals
without the pinned descriptor remain ineligible; started/uncertain work is not
automatically replayed. References confer no disclosure or delivery authority.

The other additive versioned exports are `./tool-assignment-v1` and
`./assigned-execution-v1`. The former exposes `createToolRegistration` and
`resolveToolAssignments`; the latter exposes the transport-free
`createAssignedToolAdapter`, definition validation/resolution, qualification
request/receipt validation, generic prompt rendering/hashing and
`createStructuredWorkflowAgent`. Existing package names, version, metadata and
historical exports remain unchanged.

The assigned adapter shares its registration, grant, authorization, validation,
invocation and retirement implementation with the local transport. It owns
timers and resource drains; hosts supply exact descriptors, explicit part
topology, bounded handlers, reply writing and fresh authorization. It launches
no process and loads no credentials or native provider configuration. Its reply
and lifecycle messages use the existing v1 structured execution dialect; hosts
must map their provider's calls, responses and telemetry truthfully. The
structured executor's `codex` option retains its historical name and accepts
this adapter when an explicit `assignedExecution` host dispatcher is supplied.
Legacy research methods are still required for legacy execution.

The composed draft editor accepts a trusted `toolAssignmentHost` alongside its
catalog: the existing `{ parts, registrations, hostTools }` context. The editor
snapshots it when constructed. For an existing assigned definition, an optional
patch `toolAssignments: [{ partId, tools: [{ toolId, bindingId }] }]` replaces only
the named parts' invocation sets within that host ceiling. An explicit empty
`tools` list removes that part's tools; omitted parts retain their sets. The
existing assignment resolver validates exact bindings and unchanged host topology.
Unknown, stale, duplicate and outside-ceiling references reject. Legacy definitions
cannot be converted through this patch. Normalized patches sort by part/tool
identity, omit unchanged tool edits, and replay the same definition hash.
An old baseline binding may be replaced with a current host-reviewed binding;
every reference in the resulting draft must resolve against the supplied catalog.

Assigned draft previews require an explicit `partId` matching the requested
step and stage. The editor forwards that identity and its trusted host context
to the application's catalog renderer; the host still owns stage
meaning and prompt selection. Comparison reports each changed part's tool set.
Editing and preview remain inactive (`executable: false`): they register no
handler, publish no version, acquire no execution grant and authorize no data.
Actual dispatch resolves the reviewed definition against current host bindings
through the existing assigned execution path.

Library and external tools use the same registration API, schema validation,
byte bounds and owned drain. A rendered binding is not an executable grant.
Exact current implementation identities, parent relationships, generation,
attempt lineage and fresh data/disclosure authority are checked separately.
An empty part assignment permits no tools and does not inherit another part's
catalog. The existing journal capability profile stores the exact assignment
and selected part without a schema change; read-only `inspectVersion` verifies
the run's pinned definition before admission.

Provider qualification remains independent of tool dispatch. A host-part
receipt cannot qualify a native model turn; the native route requires the
provider adapter's supported `qualifyAssignedExecution` method. No supported
native adapter is shipped here. Unknown effective capabilities remain denied.
The independent calibration fixture runs the built bounded reader and its own
conversion handler through different root/child/sibling assignments. Its
scripted receipts and token events prove host composition only, with zero real
model calls and no native capability attestation or real database socket.
The local process bridge, application stage vocabulary and domain workflows
are absent from these distributions.
PostgreSQL journals expose `appendEventInTransaction(client, input)` to compose
an event with application writes in one caller-owned transaction. Supply a client
from the journal's database with an active transaction; the caller controls commit
and rollback, and must roll back on failure. Event insertion, claim validation,
idempotency checks, and the awaited `observeJournalEvent` hook use that same
client, so rollback also removes observer writes. `appendEvent(input)` owns its
transaction and retains the same event behavior. Claim-bound journals expose the
composition method when supported and enforce their captured run and exact claim.

The journal factory also accepts an optional host `authorizeRunSummary` callback.
With that callback, `journal.getRunSummary({ runId, owner }, { signal })` reads a
single lifecycle summary. Both `read` and `disclose` callback phases must return
exactly `true`; the callback receives the frozen requested identity and signal.
The host binds the owner from its trusted actor context and independently checks
eligibility. Owner equality selects a row; it does not authorize an actor.
Authenticated owners must explicitly supply `accountId`, including `null` when
appropriate. Public-session and system owners retain their distinct forms.
Missing and foreign rows return `null` after disclosure authorization.

The frozen summary contains `runId`, `workflowVersionId`, `workflowType`, `status`,
`createdAt` and `updatedAt`, plus `startedAt` and `completedAt` when present. IDs
are bounded to 512 UTF-8 bytes; timestamps are ISO strings. No input, result,
failure, metrics, claim or child records are selected. PostgreSQL uses one
parameterized owner-filtered SELECT in a read-only owned transaction. Hosts own
database policy and signal-aware cancellation/drain; this method creates no
roles, grants or deadlines. `inspectRun` remains the trusted full-journal API.
Register summary retrieval through the same assigned-tool contract as other
tools, with a bounded schema that accepts only the requested run ID. Keep owner
and authorization decisions outside model-supplied arguments. The installed
summary fixture proves host composition without a model or live database.

The `./postgres-reader` export provides `createAgentPostgresReaderFactory`,
`AgentPostgresQueryError`, and `normalizeAgentPostgresQuery`. The factory takes
host callbacks for transaction configuration and read-context validation, with
an optional read-context issuer. Its `rowOmissionMarker` option defaults to
`$agent` and controls the reserved key in a row-byte-limit omission object.
Applications own their database transaction adapter and any policy context.
The `./workflow-review` export provides `createWorkflowReviewFactory` with host
callbacks `{ hashPayload, renderReviewPrompt, validateVerdict }`. The resulting
runtime accepts `{ journal, catalog, structuredAgent, staging, store?, createId?,
now? }` and returns `{ review, requireApproval }`. Applications supply their
own prompt, hash, verdict validator, and optional persistent review store. The
review checks the exact manifest and deterministic report, records an independent
reviewer assignment, and requires completed attempt lineage for approval.
The worker runtime factory
accepts a synchronous `buildWorker(options, { own })` callback. Register each
acquired resource with `own` immediately; the runtime handles signals, stop,
abort, and reverse awaited disposal. Applications compose their own worker,
database roles, prompts, policies, and admission behavior around that lifecycle.
The bounded-analysis worker factory requires
host functions for definition validation, execution, and structured-agent
construction. A worker may supply `createStructuredAgentForRun` to receive a
claim-bound journal after each claim. A preconstructed `structuredAgent` is a
trusted compatibility adapter whose existing journal closure is not fenced.
The revision controller takes application-owned gates, carry-forward selection,
command IDs, and thread continuation. Its journal must provide
`createRunAndAppendEvent({ run, event })`, which atomically records one resumed
child and one unassigned parent event. Exact retries keep a progressed child
in its current status and can repair a missing parent event; conflicting
events roll back newly created children and family reservations. The journal
retains its separate `createRun` and `appendEvent` methods for other callers.
The `content-ledger` exports are `./capture`, `./relationships`, `./assessments`,
`./transfers`, and `./delivery`. Both packages use host-supplied PostgreSQL
clients; neither contains a database driver. The content capture bootstrap
requires compatible journal relations supplied by the host or agent platform.
The relationship writer exposes `insertContentRelationships(client, values)`
for caller-owned transactions. It validates the full input before writing,
inserts distinct relationship identities in bounded chunks of at most 1,000,
and returns one `{ relationshipId }` result per input in input order, including
repeated identities. Repeated identities keep the timestamp from their first
input. An empty array returns an empty result without querying PostgreSQL.
The caller controls transaction boundaries and rollback when a batch fails.
For clean-schema descendant-run budgets, use
`bootstrapAgentPlatformPostgresWithRunFamilies` and pass its separate
`postgresRunFamilyRelations` map to the journal factory. The family tables
must have the bootstrap's exact names in the same schema as
`agent_workflow_runs`. Declare `{ version: 1, maxDescendantRuns }`,
`{ version: 2, maxDescendantRuns, maxTotalAttempts }`, or
`{ version: 3, maxDescendantRuns, maxTotalAttempts, maxTotalTokens }` only on
a root run. The descendant limit counts all generations; v2 and v3 attempt
limits count every durable attempt on the root and its descendants, including
completed and failed attempts. Neither limit is refunded. V3 requires a
`resourceReservation.totalTokens` value when an attempt starts and a
`resourceUsageBasis: "attempt"` or `"unknown"` value when it finishes. An
`attempt` settlement requires complete telemetry with `inputTokens`,
`cachedInputTokens`, `cacheWriteInputTokens`, `outputTokens`,
`reasoningOutputTokens`, and `totalTokens`.
An `unknown` settlement may preserve partial counters, but its reservation
continues to count as held capacity. If measured usage exceeds its reservation,
the actual usage is recorded and later attempts are denied for that family,
even when unused family capacity remains. This is journal admission accounting,
not a provider-side hard token cap. `inspectRunFamilyBudget` reports measured
and held tokens, unresolved attempts, remaining capacity, and overruns. Cost
fields are recorded separately; the clean schema does not enforce a cost budget.

PostgreSQL family reservation and v2/v3 attempt insertion require `READ COMMITTED` isolation. The clean schema
guards attempts even for a journal without the optional family relation map;
that journal uses the installed family lock helper when present. The schema
also guards attempt deletion and truncation, while its privileged owner can still
alter database objects; runtime roles are trusted persistence roles, not
tenant isolation. The attempt guard traverses full ancestry with cycle
detection, so very deep families increase the work for each insertion. Older
application schemas have no family-budget enforcement.

V3 API admission and settlement lock the family before the attempt. A direct
SQL settlement takes the attempt row first, then a nonblocking family lock. If
that lock is busy, PostgreSQL returns `55P03`; roll back the transaction or
savepoint before retrying the update.
Delivery requires host authorization, adapter registration, and exact content
body loading. Canonical content admission does not approve disclosure.
Observed usage is separate from assessments and proves neither quality, factual
support, semantic influence, nor absence of sensitive information.

No package build, audit or local installation deploys or migrates a database.
Applications explicitly opt in to storage setup and own its deployment policy.

For a local archive install, declare both package tarballs in the consuming
application's `dependencies` using `file:` paths. For example, when `archives/`
and `consumer/` are sibling directories, the consumer manifest can use
`file:../archives/agent-platform-0.0.0.tgz` and
`file:../archives/content-ledger-0.0.0.tgz`. Run `npm install --offline` from
the consumer to write its lockfile, verify the tree with `npm ls --depth=0`,
then use `npm ci --offline` to replay the lockfile. Keep the tarballs available
for offline installation and replay. The dependency declarations belong to the
application; these packages retain empty runtime dependency lists.

## Bounded HTTPS text retrieval

`agent-platform/https-fetch` exports `fetchHttpsText({ url, signal }, hostPolicy)`.
It performs an HTTPS `GET` on port 443 and returns the complete UTF-8 text entity,
its decompressed bytes and SHA-256, the encoded byte count, content type and
encoding, retrieval time, and redirect chain. HTML is returned as text without
parsing or extracting claims. Only text media types and UTF-8 are accepted;
`Content-Encoding` must be `identity` or `gzip`.

The fixed transport ceilings are 8 KiB for URLs, three redirects, 16 KiB for
response headers, 1 MiB encoded body, 2 MiB decoded body, and 15 seconds for the
whole operation. A host policy may lower these values and may provide an exact
`allowedHosts` list; it cannot raise a ceiling or disable address and TLS
checks. Every redirect is resolved and checked again. All DNS answers must be
permitted public addresses, and the request is pinned to one checked address
while TLS identity remains bound to the URL hostname. Caller headers,
credentials, proxy settings, and connection reuse are not accepted.

The returned body hash identifies the complete decompressed entity bytes; it
does not identify extracted text, semantic support, truth, or disclosure
approval. The implementation uses a conservative fixed IP special-purpose
range policy based on the IANA registries, so address policy should be reviewed
when that transport policy is updated. This module does not fetch PDFs, execute
scripts, or render client-side pages.

## Exact-claim blocked-to-revision handoff

Claim-bound journal writes capture `{ runId, workerId, claimedAt }`. Ordinary
mutations require that same live lease. PostgreSQL checks the authoritative row
under its transaction lock and reads database time after acquiring the lock.
Read-only `inspectAncestorRun` follows the captured run's actual parent chain;
it does not expose unrelated runs or unfenced mutation methods.

`blockRunForRevision({ runId, expectedClaim, outcome, failure, finalSummary })`
checks the live claim and atomically blocks the run, clears the lease, and records
one reserved `workflow.revision-handoff.v1` event. The PostgreSQL
`blockRunForRevisionInTransaction(client, input)` variant performs the same work
on the caller's active transaction client; the caller still owns commit/rollback.
The outcome is `{ gate, verdict: "revision-required", manifestHash, issues }`;
issues have canonical path/severity defaults and deterministic ordering. Failure
and final summary are part of the exact durable blocked decision.

Only `createRevisionRunAndAppendEvent({ run, event, outcome, expectedClaim })`
consumes this authority. It checks the exact releasing owner/epoch, the unchanged
blocked parent/outcome and matching resumed child. It then atomically creates the
child, its ordinary parent revision event, and one reserved
`workflow.revision-handoff-consumed.v1` event. Consumption binds the whole
normalized request hash and child ID. An exact retry returns the same result;
a different child or changed request cannot consume it again. Any failed step,
including observer failure, rolls back the combined transition. Ordinary journal
append APIs reject the reserved handoff identities. General post-lease writes
remain forbidden.

The existing event storage owns these versioned domain records. Both decoders
require exact payload fields, identities and payload hashes. A handoff stores the
claim, canonical outcome, failure and final summary; consumption stores the same
claim/outcome, child ID and request hash. They are immutable authority records,
not hints inferred from arbitrary event metadata. Existing unbound non-worker
callers retain their ordinary journal contract; introducing the dedicated methods
does not automatically opt those callers into claim-bound revision behavior.

Revision metadata cannot define the reserved `childRunId` field: the child run's
canonical ID owns that identity in both the parent event and consumption record.
Child/run and event keys are normalized once before request hashing, persistence
and exact-retry lookup, so accepted padded keys cannot strand an otherwise exact
retry or produce contradictory identities.
