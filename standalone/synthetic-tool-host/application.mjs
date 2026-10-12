import assert from 'node:assert/strict';

/** Independent calibration host. Inputs are synthetic; no provider or database socket is used. */
export async function runSyntheticToolHost({ modules } = {}) {
  const publicImports = modules === undefined;
  modules ??= {
    contract: await import('agent-platform/tool-assignment-v1'),
    execution: await import('agent-platform/assigned-execution-v1'),
    journal: await import('agent-platform/journal'),
    reader: await import('agent-platform/postgres-reader'),
  };
  const { createToolRegistration } = modules.contract;
  const { createAssignedToolAdapter, validateWorkflowToolAssignments, resolveWorkflowToolAssignment,
    renderPrompt, workflowDefinitionHash, createStructuredWorkflowAgent } = modules.execution;
  const descriptor = (toolId, inputSchema, resultSchema, digest) => createToolRegistration({
    schemaVersion: 1, toolId, schemaDialect: 'value-schema.v1', inputSchema, resultSchema,
    implementation: { id: `calibration:${toolId}`, version: '1', digest: digest.repeat(64) },
    adapter: { id: 'calibration:owned-adapter', version: '1', digest: 'b'.repeat(64) },
  });
  const read = descriptor('calibration.read', { type: 'object', properties: { sql: { type: 'string', maxLength: 100 } },
    required: ['sql'], additionalProperties: false }, { type: 'object', properties: {
    rows: { type: 'array', maxItems: 2, items: { type: 'object', properties: { unit: { type: 'string' } },
      required: ['unit'], additionalProperties: false } }, truncated: { type: 'boolean' },
  }, required: ['rows', 'truncated'] }, 'a');
  const external = descriptor('lab.convert', { type: 'object', properties: { value: { type: 'integer' } },
    required: ['value'], additionalProperties: false }, { type: 'integer' }, 'c');
  const ref = ({ toolId, bindingId }) => ({ toolId, bindingId });
  const definition = { id: 'calibration-composition.v1', workflowType: 'calibration', steps: ['compose', 'sample'].map(id => ({ id,
    prompt: { id: `${id}.prompt.v1`, template: 'Inspect synthetic calibration data.', variables: [],
      model: 'scripted-calibration', reasoningEffort: 'low', serviceTier: 'standard', allowedTools: [],
      workspaceAccess: 'read-only', networkAccess: false },
  })), toolAssignments: { schemaVersion: 1, parts: [
    { partId: 'root', stepId: 'compose', stage: null, parentPartId: null, tools: [ref(external)] },
    { partId: 'child', stepId: 'sample', stage: 'sampling', parentPartId: 'root', tools: [ref(read)] },
    { partId: 'sibling', stepId: 'sample', stage: 'reporting', parentPartId: 'root', tools: [] },
  ] } };
  definition.toolAssignments = validateWorkflowToolAssignments(definition);
  definition.definitionHash = workflowDefinitionHash(definition);
  const parts = definition.toolAssignments.parts.map(({ tools, ...part }) => part);
  const host = { definition, parts, registrations: [read, external], hostTools: [ref(read), ref(external)] };
  const resolved = resolveWorkflowToolAssignment({ ...host, partId: 'root' });
  const prompt = renderPrompt(definition.steps[0], {}, { toolAssignment: resolved.selection });
  const journal = modules.journal.createAgentWorkflowJournalFactory({ assertCompletion: async () => {}, projectMetrics: () => ({}),
    observeJournalEvent: async () => {}, observeAttemptOutput: async () => {}, assignmentMarker: id => `CALIBRATION:${id}` })();
  const stored = structuredClone(definition); delete stored.definitionHash;
  await journal.registerVersion({ id: definition.id, workflowType: definition.workflowType, definition: stored,
    codeRevision: 'scripted-host-v1', changeReason: 'Independent installed tool composition.',
    improvementHypothesis: 'One scoped execution seam for library and external tools.', reviewedBy: 'synthetic-host' });
  const run = await journal.createRun({ workflowVersionId: definition.id,
    owner: { kind: 'public', assistantSessionId: 'calibration-session' }, input: {} });
  await journal.claimRun({ workerId: 'calibration-worker' });
  const assignment = await journal.createAssignment({ runId: run.runId, stepKey: 'compose', assignedTo: 'calibration-host',
    execution: 'current-agent', targets: [{ type: 'calibration', id: 'synthetic-sensor', expectedVersion: null }] });
  const replies = [], invocations = [], sqlStatements = [];
  let activeTransactions = 0, callId = 0, dispatched = 0;
  const adapter = createAssignedToolAdapter({ write: reply => replies.push(structuredClone(reply)) });
  const reader = modules.reader.createAgentPostgresReaderFactory({ issueReadContext: async () => ({ version: 1 }),
    configureTransaction: async () => {}, validateReadContext: (a, b) => assert.equal(a.version, b.version) })({
    database: { async transaction(work, { signal }) {
      activeTransactions += 1;
      try { return await work({ async query(sql) {
        signal.throwIfAborted(); sqlStatements.push(sql);
        return { rows: sql.includes('SELECT CASE') ? [{ row_data: { unit: 'celsius' } }] : [] };
      } }); } finally { activeTransactions -= 1; }
    } },
  });
  const unregisterRead = adapter.registerAssignedTool({ registration: read,
    handler: ({ arguments: args, context }) => { invocations.push({ partId: context.partId, toolId: context.toolId,
      bindingId: context.bindingId, attemptId: context.attemptId });
      return reader.query(args, { signal: context.signal, runId: context.attemptId }); },
    drain: async () => assert.equal(activeTransactions, 0), maxInputBytes: 1000, maxResultBytes: 4000 });
  adapter.registerAssignedTool({ registration: external, handler: async ({ arguments: args, context }) => {
    invocations.push({ partId: context.partId, toolId: context.toolId, bindingId: context.bindingId, attemptId: context.attemptId });
    return args.value * 2;
  }, drain: async () => {}, maxInputBytes: 1000, maxResultBytes: 4000 });
  async function call(threadId, tool, args) {
    const id = ++callId;
    await adapter.executeAssignedToolRequest({ id, params: { threadId, tool, arguments: args } });
    return replies.find(reply => reply.id === id)?.result;
  }
  const configuration = { ...host, maxRunDurationMs: 15_000, authorize: async () => true,
    qualify: request => ({ ...request, status: 'qualified', providerId: 'scripted-host-only', evidenceId: 'calibration:catalog:v1' }),
    async execute({ attempt, onThread }) {
      dispatched += 1;
      const rootGrant = await adapter.grantAssignedThread({ attempt, partId: 'root', threadId: 'root-thread' });
      await adapter.grantAssignedThread({ attempt, partId: 'child', threadId: 'child-thread', parentThreadId: 'root-thread' });
      await adapter.grantAssignedThread({ attempt, partId: 'sibling', threadId: 'sibling-thread', parentThreadId: 'root-thread' });
      assert.equal(rootGrant.providerEffectiveCatalog, null);
      await assert.rejects(adapter.grantAssignedThread({ attempt, partId: 'child', threadId: 'wrong-child', parentThreadId: 'sibling-thread' }), /host-declared parent/);
      await onThread('root-thread');
      assert.equal((await call('root-thread', read.toolId, { sql: 'SELECT unit FROM calibration' })).success, false);
      assert.equal((await call('child-thread', external.toolId, { value: 4 })).success, false);
      assert.equal((await call('sibling-thread', external.toolId, { value: 4 })).success, false);
      assert.equal((await call('sibling-thread', read.toolId, { sql: 'SELECT unit FROM calibration' })).success, false);
      assert.equal(await call('ungranted', external.toolId, { value: 4 }), undefined);
      assert.equal((await call('root-thread', external.toolId, { value: 4 })).contentItems[0].text, '8');
      const data = await call('child-thread', read.toolId, { sql: 'SELECT unit FROM calibration' });
      assert.equal(data.success, true);
      assert.equal(JSON.parse(data.contentItems[0].text).rows[0].unit, 'celsius');
      await adapter.revokeThreadGrant('child-thread');
      assert.equal(await call('child-thread', read.toolId, { sql: 'SELECT unit FROM calibration' }), undefined);
      assert.equal((await call('root-thread', external.toolId, { value: 5 })).contentItems[0].text, '10');
      for (const message of [
        { method: 'thread/settings/updated', params: { threadId: 'root-thread', threadSettings: {
          model: 'scripted-calibration', modelProvider: 'synthetic', effort: 'low', serviceTier: 'standard' } } },
        { method: 'thread/tokenUsage/updated', params: { threadId: 'root-thread', turnId: 'scripted-turn', tokenUsage: {
          last: { inputTokens: 10, cachedInputTokens: 0, outputTokens: 2, totalTokens: 12 },
          total: { inputTokens: 10, cachedInputTokens: 0, outputTokens: 2, totalTokens: 12 } } } },
        { method: 'turn/completed', params: { threadId: 'root-thread', turnId: 'scripted-turn', turn: { id: 'scripted-turn', status: 'completed' } } },
      ]) { adapter.emitLifecycle(message); await Promise.all([...adapter.lifecycleByThread.values()]); }
      return { threadId: 'root-thread', text: '{"unit":"celsius","conversion":10}', attemptId: attempt.attemptId,
        assignmentHash: attempt.assignment.assignmentHash, partId: 'root' };
    },
  };
  const prices = [{ id: 'scripted-price', provider: 'synthetic', model: 'scripted-calibration', serviceTier: 'standard', currency: 'USD',
    inputPerMillion: 1, cachedInputPerMillion: 1, outputPerMillion: 1, effectiveFrom: '2020-01-01T00:00:00Z',
    sourceReference: 'synthetic-only', reviewedBy: 'synthetic-host' }];
  const request = { runId: run.runId, assignmentId: assignment.assignmentId, prompt };
  const result = await createStructuredWorkflowAgent({ journal, codex: adapter, prices, assignedExecution: configuration }).run(request);
  assert.deepEqual(result.output, { unit: 'celsius', conversion: 10 });
  assert.ok(sqlStatements.some(sql => sql.includes('SELECT CASE')));
  assert.ok(invocations.every(call => call.attemptId === result.attempt.attemptId));
  assert.deepEqual(invocations.map(call => call.partId), ['root', 'child', 'root']);
  assert.equal(result.attempt.capabilityProfile.selectedToolPart.partId, 'root');
  assert.equal(result.attempt.capabilityProfile.toolAssignment.assignmentHash, resolved.assignment.assignmentHash);
  assert.equal(adapter.activeAssignmentAttempts.size, 0);
  assert.equal(adapter.threadGrants.size, 0);

  // An explicit host receipt cannot manufacture support for native execution.
  const { execute, ...nativeConfiguration } = configuration;
  await assert.rejects(createStructuredWorkflowAgent({ journal, codex: adapter, prices, assignedExecution: nativeConfiguration }).run(request),
    /qualification is unavailable/);
  assert.equal(dispatched, 1);
  assert.equal((await journal.inspectRun({ runId: run.runId })).assignments[0].attempts.length, 1);
  const stale = structuredClone(host);
  // Descriptor replacement is tested through the owned adapter below; exact
  // stale references reject at the definition resolver before execution.
  stale.hostTools[0].bindingId = 'f'.repeat(64);
  assert.throws(() => resolveWorkflowToolAssignment({ ...host, hostTools: stale.hostTools, partId: 'root' }), /identity mismatch/);
  const replacementAttempt = adapter.createToolAssignmentAttempt({ declarations: resolved.declarations,
    parentParts: resolved.parentParts, attemptId: 'replacement-attempt', deadline: { deadlineAtMs: Date.now() + 15_000 },
    authorize: async () => true, workspaceAccess: 'read-only', networkAccess: false });
  await adapter.grantAssignedThread({ attempt: replacementAttempt, partId: 'root', threadId: 'replacement-root' });
  unregisterRead();
  await adapter.cancelToolAssignmentAttempt(replacementAttempt);
  await assert.rejects(adapter.authorizeAssignedAnswer({ attempt: replacementAttempt, partId: 'root', threadId: 'replacement-root' }), /unavailable/);
  await adapter.drainToolAssignments();
  return { mode: 'scripted-host-only', publicPackageImports: publicImports, definitionHash: definition.definitionHash,
    assignmentHash: resolved.assignment.assignmentHash, attemptId: result.attempt.attemptId,
    readBindingId: read.bindingId, externalBindingId: external.bindingId, invocations,
    scopeEnforced: true, durableLineage: true, revokedChildRefused: true, replacementRefused: true,
    unsupportedNativeRefused: true, realModelCalls: 0, realDatabase: false, nativeCapabilitiesAttested: false, billedCost: null };
}
