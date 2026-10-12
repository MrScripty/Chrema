const preparationKind = 'read-only-analysis';
const executionProfile = 'bounded-analysis.v1';
const maximumPreviousResultsBytes = 32_000;

/**
 * Build the bounded, read-only analysis execution seam for an application.
 *
 * Request projection and output-contract meaning are supplied only by trusted
 * construction. Runtime workflow data can select among those application-owned
 * contracts, but cannot replace the adapters that interpret them.
 */
export function createBoundedAnalysisExecutor(options = {}) {
  if (!options || typeof options !== 'object' || Array.isArray(options)) {
    throw new TypeError('Bounded analysis executor configuration is required.');
  }
  const { requestJson, outputContracts } = options;
  if (typeof requestJson !== 'function') {
    throw new TypeError('Bounded analysis requestJson must be a function.');
  }
  if (!outputContracts || typeof outputContracts !== 'object' || Array.isArray(outputContracts)) {
    throw new TypeError('Bounded analysis outputContracts configuration is required.');
  }
  const { defaultId, select, validate } = outputContracts;
  if (typeof defaultId !== 'string' || !defaultId.trim() || defaultId !== defaultId.trim()) {
    throw new TypeError('Bounded analysis default output contract must be a non-empty, unpadded string.');
  }
  if (typeof select !== 'function') {
    throw new TypeError('Bounded analysis outputContracts.select must be a function.');
  }
  if (typeof validate !== 'function') {
    throw new TypeError('Bounded analysis outputContracts.validate must be a function.');
  }

  const requestJsonAdapter = requestJson;
  const defaultOutputContractId = defaultId;
  const selectOutputContract = select;
  const validateOutput = validate;

  /**
   * Execute bounded, read-only analysis before an application's coordinator.
   *
   * The interface deliberately accepts the already-pinned catalog and journal:
   * callers do not get a second definition source, and every model turn still
   * passes through the existing structured-agent telemetry seam. A completed
   * assignment is reusable only when its prompt identity, telemetry, and stored
   * output all validate against the current rendered step.
   */
  return async function executeBoundedAnalysis({
    run,
    workflowDefinition,
    workflowCatalog,
    journal,
    structuredAgent,
    authorizationRecheck = async () => true,
    signal,
  } = {}) {
    requireObject(run, 'Preparation workflow run');
    requireObject(workflowDefinition, 'Preparation workflow definition');
    requireMethod(workflowCatalog, 'render', 'Workflow catalog');
    requireMethod(journal, 'inspectRun', 'Agent workflow journal');
    requireMethod(journal, 'createAssignment', 'Agent workflow journal');
    requireMethod(structuredAgent, 'run', 'Structured workflow agent');
    if (typeof authorizationRecheck !== 'function') throw new TypeError('Preparation authorization recheck must be a function.');

    const steps = (workflowDefinition.steps ?? []).filter(step => step?.kind === preparationKind);
    if (steps.length === 0) return [];
    if (steps.length > 3) throw new TypeError('A workflow may contain at most three preparation steps.');

    const results = [];
    let previousResults = [];
    for (const step of steps) {
      signal?.throwIfAborted();
      const rendered = workflowCatalog.render(workflowDefinition.id, step.id, {
        requestJson: requestJsonAdapter(run),
        previousResultsJson: boundedPreviousResultsJson(previousResults),
      });
      if (rendered.executionProfile !== executionProfile) {
        throw preparationError('PREPARATION_PROFILE_INVALID', `Preparation step ${step.id} does not use ${executionProfile}.`);
      }
      if (rendered.allowedTools?.length !== 0 || rendered.workspaceAccess !== 'read-only' || rendered.networkAccess !== false) {
        throw preparationError('PREPARATION_CAPABILITY_INVALID', `Preparation step ${step.id} is not read-only and tool-free.`);
      }
      const outputContract = selectOutputContract({ run, step });

      const currentRun = await journal.inspectRun({ runId: run.runId });
      if (await authorizationRecheck({ phase: 'preparation', run: currentRun, step }) !== true) {
        throw Object.assign(new Error('Workflow preparation authorization was revoked.'), {
          code: 'AUTHORIZATION_REVOKED', statusCode: 403,
        });
      }
      signal?.throwIfAborted();
      const recovered = recoverPreparationResult(currentRun, step, rendered);
      let output;
      if (recovered?.output) {
        output = recovered.output;
      } else {
        const assignment = recovered?.assignment ?? await journal.createAssignment({
            runId: run.runId,
            stepKey: `${run.workflowType}.${step.id}`,
            assignedTo: `${run.workflowType}.${step.id}`,
            execution: 'current-agent',
            targets: [{ type: 'workflow-request', id: run.runId, expectedVersion: null }],
          });
        const execution = await structuredAgent.run({
          runId: run.runId,
          assignmentId: assignment.assignmentId,
          prompt: rendered,
          executionProfile,
          validateOutput: (value) => validateOutput(value, outputContract),
          signal,
        });
        output = validateOutput(execution?.output, outputContract);
      }
      results.push({ stepId: step.id, output });
      previousResults = results;
    }
    return results;
  };

  function recoverPreparationResult(run, step, rendered) {
    const matches = (run.assignments ?? []).filter(({ stepKey }) => stepKey === `${run.workflowType}.${step.id}`);
    const active = matches.filter(({ status }) => ['dispatching', 'running', 'unknown'].includes(status));
    if (active.length > 1) throw preparationError('PREPARATION_ASSIGNMENT_AMBIGUOUS', `Multiple unresolved assignments exist for preparation step ${step.id}.`);
    if (active.length === 1) {
      const assignment = active[0];
      const activeAttempt = (assignment.attempts ?? []).some(attempt => attempt.outcome === null);
      if (['dispatching', 'unknown'].includes(assignment.status) || activeAttempt) {
        throw preparationError('PREPARATION_ASSIGNMENT_STALE', `Preparation step ${step.id} has an unresolved assignment.`);
      }
      // A worker can crash after creating the current-agent assignment but
      // before starting its attempt. Reuse that identity when no attempt is
      // active; creating a second assignment would make later recovery
      // ambiguous and would violate the one-target journal invariant.
      return { assignment };
    }
    const completed = matches.filter(({ status }) => status === 'completed');
    if (completed.length > 1) throw preparationError('PREPARATION_ASSIGNMENT_AMBIGUOUS', `Multiple completed assignments exist for preparation step ${step.id}.`);
    const assignment = completed[0];
    // Failed and interrupted assignments are historical retry evidence. They
    // are not ambiguous when there is no live assignment and at most one
    // completed replacement.
    if (!assignment) return null;
    const attempts = (assignment.attempts ?? []).filter(attempt => attempt.outcome === 'completed');
    if (attempts.length !== 1) throw preparationError('PREPARATION_ATTEMPT_AMBIGUOUS', `Preparation step ${step.id} does not have exactly one completed attempt.`);
    const attempt = attempts[0];
    if (attempt.capabilityProfile?.promptVersionId !== rendered.promptVersionId
      || attempt.capabilityProfile?.executionProfile !== executionProfile
      || attempt.telemetryStatus !== 'complete') {
      throw preparationError('PREPARATION_ASSIGNMENT_STALE', `Completed preparation step ${step.id} does not match its current prompt or telemetry.`);
    }
    const stored = storedOutput(attempt);
    try {
      return { output: validateOutput(stored, step.outputContract ?? defaultOutputContractId) };
    } catch (error) {
      throw preparationError('PREPARATION_OUTPUT_INVALID', `Stored preparation output for ${step.id} is invalid: ${error.message}`);
    }
  }
}

export { executionProfile as boundedAnalysisExecutionProfile };

function storedOutput(attempt) {
  const direct = attempt.result?.output;
  if (direct !== undefined) return direct;
  const capture = attempt.result?.outputCapture;
  if (capture?.status !== 'complete' || typeof capture.text !== 'string') {
    throw preparationError('PREPARATION_OUTPUT_UNAVAILABLE', 'Completed preparation output is not durably captured.');
  }
  try {
    return JSON.parse(capture.text);
  } catch {
    throw preparationError('PREPARATION_OUTPUT_INVALID', 'Completed preparation output is not valid JSON.');
  }
}

function boundedPreviousResultsJson(results) {
  const value = JSON.stringify(results);
  if (Buffer.byteLength(value, 'utf8') > maximumPreviousResultsBytes) {
    throw preparationError('PREPARATION_CONTEXT_TOO_LARGE', 'Preparation results exceed the coordinator context budget.');
  }
  return value;
}

function preparationError(code, message) {
  return Object.assign(new Error(message), { code });
}

function requireObject(value, label) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new TypeError(`${label} is required.`);
}

function requireMethod(value, method, label) {
  if (!value || typeof value[method] !== 'function') throw new TypeError(`${label}.${method} is required.`);
}
