/** Immutable definition policy for inactive preparation edits, not execution authority. */
export function validateDraftPreparationPolicy(value) {
  exact(value, ['schemaVersion', 'kind', 'executionProfile', 'maximumSteps']);
  if (value.schemaVersion !== 1 || !identifier(value.kind) || !identifier(value.executionProfile)
    || !Number.isInteger(value.maximumSteps) || value.maximumSteps < 0 || value.maximumSteps > 3) {
    throw new TypeError('Invalid bounded draft preparation policy.');
  }
  return structuredClone(value);
}

export function assertDraftPreparationPatch(policy, steps) {
  const approved = validateDraftPreparationPolicy(policy);
  if (steps === undefined) return;
  if (!Array.isArray(steps) || steps.length > approved.maximumSteps) {
    throw new TypeError('Invalid bounded preparation composition.');
  }
  for (const step of steps) {
    exact(step, ['id', 'kind', 'executionProfile', 'prompt']);
    exact(step.prompt, ['id', 'variables', 'template', 'model', 'reasoningEffort', 'serviceTier',
      'allowedTools', 'workspaceAccess', 'networkAccess']);
    if (step.kind !== approved.kind || step.executionProfile !== approved.executionProfile
      || !Array.isArray(step.prompt.allowedTools) || step.prompt.allowedTools.length !== 0
      || step.prompt.workspaceAccess !== 'read-only' || step.prompt.networkAccess !== false) {
      throw new TypeError('Invalid bounded preparation composition.');
    }
  }
}

function identifier(value) {
  return typeof value === 'string' && /^[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,199}$/.test(value);
}
function exact(value, keys) {
  if (!value || typeof value !== 'object' || Array.isArray(value)
    || Object.keys(value).length !== keys.length || keys.some(key => !Object.hasOwn(value, key))) {
    throw new TypeError('Invalid bounded draft preparation policy or composition shape.');
  }
}
