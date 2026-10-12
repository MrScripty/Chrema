/** Inactive stage authoring; source names and settings grant no execution access. */
export function validateStageAuthoringPolicy(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)
    || Object.keys(value).sort().join('|') !== 'maximumStagesPerStep|schemaVersion'
    || value.schemaVersion !== 1 || !Number.isSafeInteger(value.maximumStagesPerStep)
    || value.maximumStagesPerStep < 1 || value.maximumStagesPerStep > 16) {
    throw new TypeError('Stage authoring requires an exact bounded stage policy.');
  }
  return structuredClone(value);
}

export function validateStageAuthoring(baseline, effectiveDefinition, suppliedPolicy) {
  if (!baseline.toolAssignments) return;
  const policy = suppliedPolicy === undefined ? undefined : validateStageAuthoringPolicy(suppliedPolicy);
  for (const original of baseline.steps.filter(step => step.prompt)) {
    const prompt = effectiveDefinition.steps.find(step => step.id === original.id)?.prompt;
    if (!prompt) throw new TypeError('Stage authoring must preserve existing prompt steps.');
    const baselineStages = Object.keys(original.prompt.stageTemplates ?? {});
    const stages = Object.keys(prompt.stageTemplates ?? {});
    const introduced = stages.filter(stage => !baselineStages.includes(stage));
    if (introduced.length && !policy) throw new TypeError('New assigned stages require explicit host stage authoring approval.');
    if (policy && (stages.length > policy.maximumStagesPerStep || baselineStages.some(stage => !stages.includes(stage)))) {
      throw new TypeError('Stage authoring must preserve baseline stages within the approved stage limit.');
    }
    if (introduced.some(stage => !/^[A-Za-z][A-Za-z0-9_-]{0,63}$/.test(stage))) {
      throw new TypeError('New stages require a letter followed by letters, numbers, underscores or hyphens (maximum 64 characters).');
    }
    for (const stage of Object.keys(prompt.stageSettings ?? {})) {
      if ((policy || !Object.hasOwn(original.prompt.stageSettings ?? {}, stage)) && !stages.includes(stage)) {
        throw new TypeError('New stage settings require a declared stage template.');
      }
    }
  }
}
