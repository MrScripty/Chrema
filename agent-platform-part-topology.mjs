/** Inactive authoring only. A trusted host opts in to an existing bounded tree
 * shape. This contract never changes execution approval or tool registrations. */
export function validatePartTopologyPolicy(value) {
  exact(value, ['schemaVersion', 'maximumDepth', 'maximumParts']);
  if (value.schemaVersion !== 1 || ![1, 2].includes(value.maximumDepth)
    || !Number.isSafeInteger(value.maximumParts) || value.maximumParts < 1 || value.maximumParts > 16) {
    throw new TypeError('Part authoring requires a bounded direct or depth-two tree policy.');
  }
  return structuredClone(value);
}
export function partTopology(definition) {
  return definition.toolAssignments.parts.map(({ partId, stepId, stage, parentPartId }) => ({ partId, stepId, stage, parentPartId }))
    .sort((a, b) => a.partId < b.partId ? -1 : a.partId > b.partId ? 1 : 0);
}
export function applyPartTopology(definition, topology) {
  return { ...definition, toolAssignments: { schemaVersion: 1, parts: topology.map(part => ({ ...part,
    tools: structuredClone(definition.toolAssignments.parts.find(original => original.partId === part.partId)?.tools ?? []) })) } };
}
export function validatePartTopology(baseline, topology, suppliedPolicy, effectiveDefinition = baseline) {
  const policy = validatePartTopologyPolicy(suppliedPolicy);
  if (!baseline.toolAssignments || baseline.steps.length > 8 || !Array.isArray(topology)
    || topology.length < 1 || topology.length > policy.maximumParts) throw new TypeError('Part topology exceeds the approved authoring bounds.');
  for (const part of topology) exact(part, ['partId', 'stepId', 'stage', 'parentPartId']);
  const original = baseline.toolAssignments;
  const root = original.parts.find(part => part.parentPartId === null);
  const proposedRoot = topology.find(part => part.partId === root.partId);
  if (!proposedRoot || ['partId', 'stepId', 'stage', 'parentPartId'].some(key => proposedRoot[key] !== root[key])) {
    throw new TypeError('The approved composition root must be preserved.');
  }
  const next = applyPartTopology(baseline, topology);
  const ids = next.toolAssignments.parts.map(part => part.partId);
  if (new Set(ids).size !== ids.length || next.toolAssignments.parts.filter(part => part.parentPartId === null).length !== 1
    || baseline.steps.filter(step => step.prompt).some(step => !next.toolAssignments.parts.some(part => part.stepId === step.id))) {
    throw new TypeError('Parts require unique identities, one root and a reference to every prompt step.');
  }
  for (const part of next.toolAssignments.parts) {
    for (const key of ['partId', 'stepId', 'stage', 'parentPartId']) {
      const value = part[key];
      if (value === null && ['stage', 'parentPartId'].includes(key)) continue;
      if (typeof value !== 'string' || !value.trim() || value.trim() !== value || value.length > 512) throw new TypeError('Invalid part reference.');
    }
  }
  const byId = new Map(next.toolAssignments.parts.map(part => [part.partId, part]));
  for (const part of next.toolAssignments.parts) {
    const originalStep = baseline.steps.find(step => step.id === part.stepId);
    const step = effectiveDefinition.steps.find(step => step.id === part.stepId);
    if (!originalStep?.prompt || originalStep.executionProfile !== undefined || !step?.prompt || step.executionProfile !== undefined || (part.parentPartId === null ? part.stage !== null
      : part.stage === null || !Object.hasOwn(step.prompt.stageTemplates ?? {}, part.stage))) {
      throw new TypeError('Parts must reference existing approved prompt steps and declared stages.');
    }
    let depth = 0, current = part;
    while (current.parentPartId !== null) {
      if (++depth > policy.maximumDepth) throw new TypeError('Part topology exceeds the approved authoring depth.');
      current = byId.get(current.parentPartId);
      if (!current) throw new TypeError('Part parent is unavailable.');
    }
  }
  return partTopology(next);
}
function exact(value, keys) {
  if (!value || typeof value !== 'object' || Array.isArray(value)
    || Object.keys(value).sort().join('|') !== [...keys].sort().join('|')) throw new TypeError('Invalid part authoring fields.');
}
