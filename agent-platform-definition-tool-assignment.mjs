import { resolveToolAssignments } from './agent-platform-tool-assignment.mjs';
import { workflowDefinitionHash } from './agent-platform-prompt.mjs';

/** Opt-in definition structure. Stage meaning and approved topology belong to the host. */
export function validateWorkflowToolAssignments(definition, { parts } = {}) {
  const stepList = array(definition.steps);
  if (!stepList.length || stepList.length > 64 || definition.executionProfile !== undefined) {
    fail('Assigned definitions require explicit bounded steps without an execution profile.');
  }
  unique(stepList.map(step => identity(step.id)));
  const input = definition.toolAssignments;
  exact(input, ['schemaVersion', 'parts']);
  if (input.schemaVersion !== 1) fail('Unsupported definition tool assignment version.');
  const steps = new Map(stepList.map(step => [step.id, step]));
  const normalized = array(input.parts).map(part => {
    exact(part, ['partId', 'stepId', 'stage', 'parentPartId', 'tools']);
    const topology = normalizePart(part);
    const step = steps.get(topology.stepId);
    if (!step?.prompt || step.executionProfile !== undefined
      || !Array.isArray(step.prompt.allowedTools) || step.prompt.allowedTools.length !== 0) {
      fail('Assigned definition parts require prompts with explicit empty legacy tools and no execution profile.');
    }
    const tools = array(part.tools).map(tool => {
      exact(tool, ['toolId', 'bindingId']);
      return { toolId: identity(tool.toolId), bindingId: digest(tool.bindingId) };
    }).sort((a, b) => compare(a.toolId, b.toolId));
    unique(tools.map(tool => tool.toolId));
    return { ...topology, tools };
  }).sort((a, b) => compare(a.partId, b.partId));
  unique(normalized.map(part => part.partId));
  if (!normalized.length || [...steps.values()].some(step => step.prompt && !normalized.some(part => part.stepId === step.id))) {
    fail('Every executable prompt requires an explicit definition part.');
  }
  const byId = new Map(normalized.map(part => [part.partId, part]));
  if (normalized.filter(part => part.parentPartId === null).length !== 1) fail('Assigned definition requires one root part.');
  for (const part of normalized) {
    const visited = new Set();
    let current = part;
    while (current.parentPartId !== null) {
      if (visited.has(current.partId)) fail('Assigned definition part topology is cyclic.');
      visited.add(current.partId);
      current = byId.get(current.parentPartId);
      if (!current) fail('Unknown definition parent part.');
    }
  }
  if (parts !== undefined) {
    const approved = array(parts).map(part => {
      exact(part, ['partId', 'stepId', 'stage', 'parentPartId']);
      return normalizePart(part);
    }).sort((a, b) => compare(a.partId, b.partId));
    unique(approved.map(part => part.partId));
    if (JSON.stringify(approved) !== JSON.stringify(normalized.map(({ tools, ...part }) => part))) {
      fail('Definition parts do not match the host-approved topology.');
    }
  }
  return freeze({ schemaVersion: 1, parts: normalized });
}

/** Resolve against the current host catalog; a rendered selection conveys no authority. */
export function resolveWorkflowToolAssignment({ definition, parts, registrations, hostTools, partId }) {
  if (parts === undefined) fail('Assigned definitions require an explicit host-approved topology.');
  const canonical = validateWorkflowToolAssignments(definition, { parts });
  if (JSON.stringify(definition.toolAssignments) !== JSON.stringify(canonical)) {
    // Object key order is immaterial, but array order is canonical definition identity.
    if (workflowDefinitionHash({ toolAssignments: definition.toolAssignments })
      !== workflowDefinitionHash({ toolAssignments: canonical })) fail('Definition tool assignments are not canonical.');
  }
  if (definition.definitionHash !== workflowDefinitionHash(definition)) fail('Assigned definition hash is stale.');
  const part = canonical.parts.find(part => part.partId === partId);
  if (!part) fail('Unknown selected definition part.');
  const declarations = {
    schemaVersion: 1, definitionId: definition.id, definitionHash: definition.definitionHash,
    partIds: canonical.parts.map(part => part.partId), registrations, hostTools,
    assignments: canonical.parts.map(({ partId, tools }) => ({ partId, tools })),
  };
  const assignment = resolveToolAssignments(declarations);
  const selection = {
    schemaVersion: 1, definitionId: assignment.definitionId, definitionHash: assignment.definitionHash,
    assignmentHash: assignment.assignmentHash, partId: part.partId, stepId: part.stepId, stage: part.stage,
    tools: assignment.parts.find(value => value.partId === part.partId).tools,
  };
  return freeze({ assignment, selection, declarations: structuredClone(declarations),
    parentParts: canonical.parts.map(({ partId, parentPartId }) => ({ partId, parentPartId })) });
}

/** A separate host attestation gate. Requested or unknown provider capabilities never qualify. */
export function assignedProviderQualificationRequest(resolved, prompt, { executionMode = 'host-parts' } = {}) {
  if (!['host-parts', 'root-turn'].includes(executionMode)) fail('Unsupported assigned provider execution mode.');
  return freeze({ schemaVersion: 1, definitionId: resolved.selection.definitionId,
    definitionHash: resolved.selection.definitionHash, assignmentHash: resolved.assignment.assignmentHash,
    partId: resolved.selection.partId, tools: resolved.selection.tools.map(({ toolId, bindingId }) => ({ toolId, bindingId })),
    partTools: resolved.assignment.parts.map(({ partId, tools }) => ({ partId,
      tools: tools.map(({ toolId, bindingId }) => ({ toolId, bindingId })) })),
    executionMode, workspaceAccess: prompt.workspaceAccess, networkAccess: prompt.networkAccess,
    webSearchMode: 'disabled', nativeTools: [] });
}

export function validateAssignedProviderQualification(receipt, request) {
  exact(receipt, [...Object.keys(request), 'status', 'providerId', 'evidenceId']);
  if (receipt.status !== 'qualified') fail('Assigned provider capabilities are unqualified.');
  const { status, providerId, evidenceId, ...effective } = receipt;
  if (workflowDefinitionHash({ value: effective }) !== workflowDefinitionHash({ value: request })) {
    fail('Assigned provider capability attestation does not match the selected assignment.');
  }
  return freeze({ ...structuredClone(request), status, providerId: identity(providerId), evidenceId: identity(evidenceId) });
}

/** Explicit host-parts capability contract. v1 identities and nativeTools stay unchanged. */
export function assignedProviderCapabilityRequestV2(resolved, prompt, {providerCapabilities,executionMode = 'host-parts'} = {}) {
  if (executionMode !== 'host-parts') fail('Capability v2 requires host-parts execution.');
  exact(providerCapabilities, ['policyId','evidenceBinding']);
  identity(providerCapabilities.policyId); digest(providerCapabilities.evidenceBinding);
  const {nativeTools, ...base} = assignedProviderQualificationRequest(resolved,prompt,{executionMode});
  return freeze({...base,schemaVersion:2,providerCapabilities:structuredClone(providerCapabilities)});
}

function normalizePart(part) {
  return { partId: identity(part.partId), stepId: identity(part.stepId),
    stage: part.stage === null ? null : identity(part.stage),
    parentPartId: part.parentPartId === null ? null : identity(part.parentPartId) };
}
function exact(value, keys) {
  if (!value || Object.getPrototypeOf(value) !== Object.prototype
    || Reflect.ownKeys(value).length !== keys.length || keys.some(key => !Object.hasOwn(value, key))
    || Reflect.ownKeys(value).some(key => !keys.includes(key)
      || !Object.getOwnPropertyDescriptor(value, key)?.enumerable
      || !Object.hasOwn(Object.getOwnPropertyDescriptor(value, key), 'value'))) fail('Assignment record has missing or unknown fields.');
}
function array(value) {
  if (!Array.isArray(value) || Object.getPrototypeOf(value) !== Array.prototype || value.length > 1000
    || Reflect.ownKeys(value).length !== value.length + 1) fail('Assignment array must be dense and bounded.');
  for (let i = 0; i < value.length; i += 1) {
    const property = Object.getOwnPropertyDescriptor(value, i);
    if (!property?.enumerable || !Object.hasOwn(property, 'value')) fail('Assignment array must contain plain data.');
  }
  return value;
}
function unique(values) { if (new Set(values).size !== values.length) fail('Duplicate assignment identity.'); }
function identity(value) {
  if (typeof value !== 'string' || !value.trim() || value.trim() !== value || value.length > 512) fail('Invalid assignment identity.');
  return value;
}
function digest(value) { if (typeof value !== 'string' || !/^[0-9a-f]{64}$/.test(value)) fail('Invalid assignment digest.'); return value; }
function compare(a, b) { return a < b ? -1 : a > b ? 1 : 0; }
function freeze(value) { if (value && typeof value === 'object') { Object.values(value).forEach(freeze); Object.freeze(value); } return value; }
function fail(message) { throw new TypeError(message); }
