import { applyPartTopology, partTopology, validatePartTopology, validatePartTopologyPolicy } from './agent-platform-part-topology.mjs';
import { validateStageAuthoring, validateStageAuthoringPolicy } from './agent-platform-stage-authoring.mjs';
import { assertDraftPreparationPatch } from './agent-platform-draft-preparation-policy.mjs';
import { assertSafeAuditPrompt } from '../shared/agent-audit-safety.mjs';
import { resolveWorkflowToolAssignment, validateWorkflowToolAssignments } from './agent-platform-definition-tool-assignment.mjs';

/** Trusted application policy supplies kinds and contracts, never patch algorithms.
 * This editor cannot register, execute, publish, or authorize a definition.
 * validateDefinition owns topology, stage names, variables and capability rules.
 */
export function createComposedWorkflowDraftEditorFactory(options = {}) {
  const { validateDefinition, createCatalog, inputBindings, describeEditor = () => ({}), policy: suppliedPolicy } = options;
  for (const [name, value] of Object.entries({ validateDefinition, createCatalog, inputBindings, describeEditor })) {
    if (typeof value !== 'function') throw new TypeError(`Composed draft editor requires ${name}.`);
  }
  exact(suppliedPolicy, ['stagedPromptKinds', 'preparation']);
  exact(suppliedPolicy.preparation, ['kind', 'maximumSteps', 'insertionIndex']);
  const policy = structuredClone(suppliedPolicy);
  if (!Array.isArray(policy.stagedPromptKinds)
    || policy.stagedPromptKinds.some(kind => typeof kind !== 'string' || !kind.trim())
    || new Set(policy.stagedPromptKinds).size !== policy.stagedPromptKinds.length
    || typeof policy.preparation.kind !== 'string' || !policy.preparation.kind.trim()
    || policy.stagedPromptKinds.includes(policy.preparation.kind)
    || !Number.isInteger(policy.preparation.maximumSteps) || policy.preparation.maximumSteps < 0 || policy.preparation.maximumSteps > 100
    || !Number.isInteger(policy.preparation.insertionIndex) || policy.preparation.insertionIndex < 0 || policy.preparation.insertionIndex > 100) {
    throw new TypeError('Composed draft editor requires bounded, distinct operation policy.');
  }
/** Inactive configuration only. Never registers, publishes, or dispatches a run. */
  return function createWorkflowDraftEditor({ catalog, settingsCatalog = catalog, toolAssignmentHost }) {
    const definitions = structuredClone(catalog.list());
    // The host supplies reviewed descriptors/topology, never the draft request.
    const assignmentHost = structuredClone(toolAssignmentHost);
    const topologyPolicy = assignmentHost?.topologyAuthoring === undefined ? undefined : validatePartTopologyPolicy(assignmentHost.topologyAuthoring);
    const stagePolicy = assignmentHost?.stageAuthoring === undefined ? undefined : validateStageAuthoringPolicy(assignmentHost.stageAuthoring);
    if (stagePolicy && !topologyPolicy) throw new TypeError('Stage authoring requires explicit host topology authoring approval.');
    const byId = new Map(definitions.map(definition => [definition.id, definition]));
    const settingsSteps = structuredClone(settingsCatalog.list()).flatMap(d => d.steps.filter(s => s.prompt));
    // Preserve existing base-setting order, then discover configured stage-only
    // tuples from the same trusted catalog. Draft input cannot extend this set.
    const settings = [...new Map([
      ...settingsSteps.map(s => s.prompt),
      ...settingsSteps.filter(s => policy.stagedPromptKinds.includes(s.kind))
        .flatMap(s => Object.values(s.prompt.stageSettings ?? {})),
    ].map(({ model, reasoningEffort, serviceTier }) => {
      const setting = { model, reasoningEffort, serviceTier };
      return [JSON.stringify(setting), setting];
    })).values()];
    return {
      inspect() {
        return { ...structuredClone(describeEditor(structuredClone(definitions))),
          definitions: structuredClone(definitions), settings: structuredClone(settings), executable: false };
      },
      validate({ baseVersionId, patch }) {
        exact(patch, ['maximumConcurrency', 'prompts'], ['maximumSemanticRevisions', 'preparationSteps', 'toolAssignments', 'partTopology']);
        const base = structuredClone(byId.get(baseVersionId));
        if (!base) throw new TypeError(`Unknown workflow version: ${baseVersionId}.`);
        if (!Number.isInteger(patch.maximumConcurrency) || patch.maximumConcurrency < 1 || patch.maximumConcurrency > base.maximumConcurrency) {
          throw new TypeError('Concurrency must be positive and no greater than the approved baseline.');
        }
        if (!Array.isArray(patch.prompts) || patch.prompts.length > base.steps.length) throw new TypeError('Provide a bounded list of prompt edits.');
        if (Object.hasOwn(base, 'draftPreparationPolicy')) {
          assertDraftPreparationPatch(base.draftPreparationPolicy, patch.preparationSteps);
        }
        const next = structuredClone(base);
        const compositionEdit = {};
        if (Object.hasOwn(patch, 'preparationSteps')) {
          if (!Array.isArray(patch.preparationSteps) || patch.preparationSteps.length > policy.preparation.maximumSteps) throw new TypeError(`Provide at most ${policy.preparation.maximumSteps} preparation steps.`);
          const preparationSteps = patch.preparationSteps.map(step => {
            exact(step, ['id', 'kind', 'executionProfile', 'prompt']);
            if (step.kind !== policy.preparation.kind) throw new TypeError('Only approved preparation operations may be composed.');
            exact(step.prompt, ['id', 'variables', 'template', 'model', 'reasoningEffort', 'serviceTier', 'allowedTools', 'workspaceAccess', 'networkAccess']);
            if (!settings.some(s => s.model === step.prompt.model && s.reasoningEffort === step.prompt.reasoningEffort && s.serviceTier === step.prompt.serviceTier)) throw new TypeError('Preparation settings must match a configured runtime setting.');
            const result = structuredClone(step);
            result.prompt.template = assertSafeAuditPrompt(result.prompt.template, { maximumLength: 50000 });
            if (/\{\{|\}\}/.test(result.prompt.template.replace(/\{\{[A-Za-z][A-Za-z0-9]*\}\}/g, ''))) throw new TypeError('Malformed preparation prompt variable.');
            return result;
          });
          next.steps = next.steps.filter(step => step.kind !== policy.preparation.kind);
          next.steps.splice(policy.preparation.insertionIndex, 0, ...preparationSteps);
          if (JSON.stringify(preparationSteps) !== JSON.stringify(base.steps.filter(step => step.kind === policy.preparation.kind))) compositionEdit.preparationSteps = preparationSteps;
        }
        next.maximumConcurrency = patch.maximumConcurrency;
        const revisionEdit = {};
        if (Object.hasOwn(patch, 'maximumSemanticRevisions')) {
          if (!base.revisionPolicy || !Number.isInteger(patch.maximumSemanticRevisions)
            || patch.maximumSemanticRevisions < 0 || patch.maximumSemanticRevisions > base.revisionPolicy.maximumSemanticRevisions) {
            throw new TypeError('Semantic revision limit must be an integer from zero through the approved baseline limit.');
          }
          next.revisionPolicy.maximumSemanticRevisions = patch.maximumSemanticRevisions;
          if (patch.maximumSemanticRevisions !== base.revisionPolicy.maximumSemanticRevisions) revisionEdit.maximumSemanticRevisions = patch.maximumSemanticRevisions;
        }
        const seen = new Set();
        const normalized = [];
        for (const edit of patch.prompts) {
          exact(edit, ['stepId', 'template', 'model', 'reasoningEffort', 'serviceTier'], ['inputBindings', 'stageTemplates', 'stageSettings']);
          const step = next.steps.find(s => s.id === edit.stepId);
          if (!step?.prompt || step.kind === policy.preparation.kind || seen.has(edit.stepId)) throw new TypeError('Prompt edits require unique, existing prompt step IDs; preparation edits belong in preparationSteps.');
          seen.add(edit.stepId);
          if (!settings.some(s => s.model === edit.model && s.reasoningEffort === edit.reasoningEffort && s.serviceTier === edit.serviceTier)) {
            throw new TypeError('Model, reasoning, and service tier must match a configured runtime setting.');
          }
          const template = assertSafeAuditPrompt(edit.template, { maximumLength: 50000 });
          if (/\{\{|\}\}/.test(template.replace(/\{\{[A-Za-z][A-Za-z0-9]*\}\}/g, ''))) throw new TypeError('Malformed prompt variable.');
          Object.assign(step.prompt, { template, model: edit.model, reasoningEffort: edit.reasoningEffort, serviceTier: edit.serviceTier });
          const normalizedEdit = { stepId: edit.stepId, template, model: edit.model, reasoningEffort: edit.reasoningEffort, serviceTier: edit.serviceTier };
          if (Object.hasOwn(edit, 'stageSettings')) {
            if (!policy.stagedPromptKinds.includes(step.kind) || !edit.stageSettings || typeof edit.stageSettings !== 'object' || Array.isArray(edit.stageSettings)) {
              throw new TypeError('Stage settings must be an object on an approved staged prompt step.');
            }
            const overrides = Object.fromEntries(Object.entries(edit.stageSettings).sort(([a], [b]) => a.localeCompare(b)).map(([stage, value]) => {
              exact(value, ['model', 'reasoningEffort', 'serviceTier']);
              const approved = settings.find(s => s.model === value.model && s.reasoningEffort === value.reasoningEffort && s.serviceTier === value.serviceTier);
              if (!approved) throw new TypeError('Stage settings must match a configured runtime setting.');
              return [stage, { ...approved }];
            }));
            if (Object.keys(overrides).length) step.prompt.stageSettings = overrides;
            else delete step.prompt.stageSettings;
            if (Object.keys(overrides).length || base.steps.find(s => s.id === step.id).prompt.stageSettings) normalizedEdit.stageSettings = overrides;
          }
          if (Object.hasOwn(edit, 'stageTemplates')) {
            if (!policy.stagedPromptKinds.includes(step.kind) || !edit.stageTemplates || typeof edit.stageTemplates !== 'object' || Array.isArray(edit.stageTemplates)) {
              throw new TypeError('Stage templates must be an object.');
            }
            const templates = Object.fromEntries(Object.entries(edit.stageTemplates).sort(([a], [b]) => a.localeCompare(b)).map(([stage, value]) => {
              const text = assertSafeAuditPrompt(value, { maximumLength: 50000 });
              if (/\{\{|\}\}/.test(text.replace(/\{\{[A-Za-z][A-Za-z0-9]*\}\}/g, ''))) throw new TypeError('Malformed stage prompt variable.');
              return [stage, text];
            }));
            // Empty overrides mean fallback, without changing historical hashes.
            if (Object.keys(templates).length) step.prompt.stageTemplates = templates;
            else delete step.prompt.stageTemplates;
            if (Object.keys(templates).length || base.steps.find(s => s.id === step.id).prompt.stageTemplates) normalizedEdit.stageTemplates = templates;
          }
          if (Object.hasOwn(edit, 'inputBindings')) {
            const baselineBindings = inputBindings(step);
            if (!edit.inputBindings || typeof edit.inputBindings !== 'object' || Array.isArray(edit.inputBindings)
              || Object.keys(edit.inputBindings).some(name => !/^[A-Za-z][A-Za-z0-9]{0,99}$/.test(name))) {
              throw new TypeError('Input bindings require bounded prompt variable names.');
            }
            step.inputBindings = structuredClone(edit.inputBindings);
            step.prompt.variables = Object.keys(step.inputBindings);
            const bindings = inputBindings(step);
            if (JSON.stringify(Object.values(bindings).sort()) !== JSON.stringify(Object.values(baselineBindings).sort())) {
              throw new TypeError('Draft input bindings must preserve every baseline worker source.');
            }
            // An unchanged mapping must not introduce a hash-only change to old definitions.
            if (bindingJson(bindings) === bindingJson(baselineBindings)) {
              const original = base.steps.find(s => s.id === step.id);
              if (original.inputBindings === undefined) delete step.inputBindings;
            } else normalizedEdit.inputBindings = bindings;
          }
          normalized.push(normalizedEdit);
        }
        if (stagePolicy && base.toolAssignments) {
          if (!Array.isArray(assignmentHost.parts)) throw new TypeError('Stage authoring requires an explicit host-approved baseline topology.');
          validateWorkflowToolAssignments(base, { parts: assignmentHost.parts });
        }
        validateStageAuthoring(base, next, stagePolicy);
        if (Object.hasOwn(patch, 'partTopology')) {
          if (!topologyPolicy) throw new TypeError('Part topology edits require explicit host authoring approval.');
          validateWorkflowToolAssignments(base, { parts: assignmentHost.parts });
          const topology = validatePartTopology(base, patch.partTopology, topologyPolicy, next);
          next.toolAssignments = validateWorkflowToolAssignments(applyPartTopology(next, topology), { parts: topology });
          if (JSON.stringify(topology) !== JSON.stringify(partTopology(base))) compositionEdit.partTopology = topology;
        }
        const approvedParts = Object.hasOwn(patch, 'partTopology') ? partTopology(next) : assignmentHost?.parts;
        if (Object.hasOwn(patch, 'toolAssignments')) {
          if (base.toolAssignments === undefined) throw new TypeError('Tool edits require an existing assigned definition.');
          if (assignmentHost === undefined) throw new TypeError('Tool edits require an explicit host-approved topology and catalog.');
          validateWorkflowToolAssignments(base, { parts: assignmentHost.parts });
          const baselineAssignments = validateWorkflowToolAssignments(next, { parts: approvedParts });
          next.toolAssignments = structuredClone(baselineAssignments);
          if (!Array.isArray(patch.toolAssignments) || patch.toolAssignments.length > baselineAssignments.parts.length) {
            throw new TypeError('Provide a bounded list of part tool edits.');
          }
          const seenParts = new Set();
          for (const edit of patch.toolAssignments) {
            exact(edit, ['partId', 'tools']);
            const part = next.toolAssignments.parts.find(part => part.partId === edit.partId);
            if (!part || seenParts.has(edit.partId)) throw new TypeError('Tool edits require unique, existing part IDs.');
            seenParts.add(edit.partId);
            part.tools = structuredClone(edit.tools);
          }
          next.toolAssignments = structuredClone(validateWorkflowToolAssignments(next, { parts: approvedParts }));
          const edits = next.toolAssignments.parts.filter(part => JSON.stringify(part.tools)
            !== JSON.stringify(baselineAssignments.parts.find(original => original.partId === part.partId).tools))
            .map(({ partId, tools }) => ({ partId, tools: structuredClone(tools) }));
          if (edits.length) compositionEdit.toolAssignments = edits;
        }
        // Existing validator enforces the exact variables, ordering, and mandatory gates.
        const definition = validateDefinition(next);
        if (Object.hasOwn(patch, 'partTopology')) {
          if (JSON.stringify(partTopology(definition)) !== JSON.stringify(approvedParts)) throw new TypeError('Definition validation changed the approved draft topology.');
        }
        if (Object.hasOwn(patch, 'toolAssignments') || Object.hasOwn(patch, 'partTopology')) resolveAssignment(definition, definition.toolAssignments?.parts[0]?.partId);
        normalized.sort((a, b) => a.stepId.localeCompare(b.stepId));
        return { baseVersionId: base.id, baseHash: base.definitionHash,
          patch: { maximumConcurrency: next.maximumConcurrency, prompts: normalized, ...revisionEdit, ...compositionEdit },
          definition, executable: false };
      },
      render(input) {
        const { definition } = this.validate(input);
        if (JSON.stringify(input.variables ?? {}).length > 50000) throw new TypeError('Sample inputs exceed the preview limit.');
        const selection = { stage: input.stage };
        if (definition.toolAssignments !== undefined) {
          const resolved = resolveAssignment(definition, input.partId);
          if (resolved.selection.stepId !== input.stepId || resolved.selection.stage !== (input.stage ?? null)) {
            throw new TypeError('Selected assignment part does not match the preview step and stage.');
          }
          Object.assign(selection, { partId: input.partId, toolAssignmentHost: hostForDefinition(definition) });
        } else if (input.partId !== undefined) throw new TypeError('Legacy draft previews do not accept an assigned part.');
        const rendered = createCatalog({ definitions: [definition] }).render(definition.id, input.stepId, input.variables, selection);
        assertSafeAuditPrompt(rendered.text, { maximumLength: 100000 });
        return { ...rendered, executable: false };
      },
      compare(input) {
        const result = this.validate(input);
        const base = structuredClone(byId.get(result.baseVersionId));
        const changes = [];
        function compareField(path, before, after) {
          if (before !== after) changes.push({ path, before, after });
        }
        compareField('maximumConcurrency', base.maximumConcurrency, result.definition.maximumConcurrency);
        compareField('revisionPolicy.maximumSemanticRevisions', base.revisionPolicy?.maximumSemanticRevisions, result.definition.revisionPolicy?.maximumSemanticRevisions);
        compareField('preparationSteps', JSON.stringify(base.steps.filter(s => s.kind === policy.preparation.kind)), JSON.stringify(result.definition.steps.filter(s => s.kind === policy.preparation.kind)));
        if (result.definition.toolAssignments && base.toolAssignments) {
          compareField('partTopology', JSON.stringify(partTopology(base)), JSON.stringify(partTopology(result.definition)));
        }
        for (const part of result.definition.toolAssignments?.parts ?? []) {
          compareField(`toolAssignments.parts.${part.partId}.tools`,
            JSON.stringify(base.toolAssignments.parts.find(original => original.partId === part.partId)?.tools ?? []), JSON.stringify(part.tools));
        }
        for (const step of result.definition.steps) {
          if (!step.prompt || step.kind === policy.preparation.kind) continue;
          const original = base.steps.find(s => s.id === step.id).prompt;
          for (const field of ['template', 'model', 'reasoningEffort', 'serviceTier']) {
            compareField(`steps.${step.id}.prompt.${field}`, original[field], step.prompt[field]);
          }
          compareField(`steps.${step.id}.prompt.stageTemplates`, bindingJson(original.stageTemplates ?? {}), bindingJson(step.prompt.stageTemplates ?? {}));
          compareField(`steps.${step.id}.prompt.stageSettings`, bindingJson(original.stageSettings ?? {}), bindingJson(step.prompt.stageSettings ?? {}));
          compareField(`steps.${step.id}.inputBindings`,
            bindingJson(inputBindings(base.steps.find(s => s.id === step.id))),
            bindingJson(inputBindings(step)));
        }
        return { baseVersionId: base.id, baseHash: base.definitionHash,
          draftHash: result.definition.definitionHash, changes, executable: false };
      },
    };

    function hostForDefinition(definition) {
      if (!assignmentHost) return undefined;
      if (!topologyPolicy) return structuredClone(assignmentHost);
      const base = byId.get(definition.id);
      validateWorkflowToolAssignments(base, { parts: assignmentHost.parts });
      validateStageAuthoring(base, definition, stagePolicy);
      const parts = validatePartTopology(base, partTopology(definition), topologyPolicy, definition);
      return { ...structuredClone(assignmentHost), parts };
    }
    function resolveAssignment(definition, partId) {
      return resolveWorkflowToolAssignment({ definition, partId, parts: hostForDefinition(definition)?.parts,
        registrations: assignmentHost?.registrations, hostTools: assignmentHost?.hostTools });
    }
  };
}

function bindingJson(bindings) {
  return JSON.stringify(Object.fromEntries(Object.entries(bindings).sort(([a], [b]) => a.localeCompare(b))));
}

function exact(value, keys, optional = []) {
  if (!value || typeof value !== 'object' || Array.isArray(value)
    || Object.keys(value).some(k => !keys.includes(k) && !optional.includes(k)) || keys.some(k => !Object.hasOwn(value, k))) {
    throw new TypeError(`Expected exactly: ${keys.join(', ')}.`);
  }
}
