import { createHash } from "node:crypto";

/**
 * Render a selected workflow step with its already selected prompt.
 *
 * The platform primitive deliberately has no knowledge of application stages
 * or policies. Callers select the step and prompt before invoking
 * it, and the declared variables are the complete input contract.
 */
export function renderPrompt(step, variables = {}, { toolAssignment } = {}) {
  const stepId = step.id;
  if (toolAssignment !== undefined && (step.executionProfile !== undefined
    || toolAssignment.stepId !== stepId || !Array.isArray(toolAssignment.tools)
    || !Array.isArray(step.prompt.allowedTools) || step.prompt.allowedTools.length !== 0)) {
    throw new TypeError('Assigned prompt selection contradicts the selected step or legacy capabilities.');
  }
  const supplied = Object.keys(variables).sort();
  const declared = [...step.prompt.variables].sort();
  const extra = supplied.find((name) => !declared.includes(name));
  if (extra) throw new TypeError(`Workflow step ${stepId} received undeclared prompt variable: ${extra}.`);
  const missing = declared.find((name) => !Object.hasOwn(variables, name));
  if (missing) throw new TypeError(`Workflow step ${stepId} is missing prompt variable: ${missing}.`);
  for (const name of declared) {
    const value = variables[name];
    if (typeof value !== "string") throw new TypeError(`Workflow prompt variable ${name} must be a string.`);
  }
  // Substitute only placeholders in the trusted template. Supplied content
  // can itself contain placeholders; never interpret it as another template.
  const text = step.prompt.template.replace(/\{\{([A-Za-z][A-Za-z0-9]*)\}\}/g, (_, name) => {
    if (!declared.includes(name)) throw new TypeError(`Workflow step ${stepId} has an unresolved prompt variable.`);
    return variables[name];
  });
  return {
    text,
    promptVersionId: step.prompt.id,
    ...(step.executionProfile ? { executionProfile: step.executionProfile } : {}),
    model: step.prompt.model,
    reasoningEffort: step.prompt.reasoningEffort,
    serviceTier: step.prompt.serviceTier,
    allowedTools: toolAssignment === undefined ? [...step.prompt.allowedTools] : toolAssignment.tools.map(tool => tool.toolId),
    ...(toolAssignment === undefined ? {} : { toolAssignment: structuredClone(toolAssignment) }),
    workspaceAccess: step.prompt.workspaceAccess,
    networkAccess: step.prompt.networkAccess,
  };
}

/**
 * Normalize and validate the structural portion of a prompt definition.
 *
 * This platform primitive deliberately does not authorize workspace or
 * network capabilities. Application adapters own those policy checks.
 * The prompt object is normalized in place and this function returns
 * undefined for compatibility with the original application validator.
 */
export function validatePromptStructure(prompt, stepId) {
  if (!prompt || typeof prompt !== "object" || Array.isArray(prompt)) throw new TypeError(`Workflow step ${stepId} requires a prompt.`);
  prompt.id = requireText(prompt.id, `Workflow step ${stepId} prompt id`);
  prompt.template = requireText(prompt.template, `Workflow step ${stepId} prompt template`);
  prompt.variables = uniqueStrings(prompt.variables ?? [], `Workflow step ${stepId} prompt variable`).sort();
  const referenced = [...prompt.template.matchAll(/\{\{([A-Za-z][A-Za-z0-9]*)\}\}/g)].map((match) => match[1]);
  if (stableJson([...new Set(referenced)].sort()) !== stableJson(prompt.variables)) {
    throw new TypeError(`Workflow step ${stepId} prompt variables do not match its template.`);
  }
  prompt.model = requireText(prompt.model, `Workflow step ${stepId} prompt model`);
  prompt.reasoningEffort = requireText(prompt.reasoningEffort, `Workflow step ${stepId} prompt reasoning effort`);
  prompt.serviceTier = requireText(prompt.serviceTier, `Workflow step ${stepId} prompt service tier`);
  prompt.allowedTools = uniqueStrings(prompt.allowedTools ?? [], `Workflow step ${stepId} allowed tool`).sort();
}

/** Hash a definition's identity without including its derived hash field. */
export function workflowDefinitionHash(definition) {
  const unhashed = structuredClone(definition);
  delete unhashed.definitionHash;
  return createHash("sha256").update(stableJson(unhashed)).digest("hex");
}

function stableJson(value) {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stableJson(value[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

function uniqueStrings(values, label) {
  if (!Array.isArray(values)) throw new TypeError(`${label}s must be an array.`);
  const result = values.map((value) => requireText(value, label));
  if (new Set(result).size !== result.length) throw new TypeError(`${label}s must be unique.`);
  return result;
}

function requireText(value, label) {
  if (typeof value !== "string" || !value.trim()) throw new TypeError(`${label} is required.`);
  return value.trim();
}
