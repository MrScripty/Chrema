/** The gate projection, not the complete persisted validation report schema. */
const contract = {
  id: 'workflow-validation-gate.v1', channel: 'code-owned-result',
  unknownFields: 'retained',
  context: 'Trusted validation output, not model prose. These are the fields checked for routing; the full report also retains validator identity, measurements, and completion evidence. Passing requires no issues. Failure routes to the bounded revision controller, never directly to admission.',
  fields: [
    { name: 'runId', type: 'current run identity' },
    { name: 'manifestHash', type: 'exact compiled lowercase SHA-256' },
    { name: 'passed', type: 'boolean' },
    { name: 'issues', type: 'array; must be empty when passed is true' },
  ],
};

export function describeWorkflowValidationGate() {
  return structuredClone(contract);
}

export function assertWorkflowResultIdentity(value, expected, label) {
  if (typeof expected?.runId !== 'string' || !expected.runId
    || !/^[a-f0-9]{64}$/.test(expected?.manifestHash ?? '')
    || value?.runId !== expected.runId || value?.manifestHash !== expected.manifestHash) {
    throw new TypeError(`${label} must refer to the current run and exact compiled manifest.`);
  }
}

// Keep the original object, including durable audit fields; do not project them away.
export function validateWorkflowValidationGate(report, identity) {
  assertWorkflowResultIdentity(report, identity, 'Deterministic report');
  if (typeof report.passed !== 'boolean' || !Array.isArray(report.issues)
    || (report.passed && report.issues.length > 0)) {
    throw new TypeError('Deterministic report requires a boolean result and consistent issues.');
  }
  return report;
}
