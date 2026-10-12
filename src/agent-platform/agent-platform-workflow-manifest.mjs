import { canonicalStagedJson, stagedValueHash } from './agent-platform-workflow-staging.mjs';

/** Projects journal/staging snapshots and checks structural consistency.
 * Trusted application policy is captured at construction. Validation accepts
 * compiled manifests, not arbitrary untrusted nested JSON. It establishes neither
 * currentness, authorization, business correctness nor disclosure approval.
 * Callers own snapshot consistency and deterministic evidence-count policy.
 */
export function createWorkflowManifestFactory({ requiredValidators, policy, countEvidence }) {
  if (!Array.isArray(requiredValidators) || !requiredValidators.length
    || requiredValidators.some(value => !value || !text(value.name) || !text(value.version))) {
    throw new TypeError('Manifest required validators must contain names and versions.');
  }
  requiredValidators = requiredValidators.map(({ name, version }) => ({ name, version }));
  if (!policy || typeof policy.allowSupersededAssignments !== 'boolean') {
    throw new TypeError('Manifest supersession policy must be explicit.');
  }
  const captured = {
    allowSupersededAssignments: policy.allowSupersededAssignments,
    callerAttemptActivities: policy.callerAttemptActivities === true,
    allowEmptyCommands: policy.allowEmptyCommands === true,
  };
  for (const key of ['excludedStepSuffixes', 'unrestrictedTargetStepKeys', 'nonProducingStepSuffixes', 'advisoryExecutionProfiles']) {
    if (!Array.isArray(policy[key]) || policy[key].some(value => !text(value))) {
      throw new TypeError(`Manifest policy ${key} must be an array of nonempty strings.`);
    }
    captured[key] = [...policy[key]];
  }
  policy = captured;
  if (typeof countEvidence !== 'function') throw new TypeError('Manifest countEvidence is required.');
  function evidenceCount(command) {
    const count = countEvidence(clone(command));
    if (!Number.isSafeInteger(count) || count < 0) throw new TypeError('Manifest evidence count must be a nonnegative safe integer.');
    return count;
  }
  return Object.freeze({
    compile(run, stagedCommands) {
      const manifest = compileManifest(run, stagedCommands);
      return { ...manifest, manifestHash: stagedValueHash(manifest) };
    },
    validate: validateManifestShape,
  });

  function compileManifest(run, stagedCommands) {
    const activitiesByAttempt = new Map();
    const activityIds = new Set();
    if (policy.callerAttemptActivities) {
      for (const [journalOrdinal, assignment] of run.assignments.entries()) {
        if (assignment.execution !== 'current-agent' || assignment.attempts.length !== 0) continue;
        const event = run.events?.find(({ sourceKey }) => sourceKey === `assignment-prepared:${assignment.assignmentId}`);
        const ref = event?.payload?.executionRef;
        const parent = run.assignments.find(({ assignmentId }) => assignmentId === ref?.assignmentId);
        const attempt = parent?.attempts.find(({ attemptId }) => attemptId === ref?.attemptId);
        if (event?.eventType !== 'workflow.assignment-prepared'
          || event.payload?.childAssignmentId !== assignment.assignmentId
          || event.assignmentId !== ref?.assignmentId || event.attemptId !== ref?.attemptId
          || ref?.kind !== 'caller-attempt' || assignment.parentAssignmentId !== ref.assignmentId
          || assignment.agentThreadId !== ref.threadId || attempt?.threadId !== ref.threadId
          || parent?.assignmentId === assignment.assignmentId
          || policy.excludedStepSuffixes.some(suffix => parent.stepKey.endsWith(suffix))) continue;
        const activities = activitiesByAttempt.get(ref.attemptId) ?? [];
        activities.push({
          assignmentId: assignment.assignmentId,
          parentAssignmentId: assignment.parentAssignmentId,
          stepKey: assignment.stepKey,
          assignedTo: assignment.assignedTo,
          execution: assignment.execution,
          status: assignment.status,
          journalOrdinal,
          targets: assignment.targets.map(({ type, id, expectedVersion }) => ({ type, id, expectedVersion })),
          executionRef: clone(ref),
        });
        activitiesByAttempt.set(ref.attemptId, activities);
        activityIds.add(assignment.assignmentId);
      }
    }
    const commands = [...stagedCommands]
      .sort((left, right) => left.ordinal - right.ordinal || left.commandId.localeCompare(right.commandId))
      .map((command) => ({
        commandId: command.commandId,
        ordinal: command.ordinal,
        commandType: command.commandType,
        schemaVersion: command.schemaVersion,
        payloadHash: command.payloadHash,
        assignmentId: command.assignmentId,
        attemptId: command.attemptId,
        dependencyIds: [...command.dependencyIds],
        expectedVersions: clone(command.expectedVersions),
      }));
    const assignments = run.assignments
      .filter((assignment) => {
        if (activityIds.has(assignment.assignmentId)) return false;
        if (policy.callerAttemptActivities && assignment.execution === 'current-agent'
          && assignment.attempts.length === 0) return true;
        return !policy.excludedStepSuffixes.some(suffix => assignment.stepKey.endsWith(suffix));
      })
      .map((assignment) => ({
      assignmentId: assignment.assignmentId,
      parentAssignmentId: assignment.parentAssignmentId,
      stepKey: assignment.stepKey,
      assignedTo: assignment.assignedTo,
      execution: assignment.execution,
      status: assignment.status,
      targets: assignment.targets.map(({ type, id, expectedVersion }) => ({ type, id, expectedVersion })),
      attempts: assignment.attempts.map((attempt) => ({
        attemptId: attempt.attemptId,
        ordinal: attempt.ordinal,
        promptHash: attempt.promptHash,
        requested: clone(attempt.requested),
        observed: attempt.observed == null ? null : clone(attempt.observed),
        capabilityHash: attempt.capabilityHash,
        ...(policy.advisoryExecutionProfiles.includes(attempt.capabilityProfile?.executionProfile)
          ? { executionProfile: attempt.capabilityProfile.executionProfile } : {}),
        threadId: attempt.threadId,
        turnId: attempt.turnId,
        startedAt: attempt.startedAt,
        completedAt: attempt.completedAt,
        outcome: attempt.outcome,
        telemetryStatus: attempt.telemetryStatus,
        usage: attempt.usage == null ? null : clone(attempt.usage),
        priceVersionId: attempt.price?.versionId ?? null,
        apiEquivalentCost: attempt.apiEquivalentCost == null ? null : clone(attempt.apiEquivalentCost),
        resultHash: attempt.resultHash,
        ...(activitiesByAttempt.has(attempt.attemptId)
          ? { activities: activitiesByAttempt.get(attempt.attemptId).map(clone) } : {}),
      })),
    }));
    const targetsByKey = new Map();
    for (const command of commands) {
      for (const [key, expectedVersion] of Object.entries(command.expectedVersions).sort(([left], [right]) => left.localeCompare(right))) {
        if (targetsByKey.has(key) && targetsByKey.get(key).expectedVersion !== expectedVersion) {
          targetsByKey.get(key).conflictingExpectedVersions = true;
        } else if (!targetsByKey.has(key)) {
          const separator = key.indexOf(":");
          targetsByKey.set(key, {
            key,
            type: key.slice(0, separator),
            id: key.slice(separator + 1),
            expectedVersion,
          });
        }
      }
    }
    const targets = [...targetsByKey.values()].sort((left, right) => left.key.localeCompare(right.key));
    const attempts = assignments.flatMap((assignment) => assignment.attempts);
    return {
      manifestVersion: 1,
      runId: run.runId,
      workflowType: run.workflowType,
      workflowVersionId: run.workflowVersionId,
      inputHash: run.inputHash,
      commands,
      assignments,
      targets,
      requiredValidators: requiredValidators.map(clone),
      aggregates: {
        commandCount: commands.length,
        targetCount: targets.length,
        assignmentCount: assignments.length,
        attemptCount: attempts.length,
        evidenceCount: stagedCommands.reduce((total, command) => total + evidenceCount(command), 0),
        inputTokens: attempts.reduce((sum, attempt) => sum + (attempt.usage?.inputTokens ?? 0), 0),
        outputTokens: attempts.reduce((sum, attempt) => sum + (attempt.usage?.outputTokens ?? 0), 0),
        totalTokens: attempts.reduce((sum, attempt) => sum + (attempt.usage?.totalTokens ?? 0), 0),
      },
    };
  }

  function validateManifestShape(manifest) {
    const issues = [];
    const unhashed = { ...manifest };
    delete unhashed.manifestHash;
    if (stagedValueHash(unhashed) !== manifest.manifestHash) {
      issues.push(issue("MANIFEST_HASH_MISMATCH", "manifestHash", stagedValueHash(unhashed), manifest.manifestHash));
    }
    if (manifest.commands.length === 0 && !policy.allowEmptyCommands) {
      issues.push(issue("MANIFEST_HAS_NO_COMMANDS", "commands", ">=1", 0));
    }
    uniqueIdentityIssues(manifest.commands, "commandId", "DUPLICATE_COMMAND_ID", "commands", issues);
    uniqueIdentityIssues(manifest.assignments, "assignmentId", "DUPLICATE_ASSIGNMENT_ID", "assignments", issues);
    const commandIds = new Set(manifest.commands.map(({ commandId }) => commandId));
    for (const [index, command] of manifest.commands.entries()) {
      for (const dependencyId of command.dependencyIds) {
        if (!commandIds.has(dependencyId)) {
          issues.push(issue("COMMAND_DEPENDENCY_MISSING", `commands.${index}.dependencyIds`, "existing command id", dependencyId));
        }
      }
      const assignment = manifest.assignments.find(({ assignmentId }) => assignmentId === command.assignmentId);
      const attempt = assignment?.attempts.find(({ attemptId }) => attemptId === command.attemptId);
      if (!attempt) issues.push(issue("COMMAND_LINEAGE_MISSING", `commands.${index}`, "known assignment attempt", command.attemptId));
      if (attempt && attempt.outcome !== "completed") {
        issues.push(issue("COMMAND_LINEAGE_INCOMPLETE", `commands.${index}`, "completed attempt", attempt.outcome));
      }
      for (const key of Object.keys(command.expectedVersions)) {
        const expectedTarget = assignment?.targets.some((target) => `${target.type}:${target.id}` === key);
        if (!expectedTarget && !policy.unrestrictedTargetStepKeys.includes(assignment?.stepKey)) {
          issues.push(issue("COMMAND_TARGET_OUTSIDE_ASSIGNMENT", `commands.${index}.expectedVersions.${key}`, "assigned target", key));
        }
      }
    }
    if (hasDependencyCycle(manifest.commands)) {
      issues.push(issue("COMMAND_DEPENDENCY_CYCLE", "commands", "acyclic dependencies", "cycle"));
    }
    for (const [index, assignment] of manifest.assignments.entries()) {
      const superseded = assignment.status !== "completed" && policy.allowSupersededAssignments && isSupersededAssignment(manifest.assignments, index);
      if (assignment.status !== "completed" && !superseded) {
        issues.push(issue("ASSIGNMENT_NOT_COMPLETED", `assignments.${index}.status`, "completed", assignment.status));
      }
      if (assignment.attempts.length === 0) {
        issues.push(issue("ASSIGNMENT_ATTEMPT_MISSING", `assignments.${index}.attempts`, ">=1", 0));
        continue;
      }
      if (assignment.status === "completed" && commandProducingAssignment(assignment)) {
        const assignedTargets = assignment.targets.map(({ type, id }) => `${type}:${id}`).sort();
        const commandTargets = [...new Set(manifest.commands
          .filter(({ assignmentId }) => assignmentId === assignment.assignmentId)
          .flatMap(({ expectedVersions }) => Object.keys(expectedVersions)))].sort();
        if (canonicalStagedJson(assignedTargets) !== canonicalStagedJson(commandTargets)) {
          issues.push(issue("ASSIGNMENT_TARGET_MISMATCH", `assignments.${index}.targets`, assignedTargets, commandTargets));
        }
      }
      if (assignment.attempts.some(attempt => policy.advisoryExecutionProfiles.includes(attempt.executionProfile))
        && manifest.commands.some(command => command.assignmentId === assignment.assignmentId)) {
        issues.push(issue('PREPARATION_COMMAND_FORBIDDEN', `assignments.${index}`, 'no commands from preparation', assignment.assignmentId));
      }
      const finalAttempt = assignment.attempts.at(-1);
      if (assignment.status === "completed" && finalAttempt?.outcome !== "completed") {
        issues.push(issue("ATTEMPT_NOT_COMPLETED", `assignments.${index}.attempts`, "final attempt completed", finalAttempt?.outcome));
      }
      for (const [attemptIndex, attempt] of assignment.attempts.entries()) {
        const path = `assignments.${index}.attempts.${attemptIndex}`;
        for (const [activityIndex, activity] of (attempt.activities ?? []).entries()) {
          const activityPath = `${path}.activities.${activityIndex}`;
          if (activity.execution !== 'current-agent'
            || activity.parentAssignmentId !== assignment.assignmentId
            || activity.executionRef?.kind !== 'caller-attempt'
            || activity.executionRef.assignmentId !== assignment.assignmentId
            || activity.executionRef.attemptId !== attempt.attemptId
            || activity.executionRef.threadId !== attempt.threadId) {
            issues.push(issue('ACTIVITY_LINEAGE_INVALID', activityPath, 'exact parent attempt and thread', activity.executionRef));
          }
          const completed = activity.status === 'completed';
          const superseded = !completed && policy.allowSupersededAssignments
            && (isSupersededActivity(manifest.assignments, activity)
              || (["failed", "interrupted"].includes(activity.status)
                && assignment.status !== "completed"
                && isSupersededAssignment(manifest.assignments, index)));
          if (!completed && !superseded) {
            issues.push(issue('ACTIVITY_NOT_COMPLETED', `${activityPath}.status`, 'completed', activity.status));
          }
        }
        const isRequiredSuccessfulAttempt = assignment.status === "completed" && attempt === finalAttempt;
        if (!attempt.promptHash || !attempt.startedAt || !attempt.completedAt
          || !attempt.requested?.model || !attempt.requested?.reasoningEffort
          || (isRequiredSuccessfulAttempt && (!attempt.observed?.model || !attempt.observed?.provider || !attempt.observed?.reasoningEffort))) {
          issues.push(issue("ATTEMPT_AUDIT_INCOMPLETE", path,
            isRequiredSuccessfulAttempt ? "prompt/requested and observed model/effort/timing lineage" : "prompt/requested model/effort/timing lineage",
            "missing field"));
        }
        if (isRequiredSuccessfulAttempt
          && (attempt.telemetryStatus !== "complete" || !attempt.usage || !attempt.priceVersionId || !attempt.apiEquivalentCost)) {
          issues.push(issue("ATTEMPT_TELEMETRY_INCOMPLETE", path, "complete measured and priced telemetry", attempt.telemetryStatus));
        }
      }
    }
    for (const target of manifest.targets) {
      if (target.conflictingExpectedVersions) {
        issues.push(issue("TARGET_VERSION_CONFLICT", `targets.${target.key}`, "one expected version", "multiple"));
      }
    }
    return issues;
  }

  function commandProducingAssignment(assignment) {
    if (assignment.attempts.length && assignment.attempts.every(attempt => policy.advisoryExecutionProfiles.includes(attempt.executionProfile))) return false;
    return !policy.nonProducingStepSuffixes.some(suffix => assignment.stepKey.endsWith(suffix))
      && !policy.excludedStepSuffixes.some(suffix => assignment.stepKey.endsWith(suffix))
      && !policy.unrestrictedTargetStepKeys.includes(assignment.stepKey);
  }
}

function isSupersededAssignment(assignments, index) {
  const assignment = assignments[index];
  if (!["failed", "interrupted"].includes(assignment.status)) return false;
  const targetIdentity = assignment.targets
    .map(({ type, id }) => `${type}:${id}`)
    .sort()
    .join("\n");
  return assignments.slice(index + 1).some((candidate) => candidate.status === "completed"
    && candidate.targets.map(({ type, id }) => `${type}:${id}`).sort().join("\n") === targetIdentity);
}

function isSupersededActivity(assignments, activity) {
  if (!["failed", "interrupted"].includes(activity.status)) return false;
  const targetIdentity = activity.targets.map(({ type, id }) => `${type}:${id}`).sort().join("\n");
  return assignments.some((assignment) => assignment.attempts.some((attempt) =>
    (attempt.activities ?? []).some((candidate) => candidate.assignmentId !== activity.assignmentId
      && candidate.journalOrdinal > activity.journalOrdinal
      && candidate.status === "completed"
      && candidate.targets.map(({ type, id }) => `${type}:${id}`).sort().join("\n") === targetIdentity)));
}

export function orderManifestCommands(commands) {
  const byId = new Map(commands.map((command) => [command.commandId, command]));
  const result = [];
  const visiting = new Set();
  const visited = new Set();
  function visit(command) {
    if (visited.has(command.commandId)) return;
    if (visiting.has(command.commandId)) throw conflict("Staged commands contain a dependency cycle.");
    visiting.add(command.commandId);
    for (const id of command.dependencyIds) {
      const dependency = byId.get(id);
      if (!dependency) throw conflict(`Staged command dependency ${id} is missing.`);
      visit(dependency);
    }
    visiting.delete(command.commandId);
    visited.add(command.commandId);
    result.push(command);
  }
  for (const command of [...commands].sort((left, right) => left.ordinal - right.ordinal)) visit(command);
  return result;
}

function hasDependencyCycle(commands) {
  try {
    orderManifestCommands(commands);
    return false;
  } catch (error) {
    return /cycle/i.test(error.message);
  }
}

function uniqueIdentityIssues(values, field, code, path, issues) {
  const seen = new Set();
  for (const [index, value] of values.entries()) {
    if (seen.has(value[field])) issues.push(issue(code, `${path}.${index}.${field}`, "unique", value[field]));
    seen.add(value[field]);
  }
}

function issue(code, path, expected, actual) {
  return { code, path, severity: "blocking", expected, actual };
}

function conflict(message) {
  const error = new Error(message);
  error.code = "WORKFLOW_CONFLICT";
  error.statusCode = 409;
  return error;
}

function clone(value) {
  return structuredClone(value);
}

function text(value) { return typeof value === "string" && value.trim().length > 0; }
