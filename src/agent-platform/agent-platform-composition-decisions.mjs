/** The part interpreter shared by ordinary and durably fenced host execution.
 * Selection checks do not grant invocation or disclosure permission. */
export function assignedCompositionDecision({decision,toolIds=[],childPartIds=[],allowAnswer=true}={}) {
  const keys={tool:['kind','toolId','arguments'],child:['kind','partId','task'],answer:['kind','output']}[decision?.kind];
  if(!keys||!decision||Object.getPrototypeOf(decision)!==Object.prototype
    ||Reflect.ownKeys(decision).sort().join('|')!==keys.sort().join('|'))throw new Error('Unsupported composition decision.');
  if(decision.kind==='tool'&&!toolIds.includes(decision.toolId))throw new Error('Composition tool is not selected for this part.');
  if(decision.kind==='child'&&(!childPartIds.includes(decision.partId)||typeof decision.task!=='string'||!decision.task.trim()))throw new Error('Composition child is not selected for this part.');
  if(decision.kind==='answer'&&(!allowAnswer||!decision.output||Object.getPrototypeOf(decision.output)!==Object.prototype))throw new Error('Composition answer is unavailable.');
  return structuredClone(decision);
}
export function advanceCompositionToolObservation({observations,toolId,result}={}) {
  if(!Array.isArray(observations)||typeof toolId!=='string'||!toolId)throw new Error('Composition observation is unavailable.');
  return [...structuredClone(observations),{kind:'tool',toolId,result:structuredClone(result)}];
}
