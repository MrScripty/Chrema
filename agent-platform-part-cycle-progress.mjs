import {createHash} from 'node:crypto';
import {resolvePostgresRelations} from './postgres-relations.mjs';
import {normalizeExpectedClaim,assertPostgresExpectedClaim,claimLostError} from './agent-platform-claim-bound-journal.mjs';
import {workflowDefinitionHash} from './agent-platform-prompt.mjs';

export const PART_CYCLE_PROGRESS_EVENT='workflow.assigned-composition/part-cycle.v1';
const copy=value=>structuredClone(value);
const canonical=value=>workflowDefinitionHash({value});
// The journal hashes the payload itself; runtime binding identities use a value wrapper.
function journalJson(value){if(Array.isArray(value))return `[${value.map(journalJson).join(',')}]`;
 if(value&&typeof value==='object')return `{${Object.keys(value).sort().map(key=>`${JSON.stringify(key)}:${journalJson(value[key])}`).join(',')}}`;
 return JSON.stringify(value);}
const journalHash=value=>createHash('sha256').update(journalJson(value)).digest('hex');
const fail=(code='PART_CYCLE_PROGRESS_UNAVAILABLE')=>Object.assign(new Error('Part cycle progress is unavailable.'),{code,statusCode:409});
const check=value=>{if(!value)throw fail();};
function exact(value,keys){check(value&&Object.getPrototypeOf(value)===Object.prototype&&Reflect.ownKeys(value).length===keys.length&&keys.every(key=>{
 const field=Object.getOwnPropertyDescriptor(value,key);return field?.enumerable&&Object.hasOwn(field,'value');}));}
function identity(value){check(typeof value==='string'&&/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,199}$/.test(value));return value;}
function digest(value){check(typeof value==='string'&&/^[a-f0-9]{64}$/.test(value));return value;}
function integer(value,min,max){check(Number.isSafeInteger(value)&&value>=min&&value<=max);return value;}
function frozen(value){if(value&&typeof value==='object'){Object.values(value).forEach(frozen);Object.freeze(value);}return value;}
function json(value){let count=0;const visit=(item,depth)=>{
 check(++count<=65536&&depth<=64);
 if(item===null||typeof item==='boolean')return;
 if(typeof item==='string'){check(Buffer.from(item).toString('utf8')===item);return;}
 if(typeof item==='number'){check(Number.isFinite(item));return;}
 check(item&&typeof item==='object');
 if(Array.isArray(item)){check(Object.getPrototypeOf(item)===Array.prototype&&Reflect.ownKeys(item).length===item.length+1);
  for(let index=0;index<item.length;index++){const field=Object.getOwnPropertyDescriptor(item,String(index));check(field?.enumerable&&Object.hasOwn(field,'value'));visit(field.value,depth+1);}return;}
 check(Object.getPrototypeOf(item)===Object.prototype);
 for(const key of Reflect.ownKeys(item)){check(typeof key==='string');const field=Object.getOwnPropertyDescriptor(item,key);check(field?.enumerable&&Object.hasOwn(field,'value'));visit(field.value,depth+1);}
 };visit(value,0);return JSON.stringify(value);}
export function partCycleSnapshotHash(value){return createHash('sha256').update(typeof value==='string'?value:json(value)).digest('hex');}
export function partCycleRecoveryDescriptor(value){
 exact(value,['schemaVersion','mode','writerId','maximumRecoveries','bodyOwner']);
 check(value.schemaVersion===1&&value.mode==='accepted-part-cycles.v1'&&value.writerId==='restricted-host-parts.v1');integer(value.maximumRecoveries,1,3);
 exact(value.bodyOwner,['ownerId','contractId','contractVersion','maximumBodyBytes']);identity(value.bodyOwner.ownerId);identity(value.bodyOwner.contractId);
 integer(value.bodyOwner.contractVersion,1,1000000);integer(value.bodyOwner.maximumBodyBytes,1024,512000);return frozen(copy(value));
}
function descriptorFor({definition,parts,runtimeBinding,descriptor,limits}){
 const policy=partCycleRecoveryDescriptor(descriptor);const root=parts.find(part=>part.parentPartId===null);
 check(Array.isArray(parts)&&parts.length>=1&&parts.length<=16&&new Set(parts.map(part=>part.partId)).size===parts.length);
 const maximumDepth=runtimeBinding.shape==='root-nested-stages.v1'?2:runtimeBinding.shape==='root-direct-leaves.v1'?1:null;check(maximumDepth!==null);
 for(const part of parts){identity(part.partId);let current=part,depth=0;while(current.parentPartId!==null){check(++depth<=maximumDepth);current=parts.find(value=>value.partId===current.parentPartId);check(current);}}
 check(root&&parts.filter(part=>part.parentPartId===null).length===1&&definition.definitionHash===workflowDefinitionHash(definition)
  &&runtimeBinding.definitionId===definition.id&&runtimeBinding.definitionHash===definition.definitionHash&&runtimeBinding.rootPartId===root.partId);
 exact(limits,['maximumDecisions','maximumContextBytes','maxRunDurationMs']);integer(limits.maximumDecisions,1,32);integer(limits.maximumContextBytes,1024,128000);integer(limits.maxRunDurationMs,60000,120000);
 return frozen({...copy(policy),definitionId:definition.id,definitionHash:definition.definitionHash,runtimeBindingHash:canonical(runtimeBinding),assignmentHash:digest(runtimeBinding.assignmentHash),rootPartId:root.partId,limits:copy(limits)});
}
const lineageKeys=['assignmentId','attemptId','rootPartId','threadId','invocationId','hostTurnId','deadlineAtMs'];
function validateLineage(lineage,run,descriptor){
 exact(lineage,lineageKeys);lineageKeys.filter(key=>key!=='deadlineAtMs').forEach(key=>identity(lineage[key]));integer(lineage.deadlineAtMs,1,Number.MAX_SAFE_INTEGER);
 check(run.assignments.length===1);const assignment=run.assignments[0],attempt=assignment.attempts?.[0];
 check(attempt&&assignment.attempts.length===1&&assignment.assignmentId===lineage.assignmentId&&attempt.attemptId===lineage.attemptId
  &&assignment.parentAssignmentId===null&&assignment.execution==='current-agent'&&['running','completed'].includes(assignment.status)
  &&attempt.ordinal===1&&attempt.assignmentId===assignment.assignmentId&&attempt.threadId===lineage.threadId&&assignment.agentThreadId===lineage.threadId
  &&lineage.rootPartId===descriptor.rootPartId&&attempt.capabilityProfile?.selectedToolPart?.partId===descriptor.rootPartId
  &&attempt.capabilityProfile.selectedToolPart.definitionId===descriptor.definitionId&&attempt.capabilityProfile.selectedToolPart.definitionHash===descriptor.definitionHash
  &&attempt.capabilityProfile.selectedToolPart.stepId===assignment.stepKey
  &&attempt.capabilityProfile.toolAssignment.definitionId===descriptor.definitionId&&attempt.capabilityProfile.toolAssignment.definitionHash===descriptor.definitionHash
  &&attempt.capabilityProfile.toolAssignment?.assignmentHash===descriptor.assignmentHash&&attempt.capabilityProfile.deadlineAtMs===lineage.deadlineAtMs
  &&attempt.capabilityProfile.structuredOutput===true&&attempt.capabilityProfile.workspaceAccess==='read-only'&&attempt.capabilityProfile.networkAccess===false
  &&attempt.capabilityProfile.executionProfile===undefined&&attempt.promptHash===journalHash(attempt.prompt)&&attempt.capabilityHash===journalHash(attempt.capabilityProfile)
  &&(attempt.outcome===null||attempt.outcome==='completed'&&attempt.telemetryStatus==='complete')
  &&!(assignment.status==='completed'&&attempt.outcome!== 'completed'));
 return {assignment,attempt};
}
function reference(value,descriptor){exact(value,['ownerId','contractId','referenceId','sha256','byteLength']);
 check(value.ownerId===descriptor.bodyOwner.ownerId&&value.contractId===descriptor.bodyOwner.contractId);identity(value.referenceId);digest(value.sha256);integer(value.byteLength,1,descriptor.bodyOwner.maximumBodyBytes);}
function cycle(value,descriptor){exact(value,['cycleId','frameId','partId','threadId','inputHash','inputBytes','decisionsConsumed','claim']);
 for(const key of ['cycleId','frameId','partId','threadId'])identity(value[key]);digest(value.inputHash);integer(value.inputBytes,1,descriptor.limits.maximumContextBytes);
 integer(value.decisionsConsumed,1,descriptor.limits.maximumDecisions);check(normalizeExpectedClaim(value.claim));}
function action(value,options){
 if(value?.kind==='tool'){exact(value,['kind','frameId','partId','toolId','bindingId','callId']);identity(value.toolId);identity(value.callId);digest(value.bindingId);
  const part=options.definition.toolAssignments.parts.find(part=>part.partId===value.partId);check(part?.tools.some(tool=>tool.toolId===value.toolId&&tool.bindingId===value.bindingId));}
 else if(value?.kind==='child'){exact(value,['kind','frameId','childFrameId','partId','threadId','taskHash']);identity(value.childFrameId);identity(value.threadId);digest(value.taskHash);}
 else{exact(value,['kind','frameId','partId']);check(value.kind==='answer');}
 identity(value.frameId);identity(value.partId);check(options.parts.some(part=>part.partId===value.partId));
}
function delegation(value,descriptor,options){exact(value,['cycle','actionMetadata','retirementId','predecessorHash']);cycle(value.cycle,descriptor);action(value.actionMetadata,options);
 check(value.actionMetadata.kind==='child'&&value.actionMetadata.frameId===value.cycle.frameId);identity(value.retirementId);digest(value.predecessorHash);}
const frameKeys=['frameId','parentFrameId','partId','threadId','taskHash','observationsHash','observationsCount','observationSources','outputHash','delegation'];
function frames(value,descriptor,options,lineage){check(Array.isArray(value)&&value.length>=1&&value.length<=3);
 const ids=new Set(),threads=new Set();
 for(let index=0;index<value.length;index++){const frame=value[index];exact(frame,frameKeys);identity(frame.frameId);identity(frame.partId);identity(frame.threadId);
  check(!ids.has(frame.frameId)&&!threads.has(frame.threadId));ids.add(frame.frameId);threads.add(frame.threadId);digest(frame.taskHash);digest(frame.observationsHash);
  integer(frame.observationsCount,0,descriptor.limits.maximumDecisions);check(Array.isArray(frame.observationSources)&&frame.observationSources.length===frame.observationsCount);
  if(frame.outputHash!==null)digest(frame.outputHash);if(index<value.length-1)check(frame.outputHash===null);
  for(const source of frame.observationSources){exact(source,['actionMetadata','observationHash']);action(source.actionMetadata,options);digest(source.observationHash);check(source.actionMetadata.frameId===frame.frameId&&source.actionMetadata.kind!=='answer');
   if(source.actionMetadata.kind==='tool')check(source.actionMetadata.partId===frame.partId);
   else check(options.parts.some(part=>part.partId===source.actionMetadata.partId&&part.parentPartId===frame.partId));}
  if(index===0)check(frame.parentFrameId===null&&frame.partId===descriptor.rootPartId&&frame.threadId===lineage.threadId&&frame.delegation===null);
  else{const parent=value[index-1];identity(frame.parentFrameId);check(frame.parentFrameId===parent.frameId&&options.parts.some(part=>part.partId===frame.partId&&part.parentPartId===parent.partId));
   delegation(frame.delegation,descriptor,options);const delegated=frame.delegation.actionMetadata;
   check(frame.delegation.cycle.partId===parent.partId&&frame.delegation.cycle.threadId===parent.threadId&&delegated.frameId===parent.frameId
    &&delegated.childFrameId===frame.frameId&&delegated.partId===frame.partId&&delegated.threadId===frame.threadId&&delegated.taskHash===frame.taskHash);}
 }
 const depth=options.runtimeBinding.shape==='root-nested-stages.v1'?2:1;check(value.length<=depth+1);
}
const returnKeys=['returnId','parentFrameId','parentThreadId','childFrameId','childThreadId','delegationCycleId','delegationRecordHash','answerCycleId','childAnswerRecordHash','outputHash'];
function returning(value){exact(value,returnKeys);for(const key of returnKeys)if(key.endsWith('Hash'))digest(value[key]);else identity(value[key]);}
const recordKeys=['schemaVersion','sequence','previousRecordHash','descriptorHash','lineage','claim','kind','reference','contextHash','decisionsConsumed','frames','cycle','actionMetadata','retirementId','returnMetadata'];
const equal=(left,right)=>canonical(left)===canonical(right);
const frameIdentity=frame=>({frameId:frame.frameId,parentFrameId:frame.parentFrameId,partId:frame.partId,threadId:frame.threadId,taskHash:frame.taskHash,delegation:frame.delegation});
function prefix(old,next){check(equal(frameIdentity(old),frameIdentity(next))&&equal(next.observationSources.slice(0,old.observationsCount),old.observationSources));}
function emptyFrame(frame){check(frame.outputHash===null&&frame.observationsCount===0&&frame.observationsHash===partCycleSnapshotHash([])&&frame.observationSources.length===0);}
/** Validates the full metadata namespace before acquiring body-owner or provider factories. */
export function inspectPartCycleProgress(options){const {run}=options;const descriptor=descriptorFor(options),descriptorHash=canonical(descriptor);
 check(run&&['running','completed'].includes(run.status)&&run.resumedFromRunId===null&&run.workflowVersionId===descriptor.definitionId
  &&run.workflowType===options.definition.workflowType&&canonical(run.authorization?.assignedRuntime)===descriptor.runtimeBindingHash
  &&run.runFamilyBudget?.version!==3&&run.runFamilyBudgetVersion!==3);
 check(!run.events.some(event=>event.eventType==='workflow.assigned-composition/root-cycle.v1'||event.sourceKey?.startsWith('workflow.assigned-composition/root-cycle.v1')));
 const events=run.events.filter(event=>event.eventType===PART_CYCLE_PROGRESS_EVENT||event.sourceKey?.startsWith(PART_CYCLE_PROGRESS_EVENT));
 if(!events.length)return frozen({state:'absent',sequence:0,recordHash:null,lineage:null,reference:null,decisionsConsumed:0,frames:[],activeFrameId:null,observationSources:[]});
 check(events.length<=1+3*descriptor.limits.maximumDecisions);events.sort((left,right)=>left.payload.sequence-right.payload.sequence);
 let previous=null,accepted=null,state='absent';const cycleIds=new Set(),frameIds=new Set(),threadIds=new Set(),callIds=new Set(),returnIds=new Set();
 for(let index=0;index<events.length;index++){const event=events[index],record=event.payload;exact(record,recordKeys);
  check(record.schemaVersion===1&&record.sequence===index+1&&record.previousRecordHash===(previous?journalHash(previous):null)
   &&record.descriptorHash===descriptorHash&&event.payloadHash===journalHash(record)&&event.eventType===PART_CYCLE_PROGRESS_EVENT
   &&event.runId===run.runId&&event.sourceKey===`${PART_CYCLE_PROGRESS_EVENT}:${record.lineage.attemptId}:${record.sequence}`
   &&event.assignmentId===record.lineage.assignmentId&&event.attemptId===record.lineage.attemptId&&event.threadId===record.lineage.threadId);
  validateLineage(record.lineage,run,descriptor);check(normalizeExpectedClaim(record.claim)?.runId===run.runId);reference(record.reference,descriptor);digest(record.contextHash);
  integer(record.decisionsConsumed,0,descriptor.limits.maximumDecisions);frames(record.frames,descriptor,options,record.lineage);
  if(!previous){check(record.kind==='initialized'&&record.decisionsConsumed===0&&record.frames.length===1&&record.cycle===null&&record.actionMetadata===null&&record.retirementId===null&&record.returnMetadata===null);
   emptyFrame(record.frames[0]);frameIds.add(record.frames[0].frameId);threadIds.add(record.frames[0].threadId);state='ready';}
  else{check(equal(record.lineage,previous.lineage)&&record.contextHash===previous.contextHash);const old=previous.frames.at(-1),top=record.frames.at(-1);
   if(record.kind==='started'){check(state==='ready'&&equal(record.frames,previous.frames)&&equal(record.reference,previous.reference)&&record.decisionsConsumed===previous.decisionsConsumed+1
     &&record.actionMetadata===null&&record.retirementId===null&&record.returnMetadata===null);cycle(record.cycle,descriptor);
    check(record.cycle.frameId===old.frameId&&record.cycle.partId===old.partId&&record.cycle.threadId===old.threadId&&record.cycle.decisionsConsumed===record.decisionsConsumed&&equal(record.cycle.claim,record.claim)&&!cycleIds.has(record.cycle.cycleId));
    cycleIds.add(record.cycle.cycleId);state='started';
   }else if(record.kind==='accepted'){check(state==='started'&&equal(record.cycle,previous.cycle)&&equal(record.claim,previous.claim)&&record.decisionsConsumed===previous.decisionsConsumed&&record.returnMetadata===null);
    action(record.actionMetadata,options);identity(record.retirementId);const chosen=record.actionMetadata;check(chosen.frameId===old.frameId);
    if(chosen.kind==='child'){check(record.frames.length===previous.frames.length+1&&equal(record.frames.slice(0,-1),previous.frames)&&!frameIds.has(chosen.childFrameId)&&!threadIds.has(chosen.threadId));
     check(top.frameId===chosen.childFrameId&&equal(top.delegation,{cycle:record.cycle,actionMetadata:chosen,retirementId:record.retirementId,predecessorHash:record.previousRecordHash}));emptyFrame(top);
     frameIds.add(top.frameId);threadIds.add(top.threadId);state='ready';
    }else{check(record.frames.length===previous.frames.length&&equal(record.frames.slice(0,-1),previous.frames.slice(0,-1)));prefix(old,top);check(chosen.partId===old.partId);
     if(chosen.kind==='tool'){check(top.outputHash===null&&top.observationsCount===old.observationsCount+1&&equal(top.observationSources.at(-1).actionMetadata,chosen)&&!callIds.has(chosen.callId));callIds.add(chosen.callId);state='ready';}
     else{check(top.outputHash!==null&&top.observationsHash===old.observationsHash&&equal(top.observationSources,old.observationSources));state=record.frames.length===1?'answered':'returning';}
    }
   }else{check(record.kind==='returned'&&state==='returning'&&previous.frames.length>=2&&record.decisionsConsumed===previous.decisionsConsumed&&equal(record.cycle,previous.cycle));
    returning(record.returnMetadata);identity(record.retirementId);const returned=record.returnMetadata,parent=previous.frames.at(-2);
    check(!returnIds.has(returned.returnId)&&returned.parentFrameId===parent.frameId&&returned.parentThreadId===parent.threadId&&returned.childFrameId===old.frameId&&returned.childThreadId===old.threadId
     &&returned.delegationCycleId===old.delegation.cycle.cycleId&&returned.delegationRecordHash===old.delegation.predecessorHash&&returned.answerCycleId===previous.cycle.cycleId
     &&returned.childAnswerRecordHash===record.previousRecordHash&&returned.outputHash===old.outputHash&&equal(record.actionMetadata,old.delegation.actionMetadata));returnIds.add(returned.returnId);
    check(record.frames.length===previous.frames.length-1&&equal(record.frames.slice(0,-1),previous.frames.slice(0,-2)));prefix(parent,top);
    check(top.outputHash===null&&top.observationsCount===parent.observationsCount+1&&equal(top.observationSources.at(-1).actionMetadata,record.actionMetadata));state='ready';
   }
  }
  if(record.kind!=='started')accepted=record;previous=record;
 }
 const {attempt,assignment}=validateLineage(previous.lineage,run,descriptor);if(state!=='answered')check(attempt.outcome===null&&assignment.status==='running');
 return frozen({state,sequence:previous.sequence,recordHash:journalHash(previous),lineage:copy(previous.lineage),reference:copy(previous.reference),decisionsConsumed:previous.decisionsConsumed,
  frames:copy(previous.frames),activeFrameId:previous.frames.at(-1).frameId,observationSources:copy(previous.frames.flatMap(frame=>frame.observationSources)),cycle:copy(previous.cycle),claim:copy(previous.claim),previousRecordHash:previous.previousRecordHash,
  referenceCycle:copy(accepted.cycle),actionMetadata:copy(accepted.actionMetadata),retirementId:accepted.retirementId,returnMetadata:copy(accepted.returnMetadata),contextHash:previous.contextHash});
}
const snapshotFrameKeys=['frameId','parentFrameId','partId','threadId','task','observations','observationSources','output','delegation'];
function summarize(snapshot){return snapshot.frames.map(frame=>({frameId:frame.frameId,parentFrameId:frame.parentFrameId,partId:frame.partId,threadId:frame.threadId,
 taskHash:partCycleSnapshotHash(frame.task),observationsHash:partCycleSnapshotHash(frame.observations),observationsCount:frame.observations.length,observationSources:copy(frame.observationSources),outputHash:frame.output===null?null:partCycleSnapshotHash(frame.output),delegation:copy(frame.delegation)}));}
function snapshotBytes(snapshot,descriptor,options,lineage){exact(snapshot,['schemaVersion','variablesByPart','task','decisionsConsumed','frames']);const text=json(snapshot);
 check(snapshot.schemaVersion===1&&snapshot.variablesByPart&&Object.getPrototypeOf(snapshot.variablesByPart)===Object.prototype&&Object.keys(snapshot.variablesByPart).every(partId=>options.parts.some(part=>part.partId===partId))&&typeof snapshot.task==='string'&&snapshot.task.trim());
 integer(snapshot.decisionsConsumed,0,descriptor.limits.maximumDecisions);check(Array.isArray(snapshot.frames)&&snapshot.frames.length>=1&&snapshot.frames.length<=3);
 for(const frame of snapshot.frames){exact(frame,snapshotFrameKeys);check(typeof frame.task==='string'&&frame.task.trim()&&Array.isArray(frame.observations)&&(frame.output===null||Object.getPrototypeOf(frame.output)===Object.prototype));
  if(frame.parentFrameId!==null)check(Buffer.byteLength(json(frame.task))<=8000);check(Buffer.byteLength(json(frame.observations))<=descriptor.limits.maximumContextBytes);
  for(let index=0;index<frame.observations.length;index++){const observation=frame.observations[index],source=frame.observationSources[index];
   if(observation?.kind==='tool'){exact(observation,['kind','toolId','result']);identity(observation.toolId);check(source?.actionMetadata.kind==='tool'&&source.actionMetadata.toolId===observation.toolId);}
   else{exact(observation,['kind','partId','output']);check(observation.kind==='child'&&observation.output&&Object.getPrototypeOf(observation.output)===Object.prototype&&source?.actionMetadata.kind==='child'&&source.actionMetadata.partId===observation.partId);}
   check(partCycleSnapshotHash(observation)===source.observationHash);}
 }
 check(snapshot.frames[0].task===snapshot.task);frames(summarize(snapshot),descriptor,options,lineage);check(Buffer.byteLength(text)<=descriptor.bodyOwner.maximumBodyBytes);return text;
}
export function createPartCycleProgress({database,relations,journal,definition,parts,runtimeBinding,descriptor,limits,getDescriptor,authorize,bodyOwner}={}){
 check(typeof database?.transaction==='function'&&typeof journal?.appendEventInTransaction==='function');
 for(const fn of [getDescriptor,authorize,bodyOwner?.write,bodyOwner?.verify,bodyOwner?.attest,bodyOwner?.resolve])check(typeof fn==='function');
 const options=copy({definition,parts,runtimeBinding,descriptor,limits}),pinned=descriptorFor(options),descriptorHash=canonical(pinned);
 check(bodyOwner.ownerId===pinned.bodyOwner.ownerId&&bodyOwner.contractId===pinned.bodyOwner.contractId&&bodyOwner.contractVersion===pinned.bodyOwner.contractVersion);
 const sql=resolvePostgresRelations(relations,['runs','versions','assignments','attempts','events'],'Part cycle progress');
 const ownerFields=['ownerId','contractId','contractVersion','write','verify','attest','resolve'];
 const ownerIdentity=Object.fromEntries(ownerFields.map(key=>[key,bodyOwner[key]]));
 const current=()=>{check(canonical(partCycleRecoveryDescriptor(getDescriptor()))===canonical(options.descriptor));
  check(ownerFields.every(key=>bodyOwner[key]===ownerIdentity[key]));};
 function request(input){check(input&&Object.getPrototypeOf(input)===Object.prototype);const expectedClaim=normalizeExpectedClaim(input.expectedClaim);check(expectedClaim&&expectedClaim.runId===input.runId);identity(input.runId);return {...input,expectedClaim};}
 async function permit(input,operation,target={}){current();input.signal?.throwIfAborted();check(await authorize(frozen({operation,runId:input.runId,expectedClaim:copy(input.expectedClaim),descriptor:copy(pinned),...copy(target)}))===true);current();input.signal?.throwIfAborted();}
 async function custody(client,input,deadline){current();input.signal?.throwIfAborted();await assertPostgresExpectedClaim(client,sql,input.runId,input.expectedClaim);
  const row=(await client.query(`SELECT clock_timestamp() AS checked_at,EXISTS(SELECT 1 FROM ${sql.events} WHERE run_id=$1 AND source_key='workflow.cancellation-requested.v1') AS cancelled`,[input.runId])).rows[0];
  const time=row.checked_at instanceof Date?row.checked_at.getTime():Date.parse(row.checked_at);check(!row.cancelled&&Number.isFinite(time)&&(deadline===undefined||time<deadline));current();input.signal?.throwIfAborted();}
 async function load(client,input){
  const assignments=(await client.query(`SELECT * FROM ${sql.assignments} WHERE run_id=$1 ORDER BY assignment_ordinal FOR UPDATE`,[input.runId])).rows;
  const attempts=(await client.query(`SELECT * FROM ${sql.attempts} WHERE run_id=$1 ORDER BY attempt_ordinal FOR UPDATE`,[input.runId])).rows;
  const row=(await client.query(`SELECT * FROM ${sql.runs} WHERE id=$1 FOR UPDATE`,[input.runId])).rows[0];check(row&&row.status==='running'&&row.run_family_budget_version!==3);
  const version=(await client.query(`SELECT definition,definition_sha256 FROM ${sql.versions} WHERE id=$1`,[row.workflow_version_id])).rows[0];
  check(version&&version.definition_sha256===options.definition.definitionHash&&workflowDefinitionHash(version.definition)===options.definition.definitionHash);
  const events=(await client.query(`SELECT * FROM ${sql.events} WHERE run_id=$1 ORDER BY received_at,id`,[input.runId])).rows;
  const run={runId:row.id,status:row.status,workflowVersionId:row.workflow_version_id,workflowType:row.workflow_type,resumedFromRunId:row.resumed_from_run_id,
   authorization:row.authorization_snapshot,runFamilyBudgetVersion:row.run_family_budget_version,events:events.map(event=>({runId:event.run_id,assignmentId:event.assignment_id,attemptId:event.attempt_id,
    sourceKey:event.source_key,eventType:event.event_type,threadId:event.thread_id,payload:event.payload,payloadHash:event.payload_sha256})),assignments:assignments.map(assignment=>({assignmentId:assignment.id,
    parentAssignmentId:assignment.parent_assignment_id,stepKey:assignment.step_key,execution:assignment.execution,status:assignment.status,agentThreadId:assignment.agent_thread_id,attempts:attempts.filter(attempt=>attempt.assignment_id===assignment.id).map(attempt=>({
     attemptId:attempt.id,assignmentId:attempt.assignment_id,ordinal:Number(attempt.attempt_ordinal),threadId:attempt.thread_id,prompt:attempt.prompt,promptHash:attempt.prompt_sha256,
     capabilityProfile:attempt.capability_profile,capabilityHash:attempt.capability_sha256,outcome:attempt.outcome,telemetryStatus:attempt.telemetry_status}))}))};
  const metadata=inspectPartCycleProgress({...options,run});await custody(client,input,metadata.lineage?.deadlineAtMs);return {run,metadata};
 }
 async function read(input){return database.transaction(client=>load(client,input));}
 async function guarded(input,target){check(typeof input.guard==='function');current();input.signal?.throwIfAborted();check(await input.guard(frozen(copy(target)))===true);current();input.signal?.throwIfAborted();await read(input);
  if(target.lineage)await database.transaction(client=>custody(client,input,target.lineage.deadlineAtMs));}
 function targetFor(input,metadata,extra={}){return frozen({operation:extra.operation??'restore',runId:input.runId,lineage:copy(metadata.lineage),descriptor:copy(pinned),descriptorHash,
  expectedRecordHash:metadata.recordHash,frames:copy(metadata.frames),observationSources:copy(metadata.observationSources),reference:copy(metadata.reference),
  referenceCycle:copy(metadata.referenceCycle??null),referenceActionMetadata:copy(metadata.actionMetadata??null),referenceRetirementId:metadata.retirementId??null,referenceReturnMetadata:copy(metadata.returnMetadata??null),
  cycle:copy(extra.operation==='accept'?metadata.cycle:metadata.referenceCycle??null),actionMetadata:copy(metadata.actionMetadata??null),retirementId:metadata.retirementId??null,returnMetadata:copy(metadata.returnMetadata??null),...copy(extra)});}
 async function resolve(input,metadata){const target=targetFor(input,metadata);await guarded(input,target);check(await bodyOwner.verify(target,{guard:()=>guarded(input,target),signal:input.signal})===true);await guarded(input,target);
  check(await bodyOwner.attest(target,{guard:()=>guarded(input,target),signal:input.signal})===true);await guarded(input,target);
  const text=await bodyOwner.resolve(target,{guard:()=>guarded(input,target),signal:input.signal});await guarded(input,target);
  check(typeof text==='string'&&Buffer.byteLength(text)===metadata.reference.byteLength&&partCycleSnapshotHash(text)===metadata.reference.sha256);const snapshot=JSON.parse(text);
  check(snapshotBytes(snapshot,pinned,options,metadata.lineage)===text&&partCycleSnapshotHash({variablesByPart:snapshot.variablesByPart,task:snapshot.task})===metadata.contextHash
   &&snapshot.decisionsConsumed===metadata.decisionsConsumed&&equal(summarize(snapshot),metadata.frames));return frozen(snapshot);
 }
 async function append(input,expectedRecordHash,build){return database.transaction(async client=>{
  const {run,metadata}=await load(client,input);const result=build({run,metadata});
  const commitGuard=()=>{check(typeof input.commitGuard==='function'&&input.commitGuard()===true);};
  if(result.retry){commitGuard();return frozen({...copy(metadata),newlyRecorded:false,newlyStarted:false});}
  check(metadata.recordHash===expectedRecordHash);const record={...result.record,schemaVersion:1,sequence:metadata.sequence+1,previousRecordHash:metadata.recordHash,descriptorHash,claim:copy(input.expectedClaim)};
  await custody(client,input,record.lineage.deadlineAtMs);
  commitGuard();
  await journal.appendEventInTransaction(client,{runId:input.runId,assignmentId:record.lineage.assignmentId,attemptId:record.lineage.attemptId,threadId:record.lineage.threadId,
   sourceKey:`${PART_CYCLE_PROGRESS_EVENT}:${record.lineage.attemptId}:${record.sequence}`,eventType:PART_CYCLE_PROGRESS_EVENT,payload:record,expectedClaim:input.expectedClaim});
  await custody(client,input,record.lineage.deadlineAtMs);const updated=await load(client,input);commitGuard();return frozen({...copy(updated.metadata),newlyRecorded:true,newlyStarted:record.kind==='started'});
 });}
 const baseRecord=metadata=>({lineage:copy(metadata.lineage),reference:copy(metadata.reference),contextHash:metadata.contextHash,decisionsConsumed:metadata.decisionsConsumed,
  frames:copy(metadata.frames),cycle:copy(metadata.cycle??null),actionMetadata:null,retirementId:null,returnMetadata:null});
 function proposed(run,metadata,input,record,snapshot){const text=snapshotBytes(snapshot,pinned,options,record.lineage);const candidate={...record,schemaVersion:1,sequence:metadata.sequence+1,previousRecordHash:metadata.recordHash,descriptorHash,claim:copy(input.expectedClaim),
   reference:{ownerId:pinned.bodyOwner.ownerId,contractId:pinned.bodyOwner.contractId,referenceId:'candidate',sha256:partCycleSnapshotHash(text),byteLength:Buffer.byteLength(text)}};
  inspectPartCycleProgress({...options,run:{...run,events:[...run.events,{runId:input.runId,assignmentId:candidate.lineage.assignmentId,attemptId:candidate.lineage.attemptId,threadId:candidate.lineage.threadId,
   sourceKey:`${PART_CYCLE_PROGRESS_EVENT}:${candidate.lineage.attemptId}:${candidate.sequence}`,eventType:PART_CYCLE_PROGRESS_EVENT,payload:candidate,payloadHash:journalHash(candidate)}]}});return text;}
 async function store(input,metadata,snapshot,extra){const text=snapshotBytes(snapshot,pinned,options,metadata.lineage);const summary=summarize(snapshot);
  const target=targetFor(input,metadata,{...extra,frames:summary,observationSources:summary.flatMap(frame=>frame.observationSources),snapshot:copy(snapshot),json:text,sha256:partCycleSnapshotHash(text),byteLength:Buffer.byteLength(text)});
  await guarded(input,target);const ref=await bodyOwner.write(target,{guard:()=>guarded(input,target),signal:input.signal});reference(ref,pinned);check(ref.sha256===target.sha256&&ref.byteLength===target.byteLength);await guarded(input,target);
  const complete=frozen({...copy(target),reference:copy(ref)});check(await bodyOwner.verify(complete,{guard:()=>guarded(input,complete),signal:input.signal})===true);await guarded(input,complete);
  check(await bodyOwner.attest(complete,{guard:()=>guarded(input,complete),signal:input.signal})===true);await guarded(input,complete);return copy(ref);}
 const safe=method=>async input=>{try{return await method(request(input));}catch(error){if(error?.code==='WORKFLOW_CLAIM_LOST')throw claimLostError();throw fail();}};
 function commit(input){check(typeof input.commitGuard==='function'&&input.commitGuard()===true);}
 return Object.freeze({descriptorForRun:()=>copy(pinned),
  inspect:safe(async input=>{await permit(input,'inspect');return (await read(input)).metadata;}),
  restore:safe(async input=>{await permit(input,'restore');const {metadata}=await read(input);check(['ready','returning','answered'].includes(metadata.state)&&metadata.recordHash===input.expectedRecordHash);
   const snapshot=await resolve(input,metadata);check((await read(input)).metadata.recordHash===metadata.recordHash);return snapshot;}),
  initialize:safe(async input=>{await permit(input,'initialize');const {run,metadata}=await read(input);validateLineage(input.lineage,run,pinned);
   check(run.assignments[0].status==='running'&&run.assignments[0].attempts[0].outcome===null&&input.snapshot.task===run.assignments[0].attempts[0].prompt
    &&input.snapshot.decisionsConsumed===0&&input.snapshot.frames.length===1);snapshotBytes(input.snapshot,pinned,options,input.lineage);
   const initial={...metadata,lineage:copy(input.lineage)};const record={lineage:copy(input.lineage),kind:'initialized',contextHash:partCycleSnapshotHash({variablesByPart:input.snapshot.variablesByPart,task:input.snapshot.task}),
    decisionsConsumed:0,frames:summarize(input.snapshot),cycle:null,actionMetadata:null,retirementId:null,returnMetadata:null};
   if(metadata.state!=='absent'){check(metadata.sequence===1&&equal(metadata.lineage,input.lineage)&&partCycleSnapshotHash(input.snapshot)===metadata.reference.sha256&&equal(metadata.claim,input.expectedClaim));await guarded(input,targetFor(input,metadata));commit(input);return frozen({...copy(metadata),newlyRecorded:false,newlyStarted:false});}
   proposed(run,metadata,input,record,input.snapshot);const ref=await store(input,initial,input.snapshot,{operation:'initialize'});
   return append(input,null,({metadata:latest})=>{check(latest.state==='absent');return {record:{...record,reference:ref}};});}),
  start:safe(async input=>{await permit(input,'start');identity(input.frameId);identity(input.cycleId);digest(input.inputHash);integer(input.inputBytes,1,pinned.limits.maximumContextBytes);integer(input.decisionsConsumed,1,pinned.limits.maximumDecisions);check(typeof input.commitGuard==='function');
   const currentMetadata=(await read(input)).metadata;await guarded(input,targetFor(input,currentMetadata));
   return append(input,input.expectedRecordHash,({metadata})=>{if(metadata.state==='started'&&metadata.cycle.cycleId===input.cycleId){check(metadata.previousRecordHash===input.expectedRecordHash&&metadata.cycle.frameId===input.frameId&&metadata.cycle.inputHash===input.inputHash&&metadata.cycle.inputBytes===input.inputBytes
      &&metadata.decisionsConsumed===input.decisionsConsumed&&equal(metadata.cycle.claim,input.expectedClaim));return {retry:true};}
    check(metadata.state==='ready'&&metadata.activeFrameId===input.frameId&&input.decisionsConsumed===metadata.decisionsConsumed+1);const top=metadata.frames.at(-1);
    return {record:{...baseRecord(metadata),kind:'started',decisionsConsumed:input.decisionsConsumed,cycle:{cycleId:input.cycleId,frameId:top.frameId,partId:top.partId,threadId:top.threadId,
     inputHash:input.inputHash,inputBytes:input.inputBytes,decisionsConsumed:input.decisionsConsumed,claim:copy(input.expectedClaim)}}};});}),
  accept:safe(async input=>{await permit(input,'accept');action(input.actionMetadata,options);identity(input.retirementId);identity(input.frameId);const {run,metadata}=await read(input);
   if(['ready','returning','answered'].includes(metadata.state)&&metadata.cycle?.cycleId===input.cycleId&&metadata.returnMetadata===null){check(partCycleSnapshotHash(input.snapshot)===metadata.reference.sha256&&equal(input.actionMetadata,metadata.actionMetadata)&&input.retirementId===metadata.retirementId
      &&input.frameId===metadata.cycle.frameId&&input.expectedRecordHash===metadata.previousRecordHash&&equal(input.expectedClaim,metadata.cycle.claim));await guarded(input,targetFor(input,metadata));commit(input);return frozen({...copy(metadata),newlyRecorded:false,newlyStarted:false});}
   check(metadata.state==='started'&&metadata.recordHash===input.expectedRecordHash&&metadata.cycle.cycleId===input.cycleId&&metadata.cycle.frameId===input.frameId&&equal(metadata.cycle.claim,input.expectedClaim));
   const old=await resolve(input,{...metadata,state:'ready',decisionsConsumed:metadata.decisionsConsumed-1});const snapshot=input.snapshot;snapshotBytes(snapshot,pinned,options,metadata.lineage);
   check(partCycleSnapshotHash({variablesByPart:snapshot.variablesByPart,task:snapshot.task})===metadata.contextHash&&snapshot.decisionsConsumed===metadata.decisionsConsumed);
   const topIndex=old.frames.length-1;check(equal(snapshot.frames.slice(0,topIndex),old.frames.slice(0,topIndex)));
   const oldTop=old.frames.at(-1),nextTop=snapshot.frames[topIndex];check(partCycleSnapshotHash(nextTop.observations.slice(0,oldTop.observations.length))===partCycleSnapshotHash(oldTop.observations));
   const record={...baseRecord(metadata),kind:'accepted',frames:summarize(snapshot),actionMetadata:copy(input.actionMetadata),retirementId:input.retirementId};proposed(run,metadata,input,record,snapshot);
   const ref=await store(input,metadata,snapshot,{operation:'accept',actionMetadata:input.actionMetadata,retirementId:input.retirementId,returnMetadata:null});
   return append(input,input.expectedRecordHash,({metadata:latest})=>{check(latest.state==='started'&&latest.cycle.cycleId===input.cycleId);return {record:{...record,reference:ref}};});}),
  returnChild:safe(async input=>{await permit(input,'return');identity(input.childFrameId);identity(input.returnId);identity(input.retirementId);const {run,metadata}=await read(input);
   if(metadata.state==='ready'&&metadata.returnMetadata?.returnId===input.returnId){check(metadata.returnMetadata.childFrameId===input.childFrameId&&metadata.previousRecordHash===input.expectedRecordHash&&metadata.retirementId===input.retirementId&&equal(metadata.claim,input.expectedClaim));
    await guarded(input,targetFor(input,metadata));commit(input);return frozen({...copy(metadata),newlyRecorded:false,newlyStarted:false});}
   check(metadata.state==='returning'&&metadata.recordHash===input.expectedRecordHash&&metadata.activeFrameId===input.childFrameId);const old=await resolve(input,metadata),snapshot=copy(old),child=snapshot.frames.pop(),parent=snapshot.frames.at(-1);
   const observation={kind:'child',partId:child.partId,output:copy(child.output)};parent.observations.push(observation);parent.observationSources.push({actionMetadata:copy(child.delegation.actionMetadata),observationHash:partCycleSnapshotHash(observation)});
   const returnMetadata={returnId:input.returnId,parentFrameId:parent.frameId,parentThreadId:parent.threadId,childFrameId:child.frameId,childThreadId:child.threadId,
    delegationCycleId:child.delegation.cycle.cycleId,delegationRecordHash:child.delegation.predecessorHash,answerCycleId:metadata.cycle.cycleId,childAnswerRecordHash:metadata.recordHash,outputHash:partCycleSnapshotHash(child.output)};
   const record={...baseRecord(metadata),kind:'returned',frames:summarize(snapshot),actionMetadata:copy(child.delegation.actionMetadata),retirementId:input.retirementId,returnMetadata};proposed(run,metadata,input,record,snapshot);
   const ref=await store(input,metadata,snapshot,{operation:'return',cycle:metadata.cycle,actionMetadata:record.actionMetadata,retirementId:input.retirementId,returnMetadata});
   return append(input,input.expectedRecordHash,({metadata:latest})=>{check(latest.state==='returning'&&latest.recordHash===metadata.recordHash);return {record:{...record,reference:ref}};});})});
}
