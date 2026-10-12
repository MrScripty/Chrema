import {createHash} from 'node:crypto';
import {resolvePostgresRelations} from './postgres-relations.mjs';
import {normalizeExpectedClaim,assertPostgresExpectedClaim,claimLostError} from './agent-platform-claim-bound-journal.mjs';
import {workflowDefinitionHash} from './agent-platform-prompt.mjs';

export const ROOT_CYCLE_PROGRESS_EVENT='workflow.assigned-composition/root-cycle.v1';
const copy=value=>structuredClone(value);
const canonical=value=>workflowDefinitionHash({value});
// The journal hashes the payload itself; runtime binding identities use a value wrapper.
function journalJson(value){if(Array.isArray(value))return `[${value.map(journalJson).join(',')}]`;
 if(value&&typeof value==='object')return `{${Object.keys(value).sort().map(key=>`${JSON.stringify(key)}:${journalJson(value[key])}`).join(',')}}`;
 return JSON.stringify(value);}
const journalHash=value=>createHash('sha256').update(journalJson(value)).digest('hex');
const fail=(code='ROOT_CYCLE_PROGRESS_UNAVAILABLE')=>Object.assign(new Error('Root cycle progress is unavailable.'),{code,statusCode:409});
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
export function rootCycleSnapshotHash(value){return createHash('sha256').update(typeof value==='string'?value:json(value)).digest('hex');}
export function rootCycleRecoveryDescriptor(value){
 exact(value,['schemaVersion','mode','writerId','maximumRecoveries','bodyOwner']);
 check(value.schemaVersion===1&&value.mode==='accepted-root-cycles.v1'&&value.writerId==='restricted-host-parts.v1');integer(value.maximumRecoveries,1,3);
 exact(value.bodyOwner,['ownerId','contractId','contractVersion','maximumBodyBytes']);identity(value.bodyOwner.ownerId);identity(value.bodyOwner.contractId);
 integer(value.bodyOwner.contractVersion,1,1000000);integer(value.bodyOwner.maximumBodyBytes,1024,512000);return frozen(copy(value));
}
function descriptorFor({definition,parts,runtimeBinding,descriptor,limits}){
 const policy=rootCycleRecoveryDescriptor(descriptor);const root=parts.find(part=>part.parentPartId===null);
 check(root&&parts.filter(part=>part.parentPartId===null).length===1&&definition.definitionHash===workflowDefinitionHash(definition)
  &&runtimeBinding.definitionId===definition.id&&runtimeBinding.definitionHash===definition.definitionHash&&runtimeBinding.rootPartId===root.partId);
 exact(limits,['maximumDecisions','maximumContextBytes','maxRunDurationMs']);integer(limits.maximumDecisions,1,32);integer(limits.maximumContextBytes,1024,128000);integer(limits.maxRunDurationMs,1,120000);
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
function action(value,options){const root=options.parts.find(part=>part.parentPartId===null);
 if(value?.kind==='tool'){exact(value,['kind','partId','toolId','bindingId','callId']);check(value.partId===root.partId);identity(value.toolId);identity(value.callId);digest(value.bindingId);
  const assigned=options.definition.toolAssignments?.parts?.find(part=>part.partId===root.partId);check(assigned?.tools.some(tool=>tool.toolId===value.toolId&&tool.bindingId===value.bindingId));}
 else if(value?.kind==='child'){exact(value,['kind','partId','threadId']);identity(value.threadId);check(options.parts.some(part=>part.partId===value.partId&&part.parentPartId===root.partId));}
 else{exact(value,['kind']);check(value.kind==='answer');}
}
const recordKeys=['schemaVersion','sequence','previousRecordHash','descriptorHash','lineage','claim','kind','reference','contextHash','observationsHash','observationsCount','decisionsConsumed','observationSources','cycle','actionMetadata','retirementId'];
/** Metadata validation is intentionally independent of body-owner/provider factories. */
export function inspectRootCycleProgress(options){
 const {run}=options;const descriptor=descriptorFor(options),descriptorHash=canonical(descriptor);
 check(run&&['running','completed'].includes(run.status)&&run.resumedFromRunId===null&&run.workflowVersionId===descriptor.definitionId
  &&run.workflowType===options.definition.workflowType&&canonical(run.authorization?.assignedRuntime)===descriptor.runtimeBindingHash
  &&run.runFamilyBudget?.version!==3&&run.runFamilyBudgetVersion!==3);
 const events=run.events.filter(event=>event.eventType===ROOT_CYCLE_PROGRESS_EVENT||event.sourceKey?.startsWith(ROOT_CYCLE_PROGRESS_EVENT));
 if(!events.length)return frozen({state:'absent',sequence:0,recordHash:null,lineage:null,reference:null,decisionsConsumed:0,observationSources:[]});
 check(events.length<=1+2*descriptor.limits.maximumDecisions);
 events.sort((left,right)=>left.payload.sequence-right.payload.sequence);
 let previous=null,accepted=null,state='absent';
 for(let index=0;index<events.length;index++){
  const event=events[index],record=event.payload;exact(record,recordKeys);
  check(record.schemaVersion===1&&record.sequence===index+1&&record.previousRecordHash===(previous?journalHash(previous):null)
   &&record.descriptorHash===descriptorHash&&event.payloadHash===journalHash(record)&&event.eventType===ROOT_CYCLE_PROGRESS_EVENT
   &&event.runId===run.runId&&event.sourceKey===`${ROOT_CYCLE_PROGRESS_EVENT}:${record.lineage.attemptId}:${record.sequence}`
   &&event.assignmentId===record.lineage.assignmentId&&event.attemptId===record.lineage.attemptId&&event.threadId===record.lineage.threadId);
  validateLineage(record.lineage,run,descriptor);check(normalizeExpectedClaim(record.claim)?.runId===run.runId);
  reference(record.reference,descriptor);digest(record.contextHash);digest(record.observationsHash);integer(record.observationsCount,0,descriptor.limits.maximumDecisions);
  integer(record.decisionsConsumed,0,descriptor.limits.maximumDecisions);check(Array.isArray(record.observationSources)&&record.observationSources.length===record.observationsCount);
  for(const source of record.observationSources){exact(source,['actionMetadata','observationHash']);action(source.actionMetadata,options);check(source.actionMetadata.kind!=='answer');digest(source.observationHash);}
  if(!previous){check(record.kind==='initialized'&&record.decisionsConsumed===0&&record.observationsCount===0&&record.observationsHash===rootCycleSnapshotHash([])
   &&record.cycle===null&&record.actionMetadata===null&&record.retirementId===null);state='ready';}
  else{
   check(canonical(record.lineage)===canonical(previous.lineage)&&record.contextHash===previous.contextHash);
   if(record.kind==='started'){
    check(state==='ready'&&canonical(record.reference)===canonical(previous.reference)&&record.observationsHash===previous.observationsHash
     &&canonical(record.observationSources)===canonical(previous.observationSources)&&record.observationsCount===previous.observationsCount
     &&record.decisionsConsumed===previous.decisionsConsumed+1&&record.actionMetadata===null&&record.retirementId===null);
    exact(record.cycle,['cycleId','inputHash','inputBytes','decisionsConsumed','claim']);identity(record.cycle.cycleId);digest(record.cycle.inputHash);integer(record.cycle.inputBytes,1,descriptor.limits.maximumContextBytes);
    check(record.cycle.decisionsConsumed===record.decisionsConsumed&&canonical(record.cycle.claim)===canonical(record.claim));
    check(!events.slice(0,index).some(other=>other.payload.kind==='started'&&other.payload.cycle.cycleId===record.cycle.cycleId));state='started';
   }else{
    check(state==='started'&&record.kind==='accepted'&&canonical(record.cycle)===canonical(previous.cycle)&&canonical(record.claim)===canonical(previous.claim));
    identity(record.retirementId);action(record.actionMetadata,options);const answered=record.actionMetadata.kind==='answer';
    check(record.decisionsConsumed>=(record.actionMetadata.kind==='child'?previous.decisionsConsumed+1:previous.decisionsConsumed)
     &&(record.actionMetadata.kind==='child'||record.decisionsConsumed===previous.decisionsConsumed)
     &&record.observationsCount===previous.observationsCount+(answered?0:1)
     &&canonical(record.observationSources.slice(0,previous.observationsCount))===canonical(previous.observationSources));
    if(answered)check(record.observationsHash===previous.observationsHash&&canonical(record.observationSources)===canonical(previous.observationSources));
    else check(canonical(record.observationSources.at(-1).actionMetadata)===canonical(record.actionMetadata));state=answered?'answered':'ready';
   }
  }
  if(record.kind!=='started')accepted=record;
  previous=record;
 }
 const {attempt,assignment}=validateLineage(previous.lineage,run,descriptor);
 if(state!=='answered')check(attempt.outcome===null&&assignment.status==='running');
 return frozen({state,sequence:previous.sequence,recordHash:journalHash(previous),lineage:copy(previous.lineage),reference:copy(previous.reference),decisionsConsumed:previous.decisionsConsumed,
  observationSources:copy(previous.observationSources),cycle:copy(previous.cycle),claim:copy(previous.claim),previousRecordHash:previous.previousRecordHash,
  referenceCycle:copy(accepted.cycle),actionMetadata:copy(accepted.actionMetadata),retirementId:accepted.retirementId,
  contextHash:previous.contextHash,observationsHash:previous.observationsHash,observationsCount:previous.observationsCount});
}
function snapshotBytes(snapshot,descriptor,parts){exact(snapshot,['schemaVersion','variablesByPart','task','observations','decisionsConsumed','output']);
 check(snapshot.schemaVersion===1&&snapshot.variablesByPart&&Object.getPrototypeOf(snapshot.variablesByPart)===Object.prototype
  &&Object.keys(snapshot.variablesByPart).every(partId=>parts.some(part=>part.partId===partId))&&typeof snapshot.task==='string'&&snapshot.task.trim()
  &&Array.isArray(snapshot.observations)&&(snapshot.output===null||Object.getPrototypeOf(snapshot.output)===Object.prototype));
 integer(snapshot.decisionsConsumed,0,descriptor.limits.maximumDecisions);
 for(const observation of snapshot.observations){if(observation?.kind==='tool'){exact(observation,['kind','toolId','result']);identity(observation.toolId);}
  else{exact(observation,['kind','partId','output']);check(observation.kind==='child'&&parts.some(part=>part.partId===observation.partId)&&observation.output&&Object.getPrototypeOf(observation.output)===Object.prototype);}}
 const text=json(snapshot);check(Buffer.byteLength(text)<=descriptor.bodyOwner.maximumBodyBytes&&Buffer.byteLength(json(snapshot.observations))<=descriptor.limits.maximumContextBytes);return text;
}
/** All bodies remain in the explicitly installed owner; SQL holds only linked metadata. */
export function createRootCycleProgress({database,relations,journal,definition,parts,runtimeBinding,descriptor,limits,getDescriptor,authorize,bodyOwner}={}){
 check(typeof database?.transaction==='function'&&typeof journal?.appendEventInTransaction==='function');
 for(const fn of [getDescriptor,authorize,bodyOwner?.write,bodyOwner?.verify,bodyOwner?.attest,bodyOwner?.resolve])check(typeof fn==='function');
 const options=copy({definition,parts,runtimeBinding,descriptor,limits}),pinned=descriptorFor(options),descriptorHash=canonical(pinned);
 check(bodyOwner.ownerId===pinned.bodyOwner.ownerId&&bodyOwner.contractId===pinned.bodyOwner.contractId&&bodyOwner.contractVersion===pinned.bodyOwner.contractVersion);
 const sql=resolvePostgresRelations(relations,['runs','versions','assignments','attempts','events'],'Root cycle progress');
 const ownerFields=['ownerId','contractId','contractVersion','write','verify','attest','resolve'];
 const ownerIdentity=Object.fromEntries(ownerFields.map(key=>[key,bodyOwner[key]]));
 const current=()=>{check(canonical(rootCycleRecoveryDescriptor(getDescriptor()))===canonical(options.descriptor));
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
  const metadata=inspectRootCycleProgress({...options,run});await custody(client,input,metadata.lineage?.deadlineAtMs);return {run,metadata};
 }
 async function read(input){return database.transaction(client=>load(client,input));}
 async function guarded(input,target){check(typeof input.guard==='function');current();input.signal?.throwIfAborted();check(await input.guard(frozen(copy(target)))===true);current();input.signal?.throwIfAborted();await read(input);
  if(target.lineage)await database.transaction(client=>custody(client,input,target.lineage.deadlineAtMs));}
 function targetFor(input,metadata,extra={}){return frozen({operation:extra.operation??'restore',runId:input.runId,lineage:copy(metadata.lineage),descriptor:copy(pinned),descriptorHash,
  expectedRecordHash:metadata.recordHash,cycle:copy(extra.operation==='accept'?metadata.cycle:metadata.referenceCycle??null),actionMetadata:copy(metadata.actionMetadata??null),retirementId:metadata.retirementId??null,
  observationSources:copy(metadata.observationSources),reference:copy(metadata.reference),...copy(extra)});}
 async function resolve(input,metadata){const target=targetFor(input,metadata);await guarded(input,target);check(await bodyOwner.verify(target,{guard:()=>guarded(input,target),signal:input.signal})===true);await guarded(input,target);
  check(await bodyOwner.attest(target,{guard:()=>guarded(input,target),signal:input.signal})===true);await guarded(input,target);
  const text=await bodyOwner.resolve(target,{guard:()=>guarded(input,target),signal:input.signal});await guarded(input,target);
  check(typeof text==='string'&&Buffer.byteLength(text)===metadata.reference.byteLength&&rootCycleSnapshotHash(text)===metadata.reference.sha256);
  const snapshot=JSON.parse(text);check(snapshotBytes(snapshot,pinned,options.parts)===text&&rootCycleSnapshotHash({variablesByPart:snapshot.variablesByPart,task:snapshot.task})===metadata.contextHash
   &&rootCycleSnapshotHash(snapshot.observations)===metadata.observationsHash&&snapshot.observations.length===metadata.observationsCount
   &&snapshot.decisionsConsumed===metadata.decisionsConsumed&&((metadata.state==='answered')===(snapshot.output!==null)));
  for(let index=0;index<snapshot.observations.length;index++)check(rootCycleSnapshotHash(snapshot.observations[index])===metadata.observationSources[index].observationHash);
  return frozen(snapshot);
 }
 async function append(input,expectedRecordHash,build){return database.transaction(async client=>{
  const {run,metadata}=await load(client,input);const result=build({run,metadata});
  const commitGuard=()=>{if(input.commitGuard!==undefined)check(typeof input.commitGuard==='function'&&input.commitGuard()===true);};
  if(result.retry){commitGuard();return frozen({...copy(metadata),newlyRecorded:false,newlyStarted:false});}
  check(metadata.recordHash===expectedRecordHash);const record={...result.record,schemaVersion:1,sequence:metadata.sequence+1,previousRecordHash:metadata.recordHash,descriptorHash,claim:copy(input.expectedClaim)};
  await custody(client,input,record.lineage.deadlineAtMs);
  commitGuard();
  await journal.appendEventInTransaction(client,{runId:input.runId,assignmentId:record.lineage.assignmentId,attemptId:record.lineage.attemptId,threadId:record.lineage.threadId,
   sourceKey:`${ROOT_CYCLE_PROGRESS_EVENT}:${record.lineage.attemptId}:${record.sequence}`,eventType:ROOT_CYCLE_PROGRESS_EVENT,payload:record,expectedClaim:input.expectedClaim});
  await custody(client,input,record.lineage.deadlineAtMs);const updated=await load(client,input);commitGuard();return frozen({...copy(updated.metadata),newlyRecorded:true,newlyStarted:record.kind==='started'});
 });}
 const baseRecord=metadata=>({lineage:copy(metadata.lineage),reference:copy(metadata.reference),contextHash:metadata.contextHash,observationsHash:metadata.observationsHash,
  observationsCount:metadata.observationsCount,decisionsConsumed:metadata.decisionsConsumed,observationSources:copy(metadata.observationSources),cycle:copy(metadata.cycle??null),actionMetadata:null,retirementId:null});
 async function store(input,metadata,snapshot,extra){const text=snapshotBytes(snapshot,pinned,options.parts);const target=targetFor(input,metadata,{...extra,snapshot:copy(snapshot),json:text,sha256:rootCycleSnapshotHash(text),byteLength:Buffer.byteLength(text)});
  await guarded(input,target);const ref=await bodyOwner.write(target,{guard:()=>guarded(input,target),signal:input.signal});reference(ref,pinned);check(ref.sha256===target.sha256&&ref.byteLength===target.byteLength);await guarded(input,target);
  const complete=frozen({...copy(target),reference:copy(ref)});check(await bodyOwner.verify(complete,{guard:()=>guarded(input,complete),signal:input.signal})===true);await guarded(input,complete);
  check(await bodyOwner.attest(complete,{guard:()=>guarded(input,complete),signal:input.signal})===true);await guarded(input,complete);return copy(ref);
 }
 const safe=method=>async input=>{try{return await method(request(input));}catch(error){if(error?.code==='WORKFLOW_CLAIM_LOST')throw claimLostError();throw fail();}};
 return Object.freeze({descriptorForRun:()=>copy(pinned),
  inspect:safe(async input=>{await permit(input,'inspect');return (await read(input)).metadata;}),
  restore:safe(async input=>{await permit(input,'restore');const {metadata}=await read(input);check(['ready','answered'].includes(metadata.state)&&metadata.recordHash===input.expectedRecordHash);const snapshot=await resolve(input,metadata);
   const after=(await read(input)).metadata;check(after.recordHash===metadata.recordHash);return snapshot;}),
  initialize:safe(async input=>{await permit(input,'initialize');const {run,metadata}=await read(input);check(input.snapshot.decisionsConsumed===0&&input.snapshot.observations.length===0&&input.snapshot.output===null);
   validateLineage(input.lineage,run,pinned);check(run.assignments[0].status==='running'&&run.assignments[0].attempts[0].outcome===null&&input.snapshot.task===run.assignments[0].attempts[0].prompt);
   if(metadata.state!=='absent'){check(metadata.sequence===1&&canonical(metadata.lineage)===canonical(input.lineage)&&rootCycleSnapshotHash(input.snapshot)===metadata.reference.sha256
     &&canonical(metadata.claim)===canonical(input.expectedClaim));await guarded(input,targetFor(input,metadata));return frozen({...copy(metadata),newlyRecorded:false,newlyStarted:false});}
   const initial={...metadata,lineage:copy(input.lineage)};const ref=await store(input,initial,input.snapshot,{operation:'initialize'});
   return append(input,null,({metadata:latest})=>{check(latest.state==='absent');return {record:{lineage:copy(input.lineage),kind:'initialized',reference:ref,contextHash:rootCycleSnapshotHash({variablesByPart:input.snapshot.variablesByPart,task:input.snapshot.task}),
    observationsHash:rootCycleSnapshotHash([]),observationsCount:0,decisionsConsumed:0,observationSources:[],cycle:null,actionMetadata:null,retirementId:null}};});}),
  start:safe(async input=>{await permit(input,'start');identity(input.cycleId);digest(input.inputHash);integer(input.inputBytes,1,pinned.limits.maximumContextBytes);integer(input.decisionsConsumed,1,pinned.limits.maximumDecisions);
   check(typeof input.commitGuard==='function');const currentMetadata=(await read(input)).metadata;await guarded(input,targetFor(input,currentMetadata));
   return append(input,input.expectedRecordHash,({metadata})=>{if(metadata.state==='started'&&metadata.cycle.cycleId===input.cycleId){check(metadata.previousRecordHash===input.expectedRecordHash&&metadata.cycle.inputHash===input.inputHash&&metadata.cycle.inputBytes===input.inputBytes&&metadata.decisionsConsumed===input.decisionsConsumed);return {retry:true};}
    check(metadata.state==='ready'&&input.decisionsConsumed===metadata.decisionsConsumed+1);return {record:{...baseRecord(metadata),kind:'started',decisionsConsumed:input.decisionsConsumed,
     cycle:{cycleId:input.cycleId,inputHash:input.inputHash,inputBytes:input.inputBytes,decisionsConsumed:input.decisionsConsumed,claim:copy(input.expectedClaim)}}};});}),
  accept:safe(async input=>{await permit(input,'accept');action(input.actionMetadata,options);identity(input.retirementId);const {metadata}=await read(input);
   if(['ready','answered'].includes(metadata.state)&&metadata.cycle?.cycleId===input.cycleId){check(rootCycleSnapshotHash(input.snapshot)===metadata.reference.sha256
     &&canonical(input.actionMetadata)===canonical(metadata.actionMetadata)&&input.retirementId===metadata.retirementId&&input.expectedRecordHash===metadata.previousRecordHash
     &&canonical(input.expectedClaim)===canonical(metadata.cycle.claim));await guarded(input,targetFor(input,metadata));return frozen({...copy(metadata),newlyRecorded:false,newlyStarted:false});}
   check(metadata.state==='started'&&metadata.recordHash===input.expectedRecordHash&&metadata.cycle.cycleId===input.cycleId&&canonical(metadata.cycle.claim)===canonical(input.expectedClaim));
   const previous={...metadata,state:'ready',decisionsConsumed:metadata.decisionsConsumed-1};const old=await resolve(input,previous);const snapshot=input.snapshot;const text=snapshotBytes(snapshot,pinned,options.parts);
   check(rootCycleSnapshotHash({variablesByPart:snapshot.variablesByPart,task:snapshot.task})===metadata.contextHash&&rootCycleSnapshotHash(snapshot.observations.slice(0,old.observations.length))===metadata.observationsHash);
   const answered=input.actionMetadata.kind==='answer';check(snapshot.observations.length===old.observations.length+(answered?0:1)&&(answered?snapshot.output!==null:snapshot.output===null));
   check(snapshot.decisionsConsumed>=(input.actionMetadata.kind==='child'?metadata.decisionsConsumed+1:metadata.decisionsConsumed)&&(input.actionMetadata.kind==='child'||snapshot.decisionsConsumed===metadata.decisionsConsumed));
   const sources=copy(metadata.observationSources);if(!answered){const observation=snapshot.observations.at(-1);check(observation.kind===input.actionMetadata.kind
    &&(observation.kind==='tool'?observation.toolId===input.actionMetadata.toolId:observation.partId===input.actionMetadata.partId));sources.push({actionMetadata:copy(input.actionMetadata),observationHash:rootCycleSnapshotHash(observation)});}
   const ref=await store(input,{...metadata,observationSources:sources},snapshot,{operation:'accept',actionMetadata:input.actionMetadata,retirementId:input.retirementId});
   return append(input,input.expectedRecordHash,({metadata:latest})=>{check(latest.state==='started'&&latest.cycle.cycleId===input.cycleId);return {record:{...baseRecord(latest),kind:'accepted',reference:ref,
    observationsHash:rootCycleSnapshotHash(snapshot.observations),observationsCount:snapshot.observations.length,decisionsConsumed:snapshot.decisionsConsumed,observationSources:sources,actionMetadata:copy(input.actionMetadata),retirementId:input.retirementId}};});})});
}
