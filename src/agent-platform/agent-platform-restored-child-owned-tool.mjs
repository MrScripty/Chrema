import {advanceCompositionToolObservation} from './agent-platform-composition-decisions.mjs';
import {createHash} from 'node:crypto';
import {resolvePostgresRelations} from '../shared/postgres-relations.mjs';
import {compositionCheckpointRecordHash as hash} from './agent-platform-composition-checkpoint-records.mjs';
import {normalizeExpectedClaim,assertPostgresExpectedClaim} from './agent-platform-claim-bound-journal.mjs';
import {workflowDefinitionHash} from './agent-platform-prompt.mjs';
import {completeWorkflowRunInTransaction} from './agent-platform-workflow-completion.mjs';
import {valueSchemaIssues} from '../shared/value-schema.mjs';
import {callerToolOutcomeDescriptor,createCallerToolInvocationBinding,validateCallerToolOutcome} from './agent-platform-caller-tool-outcome.mjs';

const copy=value=>structuredClone(value);
const freeze=value=>{if(value&&typeof value==='object'){Object.values(value).forEach(freeze);Object.freeze(value);}return value;};
const sha=text=>createHash('sha256').update(text).digest('hex');
const id=value=>typeof value==='string'&&/^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$/.test(value);
const textId=value=>typeof value==='string'&&value.length>0&&value.length<=512&&value.trim()===value;
const hex=value=>typeof value==='string'&&/^[a-f0-9]{64}$/.test(value);
const ms=value=>value instanceof Date?value.getTime():Date.parse(value);
const kinds=['tool-intent-accepted','tool-started','tool-result-accepted','answer-started','answer-accepted','workflow-finalized'];
function refuse(){return Object.assign(new Error('Restored child owned tool continuation is unavailable.'),{code:'RESTORED_CHILD_TOOL_UNAVAILABLE',statusCode:409});}
function check(value){if(!value)throw refuse();}
function exact(value,keys){check(value&&Object.getPrototypeOf(value)===Object.prototype&&Object.keys(value).sort().join('|')===[...keys].sort().join('|'));}
const policyKeys=['schemaVersion','mode','policyId','policyVersion','toolId','bindingId','argumentsOwnerId','argumentsContractId','answerOwnerId','answerContractId','maximumInputBytes','maximumResultBytes','maximumAnswerBytes','completionPolicyId','completionPolicyVersion'];
function policy(value){const bounded=value?.mode==='restored-child-bounded-tools.v2';exact(value,bounded?[...policyKeys,'nextToolId','nextBindingId','maximumToolInvocations']:policyKeys);if(bounded)check(textId(value.nextToolId)&&hex(value.nextBindingId)&&value.nextToolId!==value.toolId&&value.maximumToolInvocations===2);check(value.schemaVersion===1&&(value.mode==='restored-child-one-tool.v1'||bounded)&&textId(value.toolId)&&hex(value.bindingId));
 for(const key of ['policyId','argumentsOwnerId','argumentsContractId','answerOwnerId','answerContractId','completionPolicyId'])check(id(value[key]));
 for(const key of ['policyVersion','completionPolicyVersion'])check(Number.isSafeInteger(value[key])&&value[key]>0&&value[key]<=1000000);
 for(const [key,max] of [['maximumInputBytes',32000],['maximumResultBytes',128000],['maximumAnswerBytes',32000]])check(Number.isSafeInteger(value[key])&&value[key]>0&&value[key]<=max);return copy(value);}
export function restoredChildToolDescriptor({recordDescriptor,registration,nextRegistration,ownerDescriptor,policy:input}={}){
 const p=policy(input),owner=callerToolOutcomeDescriptor({policy:ownerDescriptor});
 check(recordDescriptor?.protocol==='composition-checkpoint-records.v1'&&registration?.toolId===p.toolId&&registration.bindingId===p.bindingId&&owner.maximumResultBytes===p.maximumResultBytes);
 const extra=p.mode==='restored-child-bounded-tools.v2'?{nextRegistrationHash:hash(nextRegistration)}:{};if(p.mode==='restored-child-bounded-tools.v2')check(nextRegistration?.toolId===p.nextToolId&&nextRegistration.bindingId===p.nextBindingId);
 return freeze({...p,...extra,recordDescriptorHash:hash(recordDescriptor),registrationHash:hash(registration),ownerDescriptor:owner,
  definitionId:recordDescriptor.definitionId,definitionHash:recordDescriptor.definitionHash,runtimeBindingHash:recordDescriptor.runtimeBindingHash,assignmentHash:recordDescriptor.assignmentHash});
}
export function getRestoredChildBoundedToolsSchemaUrl(){return new URL('../../schemas/agent-platform/restored-child-bounded-tools-v2.sql',import.meta.url);}
export function getRestoredChildToolSchemaUrl(){return new URL('../../schemas/agent-platform/restored-child-owned-tool-v1.sql',import.meta.url);}

/** Internal installed-host flow. Entry arguments carry no decisions, outcomes,
 * bodies, references or owner attestations. Owner effects are never inferred
 * from transport success. Only a newly committed start fence can execute.
 */
export function createRestoredChildToolContinuation({configuration,definition,assignment,registration,nextRegistration,limits,rootPrompt,inputForObservations,validateAttempt,executeDecision,executeTool,discloseToolResult}={}){
 exact(configuration,['database','relations','journalFactory','records','runtimeBinding','policy','getPolicy','getRecordPolicy','authorize','resolveChild','restoreContext','argumentStore','owner','answerStore']);
 const {database,records,journalFactory,getPolicy,getRecordPolicy,authorize,resolveChild,restoreContext}=configuration;
 for(const fn of [database?.transaction,records?.descriptorForRun,records?.inspect,records?.markSuccessorStarted,journalFactory,getPolicy,getRecordPolicy,authorize,resolveChild,restoreContext,inputForObservations,validateAttempt,executeDecision,executeTool,discloseToolResult])check(typeof fn==='function');
 const argumentStore=Object.freeze({...configuration.argumentStore}),owner=Object.freeze({...configuration.owner}),answerStore=Object.freeze({...configuration.answerStore});
 for(const store of [argumentStore,answerStore]){exact(store,['ownerId','contractId','write','attest','resolve']);for(const key of ['write','attest','resolve'])check(typeof store[key]==='function');}
 exact(owner,['descriptor','getDescriptor','lookup','attest','resolve']);for(const key of ['getDescriptor','lookup','attest','resolve'])check(typeof owner[key]==='function');
 const relations=resolvePostgresRelations(configuration.relations,['runs','versions','assignments','attempts','events','heads','records','flows'],'Restored child tool');
 const p=policy(configuration.policy),bounded=p.mode==='restored-child-bounded-tools.v2',r=copy(records.descriptorForRun()),reg=freeze(copy(registration)),nextReg=bounded?freeze(copy(nextRegistration)):null;
 const selectedRegistration=intent=>intent.invocation.toolId===reg.toolId?reg:(check(bounded&&intent.invocation.toolId===nextReg.toolId),nextReg);
 const descriptor=restoredChildToolDescriptor({recordDescriptor:r,registration:reg,...(bounded?{nextRegistration:nextReg}:{}),ownerDescriptor:owner.descriptor,policy:p}),descriptorHash=hash(descriptor);
 check(r.definitionId===definition.id&&r.definitionHash===definition.definitionHash&&r.assignmentHash===assignment.assignmentHash&&r.runtimeBindingHash===hash(configuration.runtimeBinding)
  &&r.maximumDecisions===limits.maximumDecisions&&r.maximumContextBytes===limits.maximumContextBytes);
 check(argumentStore.ownerId===p.argumentsOwnerId&&argumentStore.contractId===p.argumentsContractId&&answerStore.ownerId===p.answerOwnerId&&answerStore.contractId===p.answerContractId);
 const recordPolicy=Object.fromEntries(['schemaVersion','protocol','policyId','policyVersion','writerId','outputOwnerId','outputContractId','maximumDecisions','maximumContextBytes'].map(key=>[key,r[key]]));
 const current=()=>check(hash(policy(getPolicy()))===hash(p)&&hash(getRecordPolicy())===hash(recordPolicy)&&hash(configuration.runtimeBinding)===r.runtimeBindingHash&&hash(records.descriptorForRun())===hash(r)
  &&hash(callerToolOutcomeDescriptor({policy:owner.getDescriptor()}))===hash(descriptor.ownerDescriptor)
  &&configuration.argumentStore.ownerId===p.argumentsOwnerId&&configuration.argumentStore.contractId===p.argumentsContractId
  &&configuration.answerStore.ownerId===p.answerOwnerId&&configuration.answerStore.contractId===p.answerContractId);
 function reference(value,ownerId,contractId,max){exact(value,['ownerId','contractId','referenceId','sha256','byteLength']);check(value.ownerId===ownerId&&value.contractId===contractId&&id(value.referenceId)&&hex(value.sha256)&&Number.isSafeInteger(value.byteLength)&&value.byteLength>0&&value.byteLength<=max);return freeze(copy(value));}
 function body(text,ref,max,schema){check(typeof text==='string'&&Buffer.from(text).toString('utf8')===text&&Buffer.byteLength(text)<=max);
  if(ref)check(Buffer.byteLength(text)===ref.byteLength&&sha(text)===ref.sha256);const value=JSON.parse(text);check(JSON.stringify(value)===text&&(!schema||valueSchemaIssues(value,schema).length===0));return value;}
 function binding(s){return {runIdentityHash:s.run.identity_sha256,inputHash:s.run.input_sha256,authorizationHash:hash(s.run.authorization_snapshot),assignmentIdentityHash:s.assignment.identity_sha256,
  attemptPromptHash:s.attempt.prompt_sha256,attemptCapabilityHash:s.attempt.capability_sha256,requested:[s.attempt.requested_model,s.attempt.requested_reasoning_effort,s.attempt.requested_service_tier],
  budget:{version:s.run.run_family_budget_version,descendants:s.run.run_family_max_descendant_runs,attempts:s.run.run_family_max_total_attempts,tokens:s.run.run_family_max_total_tokens}};}
 async function clock(client,runId){const row=(await client.query(`SELECT clock_timestamp() AS checked_at,EXISTS(SELECT 1 FROM ${relations.events} WHERE run_id=$1 AND source_key='workflow.cancellation-requested.v1') AS cancelled`,[runId])).rows[0];current();check(row&&!row.cancelled&&Number.isFinite(ms(row.checked_at)));return ms(row.checked_at);}
 async function custody(client,input,deadline){current();input.signal?.throwIfAborted();await assertPostgresExpectedClaim(client,relations,input.runId,input.expectedClaim);current();const now=await clock(client,input.runId);if(deadline!==undefined)check(now<deadline);input.signal?.throwIfAborted();}
 async function load(client,input){
  current();const {runId}=input;
  // Generic completion and every flow transition share this lock order.
  const assignments=(await client.query(`SELECT * FROM ${relations.assignments} WHERE run_id=$1 ORDER BY assignment_ordinal LIMIT 2 FOR UPDATE`,[runId])).rows;
  const attempts=(await client.query(`SELECT * FROM ${relations.attempts} WHERE run_id=$1 ORDER BY attempt_ordinal LIMIT 2 FOR UPDATE`,[runId])).rows;
  const run=(await client.query(`SELECT * FROM ${relations.runs} WHERE id=$1 FOR UPDATE`,[runId])).rows[0];
  check(run&&assignments.length===1&&attempts.length===1&&run.resumed_from_run_id===null&&run.run_family_budget_version!==3&&run.workflow_version_id===r.definitionId
   &&hash(run.authorization_snapshot.compositionCheckpointProtocol)===hash(r)&&hash(run.authorization_snapshot.restoredChildOwnedTool)===descriptorHash
   &&hash(run.authorization_snapshot.assignedRuntime)===r.runtimeBindingHash&&hash(run.input_payload)===run.input_sha256);
  const a=assignments[0],x=attempts[0];check(a.parent_assignment_id===null&&a.execution==='current-agent'&&x.assignment_id===a.id&&x.attempt_ordinal===1
   &&x.resource_reserved_total_tokens===null&&x.thread_id===a.agent_thread_id&&hash(x.prompt)===x.prompt_sha256&&hash(x.capability_profile)===x.capability_sha256);
  const version=(await client.query(`SELECT definition,definition_sha256 FROM ${relations.versions} WHERE id=$1`,[r.definitionId])).rows[0];check(version?.definition_sha256===r.definitionHash&&workflowDefinitionHash(version.definition)===r.definitionHash);
  const base=(await client.query(`SELECT * FROM ${relations.records} WHERE run_id=$1 ORDER BY sequence LIMIT 4`,[runId])).rows;
  const head=(await client.query(`SELECT * FROM ${relations.heads} WHERE run_id=$1`,[runId])).rows[0];check(base.length>=2&&base.length<=3&&head?.sequence===base.length);
  let previous=null;for(let i=0;i<base.length;i++){const v=base[i].payload;check(base[i].run_id===runId&&base[i].sequence===i+1&&base[i].record_sha256===hash(v)&&v.runId===runId&&v.sequence===i+1&&v.schemaVersion===1&&v.descriptorHash===hash(r)&&v.previousRecordHash===previous&&v.kind===['child-started','child-accepted-cursor-reserved','successor-started'][i]);previous=base[i].record_sha256;}
  check(head.record_sha256===previous);const accepted=base[1].payload,parent=accepted.parent;
  check(parent.assignmentId===a.id&&parent.attemptId===x.id&&parent.threadId===x.thread_id&&parent.partId===assignment.parts.find(part=>part.partId===parent.partId)?.partId
   &&accepted.limits.maximumDecisions===r.maximumDecisions&&accepted.limits.maximumContextBytes===r.maximumContextBytes&&accepted.limits.deadlineAtMs===x.capability_profile.deadlineAtMs
   &&accepted.reservation.status==='reserved'&&accepted.reservation.nextDecisionOrdinal===accepted.cursor.decisionsConsumed+1&&accepted.cursor.completedActions===1&&accepted.cursor.observations.length===1
   &&accepted.cursor.observations[0].outputReferenceHash===hash(accepted.child.outputReference)&&accepted.cursor.parentInvocationId===parent.invocationId&&accepted.reservation.cursorHash===hash(accepted.cursor));
  if(base.length===3){const v=base[2].payload;check(hash({...v,kind:accepted.kind,reservation:{...v.reservation,status:'reserved'},sequence:accepted.sequence,requestId:accepted.requestId,requestHash:accepted.requestHash,previousRecordHash:accepted.previousRecordHash,claim:accepted.claim})===hash(accepted));}
  const rows=(await client.query(`SELECT * FROM ${relations.flows} WHERE run_id=$1 ORDER BY sequence LIMIT ${bounded?11:7} FOR UPDATE`,[runId])).rows;check(rows.length<=(bounded?10:6));let prior=null;
  const toolOffset=bounded&&rows[4]?.payload.kind==='tool-intent-accepted'?4:0;
  const expectedKinds=bounded?[...kinds.slice(0,3),'next-decision-started',...(toolOffset===4?[...kinds.slice(0,3),...kinds.slice(3)]:kinds.slice(4))]:kinds;
  const state={run,assignment:a,attempt:x,accepted,base,rows,toolOffset};
  for(let i=0;i<rows.length;i++){const row=rows[i],v=row.payload;exact(v,['schemaVersion','runId','sequence','kind','descriptorHash','previousFlowHash','baseCheckpointHash','providerStartedHash','parent','binding','claim','data']);
   check(base.length===3&&row.sequence===i+1&&row.run_id===runId&&row.record_sha256===hash(v)&&v.schemaVersion===1&&v.runId===runId&&v.sequence===i+1&&v.kind===expectedKinds[i]
    &&v.descriptorHash===descriptorHash&&v.previousFlowHash===prior&&v.baseCheckpointHash===base[1].record_sha256&&v.providerStartedHash===base[2].record_sha256
    &&hash(v.parent)===hash(parent)&&hash(v.binding)===hash(binding(state))&&normalizeExpectedClaim(v.claim)?.runId===runId);prior=row.record_sha256;}
  for(const offset of toolOffset===4?[0,4]:[0]){
   const previousCursor=offset?rows[2].payload.data.cursor:accepted.cursor,previousReservation=offset?rows[2].payload.data.reservation:accepted.reservation;
   const previousRecordHash=offset?rows[2].record_sha256:base[1].record_sha256,started=offset?rows[3]:base[2],selected=offset?nextReg:reg;
   if(rows.length>offset){const d=rows[offset].payload.data;exact(d,['intentHash','invocation','decisionHash','argumentReference','argumentsValueHash','restoration','restoredInputBytes','decisionOrdinal']);
    check(hash(d.invocation)===hash(createCallerToolInvocationBinding({...Object.fromEntries(['runId','parentInvocationId','attemptId','partId','toolId','bindingId','argumentsHash','intentHash'].map(key=>[key,d.invocation[key]]))}))
     &&d.invocation.runId===runId&&d.invocation.parentInvocationId===parent.invocationId&&d.invocation.attemptId===x.id&&d.invocation.partId===parent.partId&&d.invocation.toolId===selected.toolId&&d.invocation.bindingId===selected.bindingId
     &&d.invocation.intentHash===d.intentHash&&d.invocation.argumentsHash===d.argumentReference.sha256&&hex(d.argumentsValueHash)&&hex(d.decisionHash)&&hash(rows[offset].payload.claim)===hash(started.payload.claim)
     &&d.decisionOrdinal===previousReservation.nextDecisionOrdinal&&d.decisionOrdinal+1<=r.maximumDecisions);
    reference(d.argumentReference,p.argumentsOwnerId,p.argumentsContractId,p.maximumInputBytes);exact(d.restoration,['recordHash','inputHash','decisionsConsumed','contextBytes','maximumDecisions','maximumContextBytes','deadlineAtMs']);
    check(d.restoration.recordHash===previousRecordHash&&d.restoration.decisionsConsumed===previousCursor.decisionsConsumed&&d.restoration.contextBytes===previousCursor.contextBytes
     &&d.restoration.maximumDecisions===r.maximumDecisions&&d.restoration.maximumContextBytes===r.maximumContextBytes&&d.restoration.deadlineAtMs===accepted.limits.deadlineAtMs&&hex(d.restoration.inputHash)
     &&(!offset||d.restoration.inputHash===rows[2].payload.data.restoredInputHash)
     &&Number.isSafeInteger(d.restoredInputBytes)&&d.restoredInputBytes>0&&d.restoredInputBytes<=r.maximumContextBytes);
   }
   if(rows.length>offset+1){const d=rows[offset+1].payload.data;exact(d,['intentFlowHash','invocationId']);check(d.intentFlowHash===rows[offset].record_sha256&&d.invocationId===rows[offset].payload.data.invocation.invocationId);}
   if(rows.length>offset+2){const d=rows[offset+2].payload.data,intent=rows[offset].payload.data;exact(d,['intentFlowHash','toolStartedHash','outcome','outcomeHash','resultValueHash','restoredInputHash','restoredInputBytes','cursor','reservation']);
    const outcome=validateCallerToolOutcome(d.outcome,{descriptor:descriptor.ownerDescriptor,invocation:intent.invocation});
    check(d.intentFlowHash===rows[offset].record_sha256&&d.toolStartedHash===rows[offset+1].record_sha256&&outcome.kind==='applied-with-exact-result'&&d.outcomeHash===hash(outcome)&&hex(d.resultValueHash)&&hex(d.restoredInputHash));
    exact(d.cursor,['observationsHash','completedActions','decisionsConsumed','contextBytes']);exact(d.reservation,['reservationId','kind','partId','nextDecisionOrdinal','cursorHash','status']);
    check(hex(d.cursor.observationsHash)&&d.cursor.completedActions===previousCursor.completedActions+1&&d.cursor.decisionsConsumed===intent.decisionOrdinal&&d.cursor.contextBytes===previousCursor.contextBytes+outcome.resultRef.byteLength
     &&d.cursor.contextBytes<=r.maximumContextBytes&&d.reservation.kind==='provider-decision'&&d.reservation.partId===parent.partId&&d.reservation.nextDecisionOrdinal===intent.decisionOrdinal+1
     &&d.reservation.nextDecisionOrdinal<=r.maximumDecisions&&d.reservation.cursorHash===hash(d.cursor)&&d.reservation.status==='reserved'&&id(d.reservation.reservationId)
     &&Number.isSafeInteger(d.restoredInputBytes)&&d.restoredInputBytes>0&&d.restoredInputBytes<=r.maximumContextBytes);
   }
   if(rows.length>offset+3){const d=rows[offset+3].payload.data;exact(d,['toolAcceptedHash','reservation']);check(d.toolAcceptedHash===rows[offset+2].record_sha256&&hash({...d.reservation,status:'reserved'})===hash(rows[offset+2].payload.data.reservation)&&d.reservation.status==='started');}
  }
  if(rows.length>toolOffset+4&&rows[toolOffset+4].payload.kind==='answer-accepted'){const offset=toolOffset,d=rows[offset+4].payload.data;exact(d,['toolAcceptedHash','answerStartedHash','outputReference','outputValueHash','decisionOrdinal','restoredInputHash']);check(d.toolAcceptedHash===rows[offset+2].record_sha256&&d.answerStartedHash===rows[offset+3].record_sha256
    &&d.decisionOrdinal===rows[offset+2].payload.data.reservation.nextDecisionOrdinal&&d.restoredInputHash===rows[offset+2].payload.data.restoredInputHash&&hex(d.outputValueHash)&&hash(rows[offset+4].payload.claim)===hash(rows[offset+3].payload.claim));reference(d.outputReference,p.answerOwnerId,p.answerContractId,p.maximumAnswerBytes);}
  validateAttempt({run,assignment:a,attempt:x},accepted);current();return state;
 }
 function entry(supplied,withSignal){exact(supplied,withSignal?Object.hasOwn(supplied??{},'signal')?['runId','expectedClaim','signal']:['runId','expectedClaim']:['runId','expectedClaim']);check(id(supplied.runId));const expectedClaim=normalizeExpectedClaim(supplied.expectedClaim);check(expectedClaim&&expectedClaim.runId===supplied.runId);current();return {runId:supplied.runId,expectedClaim,signal:withSignal?supplied.signal:undefined};}
 const safe=fn=>async(...args)=>{try{return await fn(...args);}catch{throw refuse();}};
 async function permit(input,phase,extra={}){current();input.signal?.throwIfAborted();check(await authorize(freeze({phase,runId:input.runId,expectedClaim:copy(input.expectedClaim),descriptor:copy(descriptor),...copy(extra)}))===true);current();input.signal?.throwIfAborted();}
 async function fresh(input,{execution=true,expectedSequence,expectedHashes}={}){const guarded=()=>database.transaction(async client=>{const s=await load(client,input);check(s.run.status==='running'&&s.assignment.status==='running'&&s.attempt.outcome===null);await custody(client,input,execution?s.accepted.limits.deadlineAtMs:undefined);return s;});
  await permit(input,'custody');await guarded();await permit(input,'custody');const s=await guarded();if(expectedSequence!==undefined)check(s.rows.length===expectedSequence);if(expectedHashes)check(hash(s.rows.map(row=>row.record_sha256))===hash(expectedHashes));current();return s;}
 async function append(input,sequence,data,{execution=true,kind=kinds[sequence-1]}={}){await permit(input,'commit-flow',{sequence});return database.transaction(async client=>{const s=await load(client,input);check(s.run.status==='running'&&s.assignment.status==='running'&&s.attempt.outcome===null&&s.base.length===3);await custody(client,input,execution?s.accepted.limits.deadlineAtMs:undefined);
  check(s.rows.length===sequence-1);const payload={schemaVersion:1,runId:input.runId,sequence,kind,descriptorHash,previousFlowHash:s.rows.at(-1)?.record_sha256??null,
   baseCheckpointHash:s.base[1].record_sha256,providerStartedHash:s.base[2].record_sha256,parent:copy(s.accepted.parent),binding:binding(s),claim:copy(input.expectedClaim),data:copy(data)};
  await client.query(`INSERT INTO ${relations.flows}(run_id,sequence,record_sha256,payload) VALUES($1,$2,$3,$4::jsonb)`,[input.runId,sequence,hash(payload),JSON.stringify(payload)]);await permit(input,'commit-flow',{sequence});await custody(client,input,execution?s.accepted.limits.deadlineAtMs:undefined);return freeze({recordHash:hash(payload),payload});});}
 async function restoreChild(input,s){const record=s.accepted,recordHash=s.base[1].record_sha256;await permit(input,'resolve-child',{recordHash});await fresh(input);
  const text=await resolveChild(freeze({runId:input.runId,descriptor:copy(descriptor),record:copy(record),recordHash}),{signal:input.signal});await fresh(input);
  const child=body(text,record.child.outputReference,r.maximumContextBytes);check(child&&Object.getPrototypeOf(child)===Object.prototype);const observations=[{kind:'child',partId:record.child.partId,output:child}];
  const restored=inputForObservations(copy(record),copy(observations));check(restored.task===rootPrompt&&hash(restored.observations)===hash(observations)&&restored.executionState.completedActions===1);
  const bytes=Buffer.byteLength(JSON.stringify(restored));check(bytes<=r.maximumContextBytes);const context={recordHash,inputHash:hash(restored),decisionsConsumed:record.cursor.decisionsConsumed,contextBytes:record.cursor.contextBytes,
   maximumDecisions:r.maximumDecisions,maximumContextBytes:r.maximumContextBytes,deadlineAtMs:record.limits.deadlineAtMs};
  await permit(input,'restore-context',{recordHash,context});check(hash(await restoreContext(freeze({runId:input.runId,descriptor:copy(descriptor),recordHash,context:copy(context)}),{signal:input.signal}))===hash(context));await fresh(input);return {observations,restored,context,bytes};
 }
 async function storeBody(input,store,text,target,max){await fresh(input);const value=body(text,null,max),bodyTarget={runId:input.runId,descriptor:copy(descriptor),...copy(target),text,sha256:sha(text),byteLength:Buffer.byteLength(text),valueHash:hash(value)};
  const ref=reference(await store.write(freeze(bodyTarget),{signal:input.signal}),store.ownerId,store.contractId,max);await fresh(input);check(ref.sha256===sha(text)&&ref.byteLength===Buffer.byteLength(text));
  check(await store.attest(freeze({runId:input.runId,descriptor:copy(descriptor),...copy(target),reference:copy(ref),valueHash:hash(value)}),{signal:input.signal})===true);await fresh(input);return ref;}
 async function argumentsFor(input,s,{offset=0}={}){const intent=s.rows[offset].payload.data,selected=selectedRegistration(intent),target={runId:input.runId,descriptor:copy(descriptor),invocation:copy(intent.invocation),reference:copy(intent.argumentReference)};
  await permit(input,'resolve-arguments',{invocation:intent.invocation,argumentReference:intent.argumentReference});await fresh(input);check(await argumentStore.attest(freeze(copy(target)),{signal:input.signal})===true);await fresh(input);
  const text=await argumentStore.resolve(freeze(copy(target)),{signal:input.signal});await fresh(input);const value=body(text,intent.argumentReference,p.maximumInputBytes,selected.inputSchema);check(hash(value)===intent.argumentsValueHash&&hash({kind:'tool',toolId:selected.toolId,arguments:value})===intent.decisionHash&&intent.intentHash===hash({startedRecordHash:(offset?s.rows[offset-1]:s.base[2]).record_sha256,decisionOrdinal:intent.decisionOrdinal,decisionHash:intent.decisionHash,argumentsHash:sha(text),restoration:intent.restoration}));return value;}
 async function ownerResult(input,s,{execution=true,offset=0}={}){const intent=s.rows[offset].payload.data,selected=selectedRegistration(intent),target={runId:input.runId,descriptor:copy(descriptor),invocation:copy(intent.invocation),argumentReference:copy(intent.argumentReference)};
  await permit(input,'lookup-tool-outcome',target);await fresh(input,{execution});const raw=await owner.lookup(freeze(copy(target)),{signal:input.signal});await fresh(input,{execution});
  const outcome=validateCallerToolOutcome(raw,{descriptor:descriptor.ownerDescriptor,invocation:intent.invocation});check(outcome.kind==='applied-with-exact-result');
  const attested={...target,outcome:copy(outcome)};check(await owner.attest(freeze(copy(attested)),{signal:input.signal})===true);await fresh(input,{execution});
  await permit(input,'resolve-tool-result',{invocation:intent.invocation,outcome});await fresh(input,{execution});const text=await owner.resolve(freeze(copy(attested)),{signal:input.signal});await fresh(input,{execution});
  const value=body(text,outcome.resultRef,p.maximumResultBytes,selected.resultSchema);if(s.rows.length>=offset+3)check(hash(outcome)===s.rows[offset+2].payload.data.outcomeHash&&hash(value)===s.rows[offset+2].payload.data.resultValueHash);
  return {outcome,value,text};
 }
 function answerInput(s,child,result){const observations=advanceCompositionToolObservation({observations:child.observations,toolId:p.toolId,result:result.value});const input=inputForObservations(copy(s.accepted),copy(observations));
  check(input.task===rootPrompt&&hash(input.observations)===hash(observations)&&input.executionState.completedActions===2);const bytes=Buffer.byteLength(JSON.stringify(input));check(bytes<=r.maximumContextBytes);return {input,observations,bytes};}
 // Restore through the same part interpreter. Every accepted owner is checked
 // again; one invocation's scope or cached proof never covers another.
 async function restoredToolInput(input,s,{includePending=false}={}){
  const child=await restoreChild(input,s);check(hash(child.context)===hash(s.rows[0].payload.data.restoration)&&child.bytes===s.rows[0].payload.data.restoredInputBytes);
  let observations=child.observations,context=child.context,previousCursor=s.accepted.cursor,last;
  const offsets=s.toolOffset===4?[0,4]:[0];
  for(const offset of offsets){
   if(s.rows.length<offset+3&&!includePending)break;
   const intent=s.rows[offset].payload.data;if(offset)check(hash(context)===hash(intent.restoration));
   const result=await ownerResult(input,s,{offset});await argumentsFor(input,s,{offset});
   await discloseToolResult({record:copy(s.accepted),invocation:copy(intent.invocation),result:copy(result.value),signal:input.signal,current,
    authorizeOperation:async adapterContext=>{await permit(input,'disclose-tool',{invocation:intent.invocation,adapterPhase:adapterContext.phase});await fresh(input,{expectedSequence:s.rows.length});return true;}});
   observations=advanceCompositionToolObservation({observations,toolId:intent.invocation.toolId,result:result.value});
   const restored=inputForObservations(copy(s.accepted),copy(observations)),bytes=Buffer.byteLength(JSON.stringify(restored));
   check(restored.task===rootPrompt&&hash(restored.observations)===hash(observations)&&restored.executionState.completedActions===observations.length&&bytes<=r.maximumContextBytes);
   const cursor={observationsHash:hash(observations),completedActions:previousCursor.completedActions+1,decisionsConsumed:intent.decisionOrdinal,contextBytes:previousCursor.contextBytes+result.outcome.resultRef.byteLength};check(cursor.contextBytes<=r.maximumContextBytes);
   const accepted=s.rows[offset+2];if(accepted)check(hash(cursor)===hash(accepted.payload.data.cursor)&&hash(restored)===accepted.payload.data.restoredInputHash&&bytes===accepted.payload.data.restoredInputBytes);
   context={recordHash:accepted?.record_sha256??null,inputHash:hash(restored),decisionsConsumed:cursor.decisionsConsumed,contextBytes:cursor.contextBytes,maximumDecisions:r.maximumDecisions,maximumContextBytes:r.maximumContextBytes,deadlineAtMs:s.accepted.limits.deadlineAtMs};
   previousCursor=cursor;last={input:restored,bytes,cursor,context,result,observations};
  }
  check(last);await fresh(input,{expectedSequence:s.rows.length});return last;
 }
 async function acceptAnswer(input,s,decision,startedHash,restored){
  const offset=s.toolOffset,started=s.rows[offset+3],toolAccepted=s.rows[offset+2];check(started.record_sha256===startedHash&&hash(started.payload.claim)===hash(input.expectedClaim));
  exact(decision,['kind','output']);check(decision.kind==='answer'&&decision.output&&Object.getPrototypeOf(decision.output)===Object.prototype);
  const outputText=JSON.stringify(decision.output);body(outputText,null,p.maximumAnswerBytes);await permit(input,'persist-answer',{invocation:s.rows[offset].payload.data.invocation,toolAcceptedHash:toolAccepted.record_sha256,answerStartedHash:startedHash});
  const outputReference=await storeBody(input,answerStore,outputText,{answerStartedHash:startedHash,inputHash:hash(restored.input),toolAcceptedHash:toolAccepted.record_sha256,idempotencyKey:'tool-answer-output:'+hash({runId:input.runId,answerStartedHash:startedHash})},p.maximumAnswerBytes);
  await append(input,offset+5,{toolAcceptedHash:toolAccepted.record_sha256,answerStartedHash:startedHash,outputReference,outputValueHash:hash(decision.output),decisionOrdinal:toolAccepted.payload.data.reservation.nextDecisionOrdinal,restoredInputHash:hash(restored.input)},{kind:'answer-accepted'});return finalizeInput(input);
 }
 async function continueBounded(input,s){
  if(s.rows.length===3){
   const restored=await restoredToolInput(input,s),accepted=s.rows[2];let startedHash;
   const decision=await executeDecision({record:copy(s.accepted),input:copy(restored.input),signal:input.signal,kind:'next',current,
    authorizeOperation:async context=>{await permit(input,context.phase==='answer'?'answer':'provider-operation',{adapterPhase:context.phase});await fresh(input);return true;},
    fence:async()=>{await permit(input,'start-next-decision',{acceptedToolHash:accepted.record_sha256});const marker=await append(input,4,{toolAcceptedHash:accepted.record_sha256,reservation:{...accepted.payload.data.reservation,status:'started'}},{kind:'next-decision-started'});startedHash=marker.recordHash;await fresh(input,{expectedSequence:4});}});
   s=await fresh(input,{expectedSequence:4});check(s.rows[3].record_sha256===startedHash&&hash(s.rows[3].payload.claim)===hash(input.expectedClaim));
   if(decision.kind==='answer')return acceptAnswer(input,s,decision,startedHash,restored);
   exact(decision,['kind','toolId','arguments']);check(decision.kind==='tool'&&decision.toolId===nextReg.toolId&&accepted.payload.data.reservation.nextDecisionOrdinal+1<=r.maximumDecisions);
   const text=JSON.stringify(decision.arguments),value=body(text,null,p.maximumInputBytes,nextReg.inputSchema),ordinal=accepted.payload.data.reservation.nextDecisionOrdinal;
   const intentHash=hash({startedRecordHash:startedHash,decisionOrdinal:ordinal,decisionHash:hash(decision),argumentsHash:sha(text),restoration:restored.context});
   const invocation=createCallerToolInvocationBinding({runId:input.runId,parentInvocationId:s.accepted.parent.invocationId,attemptId:s.accepted.parent.attemptId,partId:s.accepted.parent.partId,toolId:nextReg.toolId,bindingId:nextReg.bindingId,argumentsHash:sha(text),intentHash});
   await permit(input,'persist-tool-intent',{invocation});const argumentReference=await storeBody(input,argumentStore,text,{invocation,idempotencyKey:invocation.invocationId},p.maximumInputBytes);
   await append(input,5,{intentHash,invocation,decisionHash:hash(decision),argumentReference,argumentsValueHash:hash(value),restoration:restored.context,restoredInputBytes:restored.bytes,decisionOrdinal:ordinal},{kind:'tool-intent-accepted'});s=await fresh(input,{expectedSequence:5});
  }
  if(s.rows.length===5&&s.toolOffset===4){
   const intent=s.rows[4].payload.data,restored=await restoredToolInput(input,s);check(hash(restored.context)===hash(intent.restoration)&&restored.bytes===intent.restoredInputBytes);const args=await argumentsFor(input,s,{offset:4});await permit(input,'start-tool',{invocation:intent.invocation});
   const marker=await append(input,6,{intentFlowHash:s.rows[4].record_sha256,invocationId:intent.invocation.invocationId},{kind:'tool-started'});await fresh(input,{expectedSequence:6});
   await executeTool({record:copy(s.accepted),invocation:copy(intent.invocation),arguments:copy(args),signal:input.signal,current,
    authorizeOperation:async context=>{await permit(input,context.phase==='disclose'?'disclose-tool':context.phase==='invoke'?'invoke-tool':'tool-operation',{invocation:intent.invocation,adapterPhase:context.phase});await fresh(input,{expectedSequence:6});return true;},
    beforeInvoke:async()=>{const latest=await fresh(input,{expectedSequence:6});check(latest.rows[5].record_sha256===marker.recordHash&&hash(latest.rows[5].payload.claim)===hash(input.expectedClaim));}});s=await fresh(input,{expectedSequence:6});
  }
  if(s.rows.length===6&&s.toolOffset===4){
   const restored=await restoredToolInput(input,s,{includePending:true}),intent=s.rows[4].payload.data;
   const reservation={reservationId:'tool-answer:'+hash({runId:input.runId,invocationId:intent.invocation.invocationId}),kind:'provider-decision',partId:s.accepted.parent.partId,nextDecisionOrdinal:intent.decisionOrdinal+1,cursorHash:hash(restored.cursor),status:'reserved'};
   await permit(input,'accept-tool-result',{invocation:intent.invocation,outcome:restored.result.outcome});
   await append(input,7,{intentFlowHash:s.rows[4].record_sha256,toolStartedHash:s.rows[5].record_sha256,outcome:restored.result.outcome,outcomeHash:hash(restored.result.outcome),resultValueHash:hash(restored.result.value),restoredInputHash:hash(restored.input),restoredInputBytes:restored.bytes,cursor:restored.cursor,reservation},{kind:'tool-result-accepted'});s=await fresh(input,{expectedSequence:7});
  }
  if(s.rows.length===7&&s.toolOffset===4){
   const restored=await restoredToolInput(input,s),accepted=s.rows[6];let startedHash;
   const decision=await executeDecision({record:copy(s.accepted),input:copy(restored.input),signal:input.signal,kind:'answer',current,
    authorizeOperation:async context=>{await permit(input,context.phase==='answer'?'answer':'provider-operation',{adapterPhase:context.phase});await fresh(input);return true;},
    fence:async()=>{await permit(input,'start-answer',{acceptedToolHash:accepted.record_sha256});const marker=await append(input,8,{toolAcceptedHash:accepted.record_sha256,reservation:{...accepted.payload.data.reservation,status:'started'}},{kind:'answer-started'});startedHash=marker.recordHash;await fresh(input,{expectedSequence:8});}});
   s=await fresh(input,{expectedSequence:8});return acceptAnswer(input,s,decision,startedHash,restored);
  }
  throw refuse();
 }
 async function continueFlow(supplied){const input=entry(supplied,true);await permit(input,'continue');let s=await database.transaction(client=>load(client,input));
  if(s.rows[s.toolOffset+4]?.payload.kind==='answer-accepted')return finalizeInput(input);s=await fresh(input);check(s.accepted.cursor.decisionsConsumed+2<=r.maximumDecisions);
  if(s.rows.length===0){check(s.base.length===2);const restored=await restoreChild(input,s);let startedHash;
   const authorizeOperation=async context=>{await permit(input,context.phase==='disclose'?'disclose-tool':context.phase==='invoke'?'invoke-tool':'provider-operation',{adapterPhase:context.phase});await fresh(input);return true;};
   const decision=await executeDecision({record:copy(s.accepted),input:copy(restored.restored),signal:input.signal,kind:'tool',authorizeOperation,current,
    fence:async()=>{await fresh(input,{expectedSequence:0});const started=await records.markSuccessorStarted({runId:input.runId,expectedClaim:input.expectedClaim,requestId:'owned-tool-provider:'+hash({runId:input.runId,reservationId:s.accepted.reservation.reservationId}),previousRecordHash:s.base[1].record_sha256,reservationId:s.accepted.reservation.reservationId});check(started.newlyStarted===true);startedHash=started.recordHash;await fresh(input,{expectedSequence:0});}});
   s=await fresh(input,{expectedSequence:0});check(startedHash===s.base[2]?.record_sha256&&hash(s.base[2].payload.claim)===hash(input.expectedClaim));exact(decision,['kind','toolId','arguments']);check(decision.kind==='tool'&&decision.toolId===p.toolId);
   const argumentsText=JSON.stringify(decision.arguments),argsValue=body(argumentsText,null,p.maximumInputBytes,reg.inputSchema);
   const intentHash=hash({startedRecordHash:startedHash,decisionOrdinal:s.accepted.reservation.nextDecisionOrdinal,decisionHash:hash(decision),argumentsHash:sha(argumentsText),restoration:restored.context});
   const invocation=createCallerToolInvocationBinding({runId:input.runId,parentInvocationId:s.accepted.parent.invocationId,attemptId:s.accepted.parent.attemptId,partId:s.accepted.parent.partId,toolId:p.toolId,bindingId:p.bindingId,argumentsHash:sha(argumentsText),intentHash});
   await permit(input,'persist-tool-intent',{invocation,intentHash});const argumentReference=await storeBody(input,argumentStore,argumentsText,{invocation,idempotencyKey:'tool-arguments:'+invocation.identityHash},p.maximumInputBytes);
   await append(input,1,{intentHash,invocation,decisionHash:hash(decision),argumentReference,argumentsValueHash:hash(argsValue),restoration:restored.context,restoredInputBytes:restored.bytes,decisionOrdinal:s.accepted.reservation.nextDecisionOrdinal});s=await fresh(input);
  }
  const intent=s.rows[0].payload.data;
  if(s.rows.length===1){const args=await argumentsFor(input,s);await permit(input,'start-tool',{invocation:intent.invocation});const marker=await append(input,2,{intentFlowHash:s.rows[0].record_sha256,invocationId:intent.invocation.invocationId});
   await fresh(input,{expectedSequence:2});await executeTool({record:copy(s.accepted),invocation:copy(intent.invocation),arguments:copy(args),signal:input.signal,current,
    authorizeOperation:async context=>{await permit(input,context.phase==='disclose'?'disclose-tool':context.phase==='invoke'?'invoke-tool':'tool-operation',{invocation:intent.invocation,adapterPhase:context.phase});await fresh(input,{expectedSequence:2});return true;},
    beforeInvoke:async()=>{const latest=await fresh(input,{expectedSequence:2});check(latest.rows[1].record_sha256===marker.recordHash&&hash(latest.rows[1].payload.claim)===hash(input.expectedClaim));}});s=await fresh(input);
  }
  if(s.rows.length===2){const result=await ownerResult(input,s);await argumentsFor(input,s);const child=await restoreChild(input,s);check(hash(child.context)===hash(intent.restoration)&&child.bytes===intent.restoredInputBytes);
   await discloseToolResult({record:copy(s.accepted),invocation:copy(intent.invocation),result:copy(result.value),signal:input.signal,current,
    authorizeOperation:async context=>{await permit(input,'disclose-tool',{invocation:intent.invocation,adapterPhase:context.phase});await fresh(input,{expectedSequence:2});return true;}});
   s=await fresh(input,{expectedSequence:2});const answer=answerInput(s,child,result),cursor={observationsHash:hash(answer.observations),completedActions:2,decisionsConsumed:intent.decisionOrdinal,contextBytes:s.accepted.cursor.contextBytes+result.outcome.resultRef.byteLength};check(cursor.contextBytes<=r.maximumContextBytes);
   const reservation={reservationId:'tool-answer:'+hash({runId:input.runId,invocationId:intent.invocation.invocationId}),kind:'provider-decision',partId:s.accepted.parent.partId,nextDecisionOrdinal:intent.decisionOrdinal+1,cursorHash:hash(cursor),status:'reserved'};
   await permit(input,'accept-tool-result',{invocation:intent.invocation,outcome:result.outcome});await append(input,3,{intentFlowHash:s.rows[0].record_sha256,toolStartedHash:s.rows[1].record_sha256,outcome:result.outcome,outcomeHash:hash(result.outcome),resultValueHash:hash(result.value),restoredInputHash:hash(answer.input),restoredInputBytes:answer.bytes,cursor,reservation});s=await fresh(input);
  }
  if(bounded)return continueBounded(input,s);
  if(s.rows.length===3){const result=await ownerResult(input,s);await argumentsFor(input,s);const child=await restoreChild(input,s),answer=answerInput(s,child,result),toolAccepted=s.rows[2];check(hash(child.context)===hash(intent.restoration)&&hash(answer.input)===toolAccepted.payload.data.restoredInputHash&&answer.bytes===toolAccepted.payload.data.restoredInputBytes);
   await discloseToolResult({record:copy(s.accepted),invocation:copy(intent.invocation),result:copy(result.value),signal:input.signal,current,authorizeOperation:async context=>{await permit(input,'disclose-tool',{invocation:intent.invocation,adapterPhase:context.phase});await fresh(input,{expectedSequence:3});return true;}});
   let answerStartedHash;const decision=await executeDecision({record:copy(s.accepted),input:copy(answer.input),signal:input.signal,kind:'answer',current,
    authorizeOperation:async context=>{await permit(input,context.phase==='answer'?'answer':'provider-operation',{invocation:intent.invocation,adapterPhase:context.phase});await fresh(input);return true;},
    fence:async()=>{await permit(input,'start-answer',{invocation:intent.invocation});const started=await append(input,4,{toolAcceptedHash:toolAccepted.record_sha256,reservation:{...toolAccepted.payload.data.reservation,status:'started'}});answerStartedHash=started.recordHash;await fresh(input,{expectedSequence:4});}});
   s=await fresh(input,{expectedSequence:4});check(s.rows[3].record_sha256===answerStartedHash&&hash(s.rows[3].payload.claim)===hash(input.expectedClaim));exact(decision,['kind','output']);check(decision.kind==='answer'&&decision.output&&Object.getPrototypeOf(decision.output)===Object.prototype);
   const outputText=JSON.stringify(decision.output);body(outputText,null,p.maximumAnswerBytes);await permit(input,'persist-answer',{invocation:intent.invocation,toolAcceptedHash:toolAccepted.record_sha256,answerStartedHash});
   const outputReference=await storeBody(input,answerStore,outputText,{answerStartedHash,inputHash:hash(answer.input),toolAcceptedHash:toolAccepted.record_sha256,idempotencyKey:'tool-answer-output:'+hash({runId:input.runId,answerStartedHash})},p.maximumAnswerBytes);
   await append(input,5,{toolAcceptedHash:toolAccepted.record_sha256,answerStartedHash,outputReference,outputValueHash:hash(decision.output),decisionOrdinal:toolAccepted.payload.data.reservation.nextDecisionOrdinal,restoredInputHash:hash(answer.input)});return finalizeInput(input);
  }
  // A started answer is never adopted from a caller-shaped acknowledgement.
  throw refuse();
 }
 function completedShape(s,output){const offset=s.toolOffset,extra=bounded?{acceptedInvocations:(offset?[0,4]:[0]).map(i=>({invocation:copy(s.rows[i].payload.data.invocation),acceptedToolHash:s.rows[i+2].record_sha256,ownerOutcomeHash:s.rows[i+2].payload.data.outcomeHash}))}:{};const metadata={...extra,mode:p.mode,descriptorHash,baseCheckpointHash:s.base[1].record_sha256,providerStartedHash:s.base[2].record_sha256,parent:copy(s.accepted.parent),invocation:copy(s.rows[offset].payload.data.invocation),acceptedToolHash:s.rows[offset+2].record_sha256,ownerOutcomeHash:s.rows[offset+2].payload.data.outcomeHash,acceptedAnswerHash:s.rows[offset+4].record_sha256,answerStartedHash:s.rows[offset+3].record_sha256,decisionOrdinal:s.rows[offset+4].payload.data.decisionOrdinal,outputReference:copy(s.rows[offset+4].payload.data.outputReference),outputValueHash:s.rows[offset+4].payload.data.outputValueHash};
  const result={output,restoredChildTool:metadata,telemetryIssues:['provider-usage-unobserved','reviewed-price-unobserved','costs-unobserved']};const finish={outcome:'completed',observed:null,usage:null,telemetryStatus:'incomplete',result,error:null,price:null,apiEquivalentCost:null,billedCost:null};const summary={restoredChildTool:metadata,telemetryStatus:'incomplete',usage:null,apiEquivalentCost:null,billedCost:null};return {result,finish,summary};}
 function confirm(s){const offset=s.toolOffset;check(s.rows.length===offset+6&&s.run.status==='completed'&&s.assignment.status==='completed'&&s.attempt.outcome==='completed');const d=s.rows[offset+5].payload.data,x=s.attempt,shape=completedShape(s,x.result_payload?.output);exact(d,['acceptedToolHash','acceptedAnswerHash','ownerOutcomeHash','resultHash','attemptFinishHash','assignmentFinishHash','finalSummaryHash']);
  check(d.acceptedToolHash===s.rows[offset+2].record_sha256&&d.acceptedAnswerHash===s.rows[offset+4].record_sha256&&d.ownerOutcomeHash===s.rows[offset+2].payload.data.outcomeHash&&d.resultHash===hash(shape.result)&&x.result_sha256===d.resultHash&&hash(x.result_payload)===d.resultHash
   &&hash(x.result_payload?.output)===s.rows[offset+4].payload.data.outputValueHash&&d.attemptFinishHash===hash(shape.finish)&&x.finish_sha256===d.attemptFinishHash&&d.assignmentFinishHash===hash({status:'completed',failure:null})&&s.assignment.finish_sha256===d.assignmentFinishHash
   &&d.finalSummaryHash===hash(shape.summary)&&hash(s.run.final_summary)===d.finalSummaryHash&&x.telemetry_status==='incomplete'&&s.assignment.failure===null&&s.run.claimed_by===null&&s.run.claimed_at===null&&s.run.lease_expires_at===null
   &&s.run.completed_at!=null&&s.assignment.completed_at!=null&&s.assignment.finished_at!=null&&x.completed_at!=null&&s.run.admitted_manifest_sha256===null
   &&['observed_provider','observed_model','observed_reasoning_effort','observed_service_tier','input_tokens','cached_input_tokens','cache_write_input_tokens','output_tokens','reasoning_output_tokens','total_tokens','price_version_id','api_equivalent_cost','api_equivalent_cost_currency','billed_cost','billed_cost_currency','error'].every(key=>x[key]===null));
  return freeze({runId:s.run.id,workflowCompleted:true,telemetryStatus:'incomplete',completionHash:s.rows[offset+5].record_sha256,acceptedToolHash:s.rows[offset+2].record_sha256,acceptedAnswerHash:s.rows[offset+4].record_sha256});}
 async function finalizeInput(input){await permit(input,'inspect-finalization');let s=await database.transaction(client=>load(client,input));const offset=s.toolOffset,acceptedSequence=offset+5;
  if(s.run.status==='completed'){await permit(input,'confirm-completion',{completionHash:s.rows[offset+5]?.record_sha256});const result=await database.transaction(async client=>{const latest=await load(client,input),result=confirm(latest);await clock(client,input.runId);input.signal?.throwIfAborted();return result;});await permit(input,'confirm-completion',{completionHash:result.completionHash});current();input.signal?.throwIfAborted();return result;}
  check(s.rows.length===acceptedSequence);s=await fresh(input,{execution:bounded,expectedSequence:acceptedSequence});for(const toolOffset of offset?[0,4]:[0]){const result=await ownerResult(input,s,{execution:bounded,offset:toolOffset});if(bounded){const invocation=s.rows[toolOffset].payload.data.invocation;await discloseToolResult({record:copy(s.accepted),invocation:copy(invocation),result:copy(result.value),signal:input.signal,current,authorizeOperation:async context=>{await permit(input,'disclose-tool',{invocation,adapterPhase:context.phase});await fresh(input,{execution:bounded,expectedSequence:acceptedSequence});return true;}});await fresh(input,{execution:bounded,expectedSequence:acceptedSequence});}}const accepted=s.rows[offset+4],target={runId:input.runId,descriptor:copy(descriptor),answerStartedHash:s.rows[offset+3].record_sha256,inputHash:accepted.payload.data.restoredInputHash,toolAcceptedHash:s.rows[offset+2].record_sha256,reference:copy(accepted.payload.data.outputReference)};
  await permit(input,'resolve-answer',{acceptedAnswerHash:accepted.record_sha256});await fresh(input,{execution:bounded,expectedSequence:acceptedSequence});check(await answerStore.attest(freeze(copy(target)),{signal:input.signal})===true);await fresh(input,{execution:bounded,expectedSequence:acceptedSequence});
  const output=body(await answerStore.resolve(freeze(copy(target)),{signal:input.signal}),target.reference,p.maximumAnswerBytes);check(output&&Object.getPrototypeOf(output)===Object.prototype&&hash(output)===accepted.payload.data.outputValueHash);await fresh(input,{execution:bounded,expectedSequence:acceptedSequence});await permit(input,'complete-workflow',{acceptedToolHash:s.rows[offset+2].record_sha256,acceptedAnswerHash:accepted.record_sha256});
  const finalized=await database.transaction(async client=>{const latest=await load(client,input);if(latest.run.status==='completed'){await permit(input,'confirm-completion',{completionHash:latest.rows[offset+5]?.record_sha256});const result=confirm(latest);await clock(client,input.runId);current();input.signal?.throwIfAborted();return result;}
   check(latest.rows.length===acceptedSequence&&latest.rows[offset+4].record_sha256===accepted.record_sha256&&latest.assignment.status==='running'&&latest.attempt.outcome===null);await custody(client,input,bounded?latest.accepted.limits.deadlineAtMs:undefined);const leaseDeadline=bounded?Math.min(ms(latest.run.lease_expires_at),latest.accepted.limits.deadlineAtMs):ms(latest.run.lease_expires_at),shape=completedShape(latest,output);
   await completeWorkflowRunInTransaction({client,journalFactory,attempt:{attemptId:latest.attempt.id,...shape.finish,expectedClaim:input.expectedClaim},assignment:{assignmentId:latest.assignment.id,status:'completed',failure:null,expectedClaim:input.expectedClaim},run:{runId:input.runId,status:'completed',finalSummary:shape.summary,expectedClaim:input.expectedClaim}});
   await permit(input,'complete-workflow',{acceptedToolHash:latest.rows[offset+2].record_sha256,acceptedAnswerHash:accepted.record_sha256});check(await clock(client,input.runId)<leaseDeadline);current();input.signal?.throwIfAborted();const data={acceptedToolHash:latest.rows[offset+2].record_sha256,acceptedAnswerHash:accepted.record_sha256,ownerOutcomeHash:latest.rows[offset+2].payload.data.outcomeHash,resultHash:hash(shape.result),attemptFinishHash:hash(shape.finish),assignmentFinishHash:hash({status:'completed',failure:null}),finalSummaryHash:hash(shape.summary)};
   const payload={schemaVersion:1,runId:input.runId,sequence:offset+6,kind:'workflow-finalized',descriptorHash,previousFlowHash:accepted.record_sha256,baseCheckpointHash:latest.base[1].record_sha256,providerStartedHash:latest.base[2].record_sha256,parent:copy(latest.accepted.parent),binding:binding(latest),claim:copy(input.expectedClaim),data};
   await client.query(`INSERT INTO ${relations.flows}(run_id,sequence,record_sha256,payload) VALUES($1,$2,$3,$4::jsonb)`,[input.runId,offset+6,hash(payload),JSON.stringify(payload)]);const result=confirm(await load(client,input));check(await clock(client,input.runId)<leaseDeadline);current();input.signal?.throwIfAborted();return result;
  });await permit(input,'confirm-completion',{completionHash:finalized.completionHash});current();input.signal?.throwIfAborted();return finalized;
 }
 current();return Object.freeze({descriptorForRun:()=>copy(descriptor),continueRestoredChildTool:safe(continueFlow),finalizeRestoredChildTool:safe(supplied=>finalizeInput(entry(supplied,false)))});
}
