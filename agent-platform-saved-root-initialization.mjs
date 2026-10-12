import {createHash} from 'node:crypto';
import {resolvePostgresRelations} from './postgres-relations.mjs';
import {normalizeExpectedClaim,assertPostgresExpectedClaim} from './agent-platform-claim-bound-journal.mjs';
import {workflowDefinitionHash} from './agent-platform-prompt.mjs';
import {compositionCheckpointRecordHash as hash} from './agent-platform-composition-checkpoint-records.mjs';
import {valueSchemaIssues} from './value-schema.mjs';
import {callerToolOutcomeDescriptor,createCallerToolInvocationBinding,validateCallerToolOutcome} from './agent-platform-caller-tool-outcome.mjs';

const copy=v=>structuredClone(v),sha=text=>createHash('sha256').update(text).digest('hex');
const freeze=v=>{if(v&&typeof v==='object'){Object.values(v).forEach(freeze);Object.freeze(v);}return v;};
const id=v=>typeof v==='string'&&/^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$/.test(v);
const hex=v=>typeof v==='string'&&/^[a-f0-9]{64}$/.test(v);
const v1Kinds=['root-decision-started','child-intent-accepted','child-decision-started','child-output-accepted'];
const v2Kinds=[...v1Kinds.slice(0,3),'child-tool-intent-accepted','child-tool-started','child-tool-result-accepted','child-answer-started','child-output-accepted'];
const commonPolicyKeys=['schemaVersion','mode','policyId','policyVersion','childPartId','maximumTaskBytes','maximumOutputBytes','taskOwnerId','taskContractId','outputOwnerId','outputContractId'];
const childToolPolicyKeys=['childStepId','childStage','toolId','bindingId','argumentsOwnerId','argumentsContractId','maximumInputBytes','maximumResultBytes'];
function refuse(){return Object.assign(new Error('Saved root initialization is unavailable.'),{code:'SAVED_ROOT_INITIALIZATION_UNAVAILABLE',statusCode:409});}
function check(v){if(!v)throw refuse();}
function exact(v,keys){check(v&&Object.getPrototypeOf(v)===Object.prototype&&Object.keys(v).sort().join('|')===[...keys].sort().join('|'));}
function policy(value){
 const toolMode=value?.mode==='saved-root-initialization.v2';exact(value,toolMode?[...commonPolicyKeys,...childToolPolicyKeys]:commonPolicyKeys);
 check(value.schemaVersion===1&&(value.mode==='saved-root-initialization.v1'||toolMode));
 for(const k of ['policyId','childPartId','taskOwnerId','taskContractId','outputOwnerId','outputContractId',...(toolMode?['childStepId','argumentsOwnerId','argumentsContractId']:[])])check(id(value[k]));
 check(Number.isSafeInteger(value.policyVersion)&&value.policyVersion>0&&value.policyVersion<=1000000);
 for(const [k,max] of [['maximumTaskBytes',8000],['maximumOutputBytes',32000],...(toolMode?[['maximumInputBytes',32000],['maximumResultBytes',128000]]:[])])check(Number.isSafeInteger(value[k])&&value[k]>0&&value[k]<=max);
 if(toolMode)check((value.childStage===null||id(value.childStage))&&typeof value.toolId==='string'&&value.toolId.trim()===value.toolId&&value.toolId.length>0&&value.toolId.length<=512&&hex(value.bindingId));
 return copy(value);
}
export function savedRootInitializationDescriptor({recordDescriptor,policy:input,registration,ownerDescriptor}={}){
 const p=policy(input);check(recordDescriptor?.protocol==='composition-checkpoint-records.v1'
  &&recordDescriptor.outputOwnerId===p.outputOwnerId&&recordDescriptor.outputContractId===p.outputContractId);
 let extra={};if(p.mode==='saved-root-initialization.v2'){
  const owner=callerToolOutcomeDescriptor({policy:ownerDescriptor});check(registration?.toolId===p.toolId&&registration.bindingId===p.bindingId&&owner.maximumResultBytes===p.maximumResultBytes);
  extra={registrationHash:hash(registration),ownerDescriptor:owner};
 }
 return freeze({...p,...extra,recordDescriptorHash:hash(recordDescriptor),definitionId:recordDescriptor.definitionId,
  definitionHash:recordDescriptor.definitionHash,runtimeBindingHash:recordDescriptor.runtimeBindingHash,assignmentHash:recordDescriptor.assignmentHash});
}
export function getSavedRootInitializationSchemaUrl({mode='saved-root-initialization.v1'}={}){
 check(['saved-root-initialization.v1','saved-root-initialization.v2'].includes(mode));
 if(mode==='saved-root-initialization.v2')return new URL('./agent-platform-sql/saved-root-initialization-v2.sql',import.meta.url);
 return new URL('./agent-platform-sql/saved-root-initialization-v1.sql',import.meta.url);
}

/** One admitted root/answer-only-child bridge into unchanged records v1/v2.
 * Public entries contain no decisions, task bodies, output references or grants.
 * A started operation without an accepted outcome is never dispatched again.
 */
export function createSavedRootInitialization({configuration,definition,assignment,rootPartId,validateAttempt,inputForPart,executeDecision,authorizeChildOutput,executeChildTool,discloseChildToolResult}={}){
 const {database,records,runtimeBinding,getPolicy,getRecordPolicy,authorize,taskStore,outputStore,attestProvider}=configuration??{};
 const p=policy(configuration?.policy),toolMode=p.mode==='saved-root-initialization.v2',kinds=toolMode?v2Kinds:v1Kinds;
 const childTool=toolMode?configuration.childTool:null,argumentStore=childTool?.argumentStore,owner=childTool?.owner,registration=toolMode?freeze(copy(childTool?.registration)):null;
 const r=records?.descriptorForRun(),descriptor=savedRootInitializationDescriptor({recordDescriptor:r,policy:p,...(toolMode?{registration,ownerDescriptor:owner?.descriptor}:{})}),descriptorHash=hash(descriptor);
 if(toolMode){
  exact(childTool,['registration','argumentStore','owner']);exact(argumentStore,['ownerId','contractId','write','attest','resolve']);exact(owner,['descriptor','getDescriptor','lookup','attest','resolve']);
  for(const fn of [executeChildTool,discloseChildToolResult,...['write','attest','resolve'].map(k=>argumentStore[k]),...['getDescriptor','lookup','attest','resolve'].map(k=>owner[k])])check(typeof fn==='function');
  check(argumentStore.ownerId===p.argumentsOwnerId&&argumentStore.contractId===p.argumentsContractId);
  const authored=definition.toolAssignments.parts.find(part=>part.partId===p.childPartId);
  check(authored?.stepId===p.childStepId&&authored.stage===p.childStage&&authored.tools.some(ref=>ref.toolId===p.toolId&&ref.bindingId===p.bindingId)
   &&assignment.parts.find(part=>part.partId===p.childPartId)?.tools.some(ref=>ref.toolId===p.toolId&&ref.bindingId===p.bindingId));
 }
 const outputSequence=toolMode?8:4,outputStartSequence=toolMode?7:3,initialDecisions=toolMode?3:2;
 const childMethods=toolMode?[...['write','attest','resolve'].map(k=>[argumentStore,k,argumentStore[k]]),...['getDescriptor','lookup','attest','resolve'].map(k=>[owner,k,owner[k]])]:[];
 const assignmentSnapshotHash=hash(assignment);
 const continuationDescriptorHash=configuration.continuationDescriptorHash;check(hex(continuationDescriptorHash)&&typeof configuration.validateContinuation==='function');
 check(typeof database?.transaction==='function'&&definition.definitionHash===workflowDefinitionHash(definition)
  &&definition.id===descriptor.definitionId&&definition.definitionHash===descriptor.definitionHash&&assignment.assignmentHash===descriptor.assignmentHash
  &&hash(runtimeBinding)===descriptor.runtimeBindingHash&&id(rootPartId));
 for(const fn of [getPolicy,getRecordPolicy,authorize,attestProvider,validateAttempt,inputForPart,executeDecision,authorizeChildOutput])check(typeof fn==='function');
 for(const [store,owner,contract] of [[taskStore,p.taskOwnerId,p.taskContractId],[outputStore,p.outputOwnerId,p.outputContractId]]){
  check(store?.ownerId===owner&&store.contractId===contract);for(const method of ['write','attest','resolve'])check(typeof store[method]==='function');
 }
 const relations=resolvePostgresRelations(configuration.relations,['runs','versions','assignments','attempts','events','heads','records','initializations'],'Saved root initialization');
 const pinnedRecordPolicy=copy(r);for(const key of ['definitionId','definitionHash','runtimeBindingHash','assignmentHash'])delete pinnedRecordPolicy[key];
 const current=()=>{configuration.validateContinuation();check(configuration.continuationDescriptorHash===continuationDescriptorHash&&hash(policy(getPolicy()))===hash(p)&&hash(getRecordPolicy())===hash(pinnedRecordPolicy)&&hash(records.descriptorForRun())===hash(r)
  &&hash(runtimeBinding)===descriptor.runtimeBindingHash&&taskStore.ownerId===p.taskOwnerId&&taskStore.contractId===p.taskContractId
  &&outputStore.ownerId===p.outputOwnerId&&outputStore.contractId===p.outputContractId
  &&(!toolMode||(workflowDefinitionHash(definition)===descriptor.definitionHash&&hash(assignment)===assignmentSnapshotHash&&childMethods.every(([object,key,method])=>object[key]===method)&&hash(configuration.childTool.registration)===descriptor.registrationHash&&configuration.childTool.argumentStore===argumentStore&&configuration.childTool.owner===owner
   &&argumentStore.ownerId===p.argumentsOwnerId&&argumentStore.contractId===p.argumentsContractId&&hash(callerToolOutcomeDescriptor({policy:owner.getDescriptor()}))===hash(descriptor.ownerDescriptor))));};
 const entry=supplied=>{
  exact(supplied,Object.hasOwn(supplied??{},'signal')?['runId','expectedClaim','signal']:['runId','expectedClaim']);
  check(id(supplied.runId));const expectedClaim=normalizeExpectedClaim(supplied.expectedClaim);check(expectedClaim&&expectedClaim.runId===supplied.runId);
  return{runId:supplied.runId,expectedClaim,signal:supplied.signal};
 };
 async function permit(input,phase,extra={}){current();input.signal?.throwIfAborted();check(await authorize(freeze({phase,runId:input.runId,expectedClaim:copy(input.expectedClaim),descriptor:copy(descriptor),...copy(extra)}))===true);current();input.signal?.throwIfAborted();}
 async function custody(client,input,deadline){
  current();input.signal?.throwIfAborted();await assertPostgresExpectedClaim(client,relations,input.runId,input.expectedClaim);
  const s=(await client.query(`SELECT clock_timestamp() AS checked_at,EXISTS(SELECT 1 FROM ${relations.events} WHERE run_id=$1 AND source_key='workflow.cancellation-requested.v1') AS cancelled`,[input.runId])).rows[0];
  const now=s.checked_at instanceof Date?s.checked_at.getTime():Date.parse(s.checked_at);
  check(!s.cancelled&&Number.isFinite(now)&&now<deadline);current();input.signal?.throwIfAborted();
 }
 async function load(client,input){
  const run=(await client.query(`SELECT * FROM ${relations.runs} WHERE id=$1 FOR UPDATE`,[input.runId])).rows[0];
  const assignments=(await client.query(`SELECT * FROM ${relations.assignments} WHERE run_id=$1`,[input.runId])).rows;
  const attempts=(await client.query(`SELECT * FROM ${relations.attempts} WHERE run_id=$1`,[input.runId])).rows;
  check(run&&run.status==='running'&&run.resumed_from_run_id===null&&run.run_family_budget_version!==3&&assignments.length===1&&attempts.length===1
   &&run.workflow_version_id===definition.id&&hash(run.authorization_snapshot.savedRootInitialization)===descriptorHash
   &&hash(run.authorization_snapshot.compositionCheckpointProtocol)===hash(r)&&hash(run.authorization_snapshot.assignedRuntime)===descriptor.runtimeBindingHash
   &&hash(run.authorization_snapshot.restoredChildOwnedTool)===continuationDescriptorHash&&hash(run.input_payload)===run.input_sha256);
  const a=assignments[0],x=attempts[0];check(a.parent_assignment_id===null&&a.execution==='current-agent'&&a.status==='running'
   &&x.assignment_id===a.id&&x.attempt_ordinal===1&&x.outcome===null&&x.resource_reserved_total_tokens===null
   &&x.thread_id===a.agent_thread_id&&id(x.thread_id)&&hash(x.prompt)===x.prompt_sha256&&hash(x.capability_profile)===x.capability_sha256);
  const version=(await client.query(`SELECT definition,definition_sha256 FROM ${relations.versions} WHERE id=$1`,[definition.id])).rows[0];
  check(version&&version.definition_sha256===definition.definitionHash&&workflowDefinitionHash(version.definition)===definition.definitionHash);
  validateAttempt({run,assignment:a,attempt:x});
  const deadline=x.capability_profile.deadlineAtMs;check(Number.isSafeInteger(deadline)&&deadline>0);await custody(client,input,deadline);
  const parent={invocationId:'initial-parent:'+hash({runId:input.runId,attemptId:x.id}),partId:rootPartId,assignmentId:a.id,attemptId:x.id,threadId:x.thread_id};
  const limits={maximumDecisions:r.maximumDecisions,maximumContextBytes:r.maximumContextBytes,deadlineAtMs:deadline};
  const rows=(await client.query(`SELECT * FROM ${relations.initializations} WHERE run_id=$1 ORDER BY sequence`,[input.runId])).rows;
  check(rows.length<=outputSequence);let previous=null;
  for(let i=0;i<rows.length;i++){
   const row=rows[i],v=row.payload;exact(v,['schemaVersion','runId','sequence','kind','descriptorHash','previousRecordHash','parent','limits','claim','data']);
   check(row.sequence===i+1&&v.schemaVersion===1&&v.runId===input.runId&&v.sequence===row.sequence&&v.kind===kinds[i]
    &&v.descriptorHash===descriptorHash&&v.previousRecordHash===previous&&row.record_sha256===hash(v)&&hash(v.parent)===hash(parent)&&hash(v.limits)===hash(limits));
   check(hash(v.claim)===hash(rows[0].payload.claim));previous=row.record_sha256;
  }
  if(rows[0]){const d=rows[0].payload.data;exact(d,['inputHash','inputBytes']);check(hex(d.inputHash)&&Number.isSafeInteger(d.inputBytes)&&d.inputBytes>0&&d.inputBytes<=r.maximumContextBytes);}
  if(rows[1]){const d=rows[1].payload.data;exact(d,['rootStartedHash','decisionHash','taskReference','taskValueHash','child','decisionsConsumed','contextBytes']);
   check(d.rootStartedHash===rows[0].record_sha256&&hex(d.decisionHash)&&hex(d.taskValueHash)&&d.decisionsConsumed===1&&d.contextBytes===d.taskReference.byteLength);
   exact(d.child,['invocationId','partId','threadId','taskHash']);check(d.child.partId===p.childPartId&&d.child.taskHash===d.taskValueHash&&id(d.child.invocationId)&&id(d.child.threadId)&&d.child.threadId!==parent.threadId);
   reference(d.taskReference,taskStore,p.maximumTaskBytes);}
  if(rows[2]){const d=rows[2].payload.data;exact(d,['childIntentHash','inputHash','inputBytes']);check(d.childIntentHash===rows[1].record_sha256&&hex(d.inputHash)&&Number.isSafeInteger(d.inputBytes)&&d.inputBytes>0&&d.inputBytes<=r.maximumContextBytes);}
  if(toolMode){
   if(rows[3]){const d=rows[3].payload.data;exact(d,['childStartedHash','decisionHash','intentHash','invocation','argumentReference','argumentsValueHash']);
    check(d.childStartedHash===rows[2].record_sha256&&hex(d.decisionHash)&&hex(d.intentHash)&&hex(d.argumentsValueHash));
    reference(d.argumentReference,argumentStore,p.maximumInputBytes);
    const invocation=createCallerToolInvocationBinding({runId:input.runId,attemptId:parent.attemptId,parentInvocationId:rows[1].payload.data.child.invocationId,
     partId:p.childPartId,toolId:p.toolId,bindingId:p.bindingId,argumentsHash:d.argumentReference.sha256,intentHash:d.intentHash});
    check(hash(d.invocation)===hash(invocation)&&d.intentHash===hash({childStartedHash:rows[2].record_sha256,decisionHash:d.decisionHash,argumentsHash:d.argumentReference.sha256}));
   }
   if(rows[4]){const d=rows[4].payload.data;exact(d,['toolIntentHash','invocationId']);check(d.toolIntentHash===rows[3].record_sha256&&d.invocationId===rows[3].payload.data.invocation.invocationId);}
   if(rows[5]){const d=rows[5].payload.data;exact(d,['toolIntentHash','toolStartedHash','outcome','outcomeHash','resultValueHash','decisionsConsumed','contextBytes']);
    const outcome=validateCallerToolOutcome(d.outcome,{descriptor:descriptor.ownerDescriptor,invocation:rows[3].payload.data.invocation});
    check(d.toolIntentHash===rows[3].record_sha256&&d.toolStartedHash===rows[4].record_sha256&&outcome.kind==='applied-with-exact-result'&&d.outcomeHash===hash(outcome)&&hex(d.resultValueHash)
     &&d.decisionsConsumed===2&&d.contextBytes===rows[1].payload.data.contextBytes+outcome.resultRef.byteLength&&d.contextBytes<=r.maximumContextBytes);
   }
   if(rows[6]){const d=rows[6].payload.data;exact(d,['toolAcceptedHash','inputHash','inputBytes']);check(d.toolAcceptedHash===rows[5].record_sha256&&hex(d.inputHash)&&Number.isSafeInteger(d.inputBytes)&&d.inputBytes>0&&d.inputBytes<=r.maximumContextBytes);}
  }
  if(rows[outputSequence-1]){const d=rows[outputSequence-1].payload.data;exact(d,['childStartedHash','decisionHash','outputReference','outputValueHash','retirementId','decisionsConsumed','contextBytes']);
   check(d.childStartedHash===rows[outputStartSequence-1].record_sha256&&hex(d.decisionHash)&&hex(d.outputValueHash)&&id(d.retirementId)&&d.decisionsConsumed===initialDecisions
    &&d.contextBytes===(toolMode?rows[5]:rows[1]).payload.data.contextBytes+d.outputReference.byteLength&&d.contextBytes<=r.maximumContextBytes);reference(d.outputReference,outputStore,p.maximumOutputBytes);}

  return{run,parent,limits,rows};
 }
 async function fresh(input){await permit(input,'custody');return database.transaction(client=>load(client,input));}
 async function append(input,sequence,data){await permit(input,'commit-initialization',{sequence});return database.transaction(async client=>{
  const s=await load(client,input);check(s.rows.length===sequence-1);if(s.rows.length)check(hash(s.rows[0].payload.claim)===hash(input.expectedClaim));
  const payload={schemaVersion:1,runId:input.runId,sequence,kind:kinds[sequence-1],descriptorHash,previousRecordHash:s.rows.at(-1)?.record_sha256??null,parent:s.parent,limits:s.limits,claim:copy(input.expectedClaim),data:copy(data)};
  const recordHash=hash(payload);await client.query(`INSERT INTO ${relations.initializations}(run_id,sequence,record_sha256,payload) VALUES($1,$2,$3,$4::jsonb)`,[input.runId,sequence,recordHash,JSON.stringify(payload)]);
  await permit(input,'commit-initialization',{sequence});await custody(client,input,s.limits.deadlineAtMs);return{payload,recordHash};
 });}
 function reference(ref,store,max){exact(ref,['ownerId','contractId','referenceId','sha256','byteLength']);check(ref.ownerId===store.ownerId&&ref.contractId===store.contractId&&id(ref.referenceId)&&hex(ref.sha256)&&Number.isSafeInteger(ref.byteLength)&&ref.byteLength>0&&ref.byteLength<=max);return copy(ref);}
 function body(text,ref,max){check(typeof text==='string'&&Buffer.byteLength(text)<=max);if(ref)check(sha(text)===ref.sha256&&Buffer.byteLength(text)===ref.byteLength);return JSON.parse(text);}
 async function owned(input,store,referenceValue,target,max){
  const ref=reference(referenceValue,store,max);const scope={runId:input.runId,descriptor:copy(descriptor),...copy(target),reference:ref};
  const work=async(disclosure=async()=>{})=>{
   await permit(input,store===taskStore?'resolve-task':store===argumentStore?'resolve-child-tool-arguments':'disclose-child',scope);await fresh(input);
   await disclosure();check(await store.attest(freeze(copy(scope)),{signal:input.signal})===true);await fresh(input);await disclosure();
   const text=await store.resolve(freeze(copy(scope)),{signal:input.signal,...(toolMode&&store===outputStore?{disclosure}:{})});await fresh(input);await disclosure();const value=body(text,ref,max);
   check(hash(value)===target.valueHash);return{value,text,reference:ref};
  };
  if(store===taskStore||store===argumentStore)return work();
  const s=await fresh(input);check(s.rows.length>=3);
  return authorizeChildOutput({parent:copy(s.parent),child:copy(s.rows[1].payload.data.child),deadlineAtMs:s.limits.deadlineAtMs,signal:input.signal,current,
   authorizeOperation:async context=>{await permit(input,'disclose-child',{adapterPhase:context.phase});await fresh(input);return true;},operation:work});
 }
 async function store(input,owner,text,target,max){
  const value=body(text,null,max),t={runId:input.runId,descriptor:copy(descriptor),...copy(target),text,sha256:sha(text),byteLength:Buffer.byteLength(text),valueHash:hash(value)};
  await permit(input,'store-initialization-output',{ownerId:owner.ownerId});await fresh(input);
  const ref=reference(await owner.write(freeze(copy(t)),{signal:input.signal}),owner,max);await fresh(input);
  check(ref.sha256===t.sha256&&ref.byteLength===t.byteLength);const saved=await owned(input,owner,ref,{...target,valueHash:t.valueHash},max);check(saved.text===text);return saved;
 }
 function boundedInput(s,partId,task,observations=[]){const input=inputForPart(partId,task,observations);const bytes=Buffer.byteLength(JSON.stringify(input));check(bytes>0&&bytes<=r.maximumContextBytes);return{input,bytes};}
 async function decision(input,s,phase,partId,task,start,observations=[]){
  const prepared=boundedInput(s,partId,task,observations);check(hash(prepared.input)===start.payload.data.inputHash&&prepared.bytes===start.payload.data.inputBytes);
  await fresh(input);const result=await executeDecision({phase,partId,parent:copy(s.parent),child:copy(s.rows[1]?.payload.data.child??null),input:copy(prepared.input),deadlineAtMs:s.limits.deadlineAtMs,signal:input.signal,
   current,authorizeOperation:async context=>{await permit(input,'provider-operation',{phase,adapterPhase:context.phase});await fresh(input);return true;},
   fence:async()=>{const latest=await fresh(input);check(latest.rows.length===(phase==='root'?1:phase==='child-answer'?7:3)&&latest.rows.at(-1).record_sha256===start.recordHash&&hash(latest.rows[0].payload.claim)===hash(input.expectedClaim));}});
  await fresh(input);const target={runId:input.runId,phase,startHash:start.recordHash,inputHash:hash(prepared.input),decisionHash:hash(result.decision),evidenceHash:hash(result.evidence),decision:copy(result.decision),evidence:copy(result.evidence),invocationId:phase==='root'?s.parent.invocationId:s.rows[1].payload.data.child.invocationId};
  await permit(input,'attest-provider',{phase,startHash:start.recordHash});check(await attestProvider(freeze(copy(target)),{signal:input.signal})===true);await fresh(input);return{...result,target};
 }
 function childArgs(s,input,extra={}){return{parent:copy(s.parent),child:copy(s.rows[1].payload.data.child),deadlineAtMs:s.limits.deadlineAtMs,signal:input.signal,current,
  authorizeOperation:async context=>{await permit(input,'child-tool-operation',{adapterPhase:context.phase});await fresh(input);return true;},...extra};}
 async function acceptToolIntent(input,s,result,start){
  exact(result.decision,['kind','toolId','arguments']);check(result.decision.kind==='tool'&&result.decision.toolId===p.toolId&&valueSchemaIssues(result.decision.arguments,registration.inputSchema).length===0);
  const text=JSON.stringify(result.decision.arguments),intentHash=hash({childStartedHash:start.recordHash,decisionHash:result.target.decisionHash,argumentsHash:sha(text)});
  const invocation=createCallerToolInvocationBinding({runId:input.runId,attemptId:s.parent.attemptId,parentInvocationId:s.rows[1].payload.data.child.invocationId,
   partId:p.childPartId,toolId:p.toolId,bindingId:p.bindingId,argumentsHash:sha(text),intentHash});
  const args=await store(input,argumentStore,text,{invocation,idempotencyKey:invocation.invocationId},p.maximumInputBytes);
  await append(input,4,{childStartedHash:start.recordHash,decisionHash:result.target.decisionHash,intentHash,invocation,argumentReference:args.reference,argumentsValueHash:hash(args.value)});
 }
 async function argumentsFor(input,s){
  const intent=s.rows[3].payload.data,args=await owned(input,argumentStore,intent.argumentReference,{invocation:copy(intent.invocation),idempotencyKey:intent.invocation.invocationId,valueHash:intent.argumentsValueHash},p.maximumInputBytes);
  check(JSON.stringify(args.value)===args.text&&valueSchemaIssues(args.value,registration.inputSchema).length===0
   &&hash({kind:'tool',toolId:p.toolId,arguments:args.value})===intent.decisionHash);return args.value;
 }
 async function ownerResult(input,s){
  const intent=s.rows[3].payload.data,target={runId:input.runId,descriptor:copy(descriptor),invocation:copy(intent.invocation),argumentReference:copy(intent.argumentReference)};
  return discloseChildToolResult(childArgs(s,input,{invocation:copy(intent.invocation),operation:async disclosure=>{
   await disclosure();await permit(input,'lookup-child-tool-outcome',target);await fresh(input);
   const raw=await owner.lookup(freeze(copy(target)),{signal:input.signal});await fresh(input);await disclosure();
   const outcome=validateCallerToolOutcome(raw,{descriptor:descriptor.ownerDescriptor,invocation:intent.invocation});check(outcome.kind==='applied-with-exact-result');
   const attested={...target,outcome:copy(outcome)};await disclosure();check(await owner.attest(freeze(copy(attested)),{signal:input.signal})===true);await fresh(input);await disclosure();
   await permit(input,'resolve-child-tool-result',{invocation:intent.invocation,outcome});await fresh(input);await disclosure();
   const text=await owner.resolve(freeze(copy(attested)),{signal:input.signal,disclosure});await fresh(input);await disclosure();
   const value=body(text,outcome.resultRef,p.maximumResultBytes);check(JSON.stringify(value)===text&&valueSchemaIssues(value,registration.resultSchema).length===0);
   if(s.rows.length>=6)check(hash(outcome)===s.rows[5].payload.data.outcomeHash&&hash(value)===s.rows[5].payload.data.resultValueHash);
   return{outcome,value,text};
  }}));
 }
 async function taskFor(input,s){const intent=s.rows[1].payload.data;return owned(input,taskStore,intent.taskReference,
  {idempotencyKey:'initial-task:'+s.rows[0].record_sha256,startHash:s.rows[0].record_sha256,inputHash:s.rows[0].payload.data.inputHash,decisionHash:intent.decisionHash,valueHash:intent.taskValueHash},p.maximumTaskBytes);}
 async function choiceContext(input,s){
  const task=await taskFor(input,s),chosen=boundedInput(s,p.childPartId,task.value);
  check(hash(chosen.input)===s.rows[2].payload.data.inputHash&&chosen.bytes===s.rows[2].payload.data.inputBytes);return task;
 }
 async function verifyChildContext(input,s){
  await argumentsFor(input,s);const result=await ownerResult(input,s),task=await choiceContext(input,s);
  const answered=boundedInput(s,p.childPartId,task.value,[{kind:'tool',toolId:p.toolId,result:result.value}]);
  check(hash(answered.input)===s.rows[6].payload.data.inputHash&&answered.bytes===s.rows[6].payload.data.inputBytes);
 }
 async function advanceChildTool(input,s){
  if(s.rows.length===4){
   await choiceContext(input,s);const args=await argumentsFor(input,s),intent=s.rows[3].payload.data;
   const start=await append(input,5,{toolIntentHash:s.rows[3].record_sha256,invocationId:intent.invocation.invocationId});s=await fresh(input);
   await executeChildTool(childArgs(s,input,{invocation:copy(intent.invocation),arguments:copy(args),beforeInvoke:async()=>{
    const latest=await fresh(input);check(latest.rows.length===5&&latest.rows[4].record_sha256===start.recordHash&&hash(latest.rows[0].payload.claim)===hash(input.expectedClaim));
   }}));s=await fresh(input);
  }
  if(s.rows.length===5){
   await argumentsFor(input,s);const result=await ownerResult(input,s);check(s.rows[1].payload.data.contextBytes+result.outcome.resultRef.byteLength<=r.maximumContextBytes);
   await append(input,6,{toolIntentHash:s.rows[3].record_sha256,toolStartedHash:s.rows[4].record_sha256,outcome:result.outcome,outcomeHash:hash(result.outcome),resultValueHash:hash(result.value),decisionsConsumed:2,contextBytes:s.rows[1].payload.data.contextBytes+result.outcome.resultRef.byteLength});s=await fresh(input);
  }
  if(s.rows.length===6){
   await argumentsFor(input,s);const result=await ownerResult(input,s),task=await choiceContext(input,s),observations=[{kind:'tool',toolId:p.toolId,result:result.value}];
   const prepared=boundedInput(s,p.childPartId,task.value,observations);
   const start=await append(input,7,{toolAcceptedHash:s.rows[5].record_sha256,inputHash:hash(prepared.input),inputBytes:prepared.bytes});s=await fresh(input);
   const response=await decision(input,s,'child-answer',p.childPartId,task.value,start,observations);
   exact(response.decision,['kind','output']);check(response.decision.kind==='answer'&&response.decision.output&&Object.getPrototypeOf(response.decision.output)===Object.prototype);
   const text=JSON.stringify(response.decision.output);check(s.rows[5].payload.data.contextBytes+Buffer.byteLength(text)<=r.maximumContextBytes);
   const output=await store(input,outputStore,text,{idempotencyKey:'initial-output:'+start.recordHash,startHash:start.recordHash,inputHash:response.target.inputHash,decisionHash:response.target.decisionHash},p.maximumOutputBytes);
   await append(input,8,{childStartedHash:start.recordHash,decisionHash:response.target.decisionHash,outputReference:output.reference,outputValueHash:hash(output.value),retirementId:'initial-retired:'+start.recordHash,decisionsConsumed:3,contextBytes:s.rows[5].payload.data.contextBytes+output.reference.byteLength});s=await fresh(input);
  }else check(s.rows.length!==7); // An existing child answer start is uncertain.
  return s;
 }
 async function initialize(supplied){
  const input=entry(supplied);await permit(input,'initialize');let s=await fresh(input);
  check(r.maximumDecisions>=(toolMode?5:4));
  let base=await records.inspect({runId:input.runId,expectedClaim:input.expectedClaim});await fresh(input);
  if(base.state==='child-accepted-cursor-reserved'||base.state==='successor-started'){
   check(s.rows.length===outputSequence);if(toolMode)await verifyChildContext(input,s);const accepted=s.rows[outputSequence-1].payload.data;
   check(hash(base.record.parent)===hash(s.parent)&&base.record.child.invocationId===s.rows[1].payload.data.child.invocationId
    &&hash(base.record.child.outputReference)===hash(accepted.outputReference)&&base.record.cursor.decisionsConsumed===initialDecisions&&base.record.cursor.contextBytes===accepted.contextBytes);
   await owned(input,outputStore,accepted.outputReference,{idempotencyKey:'initial-output:'+s.rows[outputStartSequence-1].record_sha256,startHash:s.rows[outputStartSequence-1].record_sha256,inputHash:s.rows[outputStartSequence-1].payload.data.inputHash,decisionHash:accepted.decisionHash,valueHash:accepted.outputValueHash},p.maximumOutputBytes);
   await authorizeChildOutput({parent:copy(s.parent),child:copy(s.rows[1].payload.data.child),deadlineAtMs:s.limits.deadlineAtMs,signal:input.signal,current,authorizeOperation:async context=>{await permit(input,'disclose-child',{adapterPhase:context.phase});await fresh(input);return true;}});await fresh(input);
   return freeze({initialized:true,newlyInitialized:false,checkpointHash:base.recordHash});
  }
  if(s.rows.length)check(hash(s.rows[0].payload.claim)===hash(input.expectedClaim));
  if(s.rows.length===0){
   check(base.state==='absent');const prepared=boundedInput(s,rootPartId,configuration.rootPrompt);
   const start=await append(input,1,{inputHash:hash(prepared.input),inputBytes:prepared.bytes});
   s=await fresh(input);const result=await decision(input,s,'root',rootPartId,configuration.rootPrompt,start);
   exact(result.decision,['kind','partId','task']);check(result.decision.kind==='child'&&result.decision.partId===p.childPartId&&typeof result.decision.task==='string'&&result.decision.task.trim());
   const taskText=JSON.stringify(result.decision.task);check(Buffer.byteLength(taskText)<=p.maximumTaskBytes&&Buffer.byteLength(taskText)<=r.maximumContextBytes);
   const task=await store(input,taskStore,taskText,{idempotencyKey:'initial-task:'+start.recordHash,startHash:start.recordHash,inputHash:result.target.inputHash,decisionHash:result.target.decisionHash},p.maximumTaskBytes);
   const child={invocationId:'initial-child:'+hash({runId:input.runId,parent:s.parent.invocationId,taskHash:hash(task.value)}),partId:p.childPartId,threadId:'initial-child-thread:'+hash({runId:input.runId,parent:s.parent.invocationId}),taskHash:hash(task.value)};
   await append(input,2,{rootStartedHash:start.recordHash,decisionHash:result.target.decisionHash,taskReference:task.reference,taskValueHash:hash(task.value),child,decisionsConsumed:1,contextBytes:task.reference.byteLength});s=await fresh(input);
  }else check(s.rows.length!==1); // An existing started provider is uncertain.
  if(s.rows.length===2){
   check(base.state==='absent');const intent=s.rows[1].payload.data;
   const task=await owned(input,taskStore,intent.taskReference,{idempotencyKey:'initial-task:'+s.rows[0].record_sha256,startHash:s.rows[0].record_sha256,inputHash:s.rows[0].payload.data.inputHash,decisionHash:intent.decisionHash,valueHash:intent.taskValueHash},p.maximumTaskBytes);
   check(typeof task.value==='string'&&task.value.trim());const prepared=boundedInput(s,p.childPartId,task.value);
   const begun=await records.beginChild({runId:input.runId,expectedClaim:input.expectedClaim,requestId:'initial-begin:'+s.rows[1].record_sha256,previousRecordHash:null,parent:s.parent,child:intent.child});
   check(begun.newlyRecorded===true);await fresh(input);
   const start=await append(input,3,{childIntentHash:s.rows[1].record_sha256,inputHash:hash(prepared.input),inputBytes:prepared.bytes});
   s=await fresh(input);const result=await decision(input,s,'child',p.childPartId,task.value,start);
   if(toolMode){await acceptToolIntent(input,s,result,start);s=await fresh(input);}
   else{exact(result.decision,['kind','output']);check(result.decision.kind==='answer'&&result.decision.output&&Object.getPrototypeOf(result.decision.output)===Object.prototype);
   const outputText=JSON.stringify(result.decision.output);check(intent.contextBytes+Buffer.byteLength(outputText)<=r.maximumContextBytes);
   const output=await store(input,outputStore,outputText,{idempotencyKey:'initial-output:'+start.recordHash,startHash:start.recordHash,inputHash:result.target.inputHash,decisionHash:result.target.decisionHash},p.maximumOutputBytes);
   check(intent.contextBytes+output.reference.byteLength<=r.maximumContextBytes);
   await authorizeChildOutput({parent:copy(s.parent),child:copy(intent.child),deadlineAtMs:s.limits.deadlineAtMs,signal:input.signal,current,authorizeOperation:async context=>{await permit(input,'disclose-child',{adapterPhase:context.phase});await fresh(input);return true;}});await fresh(input);
   await append(input,4,{childStartedHash:start.recordHash,decisionHash:result.target.decisionHash,outputReference:output.reference,outputValueHash:hash(output.value),retirementId:'initial-retired:'+start.recordHash,decisionsConsumed:2,contextBytes:intent.contextBytes+output.reference.byteLength});s=await fresh(input);}
  }else check(s.rows.length!==3);
  if(toolMode){s=await advanceChildTool(input,s);}
  check(s.rows.length===outputSequence);if(toolMode)await verifyChildContext(input,s);const accepted=s.rows[outputSequence-1].payload.data;
  const output=await owned(input,outputStore,accepted.outputReference,{idempotencyKey:'initial-output:'+s.rows[outputStartSequence-1].record_sha256,startHash:s.rows[outputStartSequence-1].record_sha256,inputHash:s.rows[outputStartSequence-1].payload.data.inputHash,decisionHash:accepted.decisionHash,valueHash:accepted.outputValueHash},p.maximumOutputBytes);
  check(output.value&&Object.getPrototypeOf(output.value)===Object.prototype);
  await authorizeChildOutput({parent:copy(s.parent),child:copy(s.rows[1].payload.data.child),deadlineAtMs:s.limits.deadlineAtMs,signal:input.signal,current,authorizeOperation:async context=>{await permit(input,'disclose-child',{adapterPhase:context.phase});await fresh(input);return true;}});await fresh(input);
  base=await records.inspect({runId:input.runId,expectedClaim:input.expectedClaim});check(base.state==='child-started'&&hash(base.record.parent)===hash(s.parent)&&base.record.child.invocationId===s.rows[1].payload.data.child.invocationId);
  const checkpoint=await records.acceptChildAndReserve({runId:input.runId,expectedClaim:input.expectedClaim,requestId:'initial-accept:'+s.rows[outputSequence-1].record_sha256,previousRecordHash:base.recordHash,outputReference:accepted.outputReference,retirementId:accepted.retirementId,decisionsConsumed:initialDecisions,contextBytes:accepted.contextBytes,reservationId:'initial-successor:'+s.rows[outputSequence-1].record_sha256});
  await fresh(input);return freeze({initialized:true,newlyInitialized:checkpoint.newlyRecorded,checkpointHash:checkpoint.recordHash});
 }
 return Object.freeze({descriptorForRun:()=>copy(descriptor),initializeSavedRoot:async input=>{try{return await initialize(input);}catch{throw refuse();}}});
}
