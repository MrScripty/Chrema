import {createHash} from 'node:crypto';
import {readFile} from 'node:fs/promises';
import {resolvePostgresRelations} from '../shared/postgres-relations.mjs';
import {compositionCheckpointRecordHash as hash} from './agent-platform-composition-checkpoint-records.mjs';
import {reservedAnswerFinalizationDescriptor} from './agent-platform-reserved-answer-finalization.mjs';
import {reservedChildHostContinuationDescriptor} from './agent-platform-reserved-child-host-continuation.mjs';
import {createToolRegistration} from './agent-platform-tool-assignment.mjs';

const copy=value=>structuredClone(value);
const freeze=value=>{if(value&&typeof value==='object'){Object.values(value).forEach(freeze);Object.freeze(value);}return value;};
const identity=value=>typeof value==='string'&&/^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$/.test(value);
const sourceIdentity=value=>typeof value==='string'&&value.length>0&&value.length<=512&&value.trim()===value;
const digest=value=>typeof value==='string'&&/^[a-f0-9]{64}$/.test(value);
function refuse(){return Object.assign(new Error('Checkpoint status inspection is unavailable.'),{code:'CHECKPOINT_STATUS_UNAVAILABLE',statusCode:409});}
function check(value){if(!value)throw refuse();}
function exact(value,keys){check(value&&Object.getPrototypeOf(value)===Object.prototype&&Object.keys(value).sort().join('|')===[...keys].sort().join('|'));}
function normalizePolicy(value){
 exact(value,['schemaVersion','mode','policyId','policyVersion','rootPartId','rootStepId','recordDescriptor','continuationDescriptor','finalizationDescriptor','maximumResultBytes','maximumSnapshotBytes','statementTimeoutMs']);
 check(value.schemaVersion===1&&value.mode==='checkpoint-status-read.v1'&&identity(value.policyId)&&Number.isSafeInteger(value.policyVersion)&&value.policyVersion>0&&identity(value.rootPartId)&&sourceIdentity(value.rootStepId));
 check(Number.isSafeInteger(value.maximumResultBytes)&&value.maximumResultBytes>=1024&&value.maximumResultBytes<=8192
  &&Number.isSafeInteger(value.maximumSnapshotBytes)&&value.maximumSnapshotBytes>=8192&&value.maximumSnapshotBytes<=65536
  &&Number.isSafeInteger(value.statementTimeoutMs)&&value.statementTimeoutMs>=100&&value.statementTimeoutMs<=5000);
 const r=value.recordDescriptor,c=value.continuationDescriptor,f=value.finalizationDescriptor;
 exact(r,['schemaVersion','protocol','policyId','policyVersion','writerId','outputOwnerId','outputContractId','maximumDecisions','maximumContextBytes','definitionId','definitionHash','runtimeBindingHash','assignmentHash']);
 check(r.schemaVersion===1&&Number.isSafeInteger(r.policyVersion)&&r.policyVersion>=1&&r.policyVersion<=1000000&&['policyId','writerId','outputOwnerId','outputContractId'].every(key=>identity(r[key]))&&Number.isSafeInteger(r.maximumDecisions)&&r.maximumDecisions>=2&&r.maximumDecisions<=32&&Number.isSafeInteger(r.maximumContextBytes)&&r.maximumContextBytes>=1024&&r.maximumContextBytes<=128000);
 check(r?.protocol==='composition-checkpoint-records.v1'&&identity(r.definitionId)&&digest(r.definitionHash)&&digest(r.assignmentHash)&&digest(r.runtimeBindingHash));
 const {recordDescriptorHash,...cp}=c??{};check(recordDescriptorHash===hash(r)&&hash(reservedChildHostContinuationDescriptor({recordDescriptor:r,policy:cp}))===hash(c));
 const fp=Object.fromEntries(['schemaVersion','mode','policyId','policyVersion','writerId','writerVersion','completionPolicyId','completionPolicyVersion','outputOwnerId','outputContractId','telemetryMode'].map(key=>[key,f?.[key]]));
 check(hash(reservedAnswerFinalizationDescriptor({recordDescriptor:r,continuationDescriptor:c,policy:fp}))===hash(f));return freeze(copy(value));
}
function contextSnapshot(value){
 exact(value,['actorId','sourceRunId','attemptId','definitionId','definitionHash','assignmentHash','partId','stage','threadId','sessionId','attemptGeneration','grantGeneration','authorityEpoch','owner']);
 for(const key of ['actorId','sourceRunId','attemptId','definitionId','partId','threadId','sessionId','attemptGeneration','grantGeneration','authorityEpoch'])check(sourceIdentity(value[key]));
 check(digest(value.definitionHash)&&digest(value.assignmentHash)&&(value.stage===null||sourceIdentity(value.stage)));
 const o=value.owner;exact(o,o?.kind==='authenticated'?['kind','principalId','accountId']:o?.kind==='public'?['kind','assistantSessionId']:['kind','principalId']);
 check(['authenticated','public','system'].includes(o.kind));for(const [key,v] of Object.entries(o))if(key!=='kind')check(v===null&&key==='accountId'||sourceIdentity(v));return freeze(copy(value));
}

// SQL projects only known bounded metadata. Equality rejects unknown keys at
// every level, without sending those values (or any output body) to the driver.
const genericStr={type:'string',max:2048};
const str={type:'string',max:200},hex={type:'string',max:64},num={type:'number'};
const boundedTextSql=(column,max=200)=>`CASE WHEN octet_length(${column})<=${max} THEN ${column} ELSE NULL END AS ${column}`;
const nullable=spec=>({...spec,nullable:true});
const parent={invocationId:str,partId:str,assignmentId:str,attemptId:str,threadId:str};
const claim={runId:str,workerId:genericStr,claimedAt:{type:'string',max:40}};
const ref={ownerId:str,contractId:str,referenceId:str,sha256:hex,byteLength:num};
const limits={maximumDecisions:num,maximumContextBytes:num,deadlineAtMs:num};
const reservation={reservationId:str,kind:str,partId:str,nextDecisionOrdinal:num,cursorHash:hex,status:str};
const cursor={parentInvocationId:str,observations:{list:{kind:str,invocationId:str,partId:str,outputReferenceHash:hex},max:1},completedActions:num,decisionsConsumed:num,contextBytes:num};
const child={invocationId:str,partId:str,threadId:str,taskHash:hex,stepId:genericStr,stage:nullable(genericStr),capabilityHash:hex};
const recordBase={kind:str,parent,child,cursor,limits,reservation:{type:'null'},schemaVersion:num,runId:str,sequence:num,descriptorHash:hex,requestId:str,requestHash:hex,previousRecordHash:nullable(hex),claim};
const acceptedRecord={...recordBase,child:{...child,retirementId:str,outputReference:ref},reservation};
const budgetScalar={type:'budget',nullable:true};
const runtimeSpec={schemaVersion:num,adapter:str,shape:str,definitionId:str,definitionHash:hex,assignmentHash:hex,rootPartId:str,policyHash:hex};
const binding={runIdentityHash:hex,inputHash:hex,authorizationHash:hex,assignmentIdentityHash:hex,attemptPromptHash:hex,attemptCapabilityHash:hex,
 requested:{fixed:[nullable(str),nullable(str),nullable(str)]},budget:{version:nullable(num),descendants:budgetScalar,attempts:budgetScalar,tokens:budgetScalar}};
const restoration={recordHash:hex,inputHash:hex,decisionsConsumed:num,contextBytes:num,maximumDecisions:num,maximumContextBytes:num,deadlineAtMs:num};
const acceptanceSpec={schemaVersion:num,runId:str,descriptorHash:hex,parent,binding,dispatchClaim:claim,acceptedRecordHash:hex,startedRecordHash:hex,
 reservation,decisionOrdinal:num,restoration,restoredInputBytes:num,outputReference:ref,outputValueHash:hex};
const completionSpec={schemaVersion:num,runId:str,descriptorHash:hex,acceptedAnswerHash:hex,resultHash:hex,attemptFinishHash:hex,assignmentFinishHash:hex,finalSummaryHash:hex};
function projectJson(base,spec){
 if(spec.type){
  if(spec.type==='null')return "'null'::jsonb";
  const allowed=spec.type==='budget'?`jsonb_typeof(${base}) IN ('string','number') AND octet_length(${base} #>> '{}')<=20`
   :spec.type==='string'?`jsonb_typeof(${base})='string' AND octet_length(${base} #>> '{}')<=${spec.max}`
    :spec.type==='number'?`jsonb_typeof(${base})='number' AND octet_length(${base} #>> '{}')<=20`:`jsonb_typeof(${base})='boolean'`;
  return `CASE WHEN ${allowed} THEN ${base} ELSE 'null'::jsonb END`;
 }
 if(spec.fixed)return `jsonb_build_array(${spec.fixed.map((item,i)=>projectJson(`(${base}->${i})`,item)).join(',')})`;
 if(spec.list)return `CASE WHEN ${base}='[]'::jsonb THEN '[]'::jsonb ELSE jsonb_build_array(${projectJson(`(${base}->0)`,spec.list)}) END`;
 return `jsonb_build_object(${Object.entries(spec).flatMap(([key,item])=>["'"+key+"'",projectJson(`(${base}->'${key}')`,item)]).join(',')})`;
}
function boundedRows(rows,p){check(Array.isArray(rows)&&Buffer.byteLength(JSON.stringify(rows))<=p.maximumSnapshotBytes);return rows;}
function matchesContext(source,context){for(const key of ['attemptId','definitionId','definitionHash','assignmentHash','partId','threadId','sessionId','attemptGeneration','grantGeneration'])if(Object.hasOwn(context??{},key))check(context[key]===source[key]);}
function validRecord(v){
 for(const o of [v.parent,v.child,v.claim])for(const [k,x] of Object.entries(o))if(!['outputReference','stage','claimedAt','taskHash','capabilityHash','stepId','workerId'].includes(k))check(identity(x));
 check(sourceIdentity(v.claim.workerId)&&sourceIdentity(v.child.stepId)&&digest(v.child.taskHash)&&digest(v.child.capabilityHash)&&(v.child.stage===null||sourceIdentity(v.child.stage))&&identity(v.requestId)&&digest(v.requestHash));
 for(const x of [v.cursor.completedActions,v.cursor.decisionsConsumed,v.cursor.contextBytes,v.limits.maximumDecisions,v.limits.maximumContextBytes,v.limits.deadlineAtMs])check(Number.isSafeInteger(x)&&x>=0);
 check(v.cursor.parentInvocationId===v.parent.invocationId&&v.limits.maximumDecisions>0&&v.limits.maximumContextBytes>0&&v.limits.deadlineAtMs>0&&v.cursor.decisionsConsumed<=v.limits.maximumDecisions);
 if(v.reservation)check(identity(v.reservation.reservationId)&&Number.isSafeInteger(v.reservation.nextDecisionOrdinal)&&v.reservation.nextDecisionOrdinal>0);
 if(v.child.outputReference){const o=v.child.outputReference;check(identity(o.ownerId)&&identity(o.contractId)&&identity(o.referenceId)&&digest(o.sha256)&&Number.isSafeInteger(o.byteLength)&&o.byteLength>0&&o.byteLength<=v.limits.maximumContextBytes);}
}


/** One exact composition/run, independently authorized from the caller's host
 * context. Target claims and owner-output authority are deliberately absent.
 */
export function createCheckpointStatusReader({database,relations,policy,getPolicy,resolveContext,authorizeRead}={}){
 for(const fn of [database?.transaction,getPolicy,resolveContext,authorizeRead])check(typeof fn==='function');
 const sql=resolvePostgresRelations(relations,['runs','versions','assignments','attempts','heads','records','acceptances','completions'],'Checkpoint status');
 const p=normalizePolicy(policy),descriptorHash=hash(p),r=p.recordDescriptor,c=p.continuationDescriptor,f=p.finalizationDescriptor;
 const current=()=>check(hash(normalizePolicy(getPolicy()))===descriptorHash);
 async function inspect(input,{context,signal=context?.signal}={}){
  try{
   input=copy(input);exact(input,['runId']);check(identity(input.runId));const runId=input.runId;
   current();signal?.throwIfAborted();let source=contextSnapshot(await resolveContext(context));matchesContext(source,context);current();signal?.throwIfAborted();
   async function fresh(){current();signal?.throwIfAborted();const next=contextSnapshot(await resolveContext(context));matchesContext(next,context);check(hash(next)===hash(source));current();signal?.throwIfAborted();}
   async function permit(phase){await fresh();check(await authorizeRead(freeze({phase,runId,inspectionDescriptorHash:descriptorHash,targetDefinitionId:r.definitionId,targetDefinitionHash:r.definitionHash,source:copy(source)}),context)===true);await fresh();}
   await permit('read');
   const projected=await database.transaction(async rawClient=>{
    let authorityFailure=false;
    const guardedFresh=async()=>{try{await fresh();}catch{authorityFailure=true;throw refuse();}};
    const client={async query(statement,parameters){await guardedFresh();const result=await rawClient.query(statement,parameters);await guardedFresh();return result;}};
    await client.query('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY');
    await client.query(`SET LOCAL statement_timeout = '${p.statementTimeoutMs}ms'`);
    await fresh();
    const authorizationProjection=`jsonb_build_object('assignedRuntime',${projectJson("run.authorization_snapshot->'assignedRuntime'",runtimeSpec)},'compositionCheckpointProtocol',$6::jsonb,'reservedChildHostContinuation',$7::jsonb,'reservedAnswerFinalization',$8::jsonb)`;
    const o=source.owner,owner=[o.kind,o.principalId??null,o.accountId??null,o.assistantSessionId??null];
    const run=(await client.query(`SELECT run.id,run.workflow_version_id,version.definition_sha256,run.status,run.identity_sha256,run.input_sha256,
     run.run_family_budget_version,run.run_family_max_descendant_runs,run.run_family_max_total_attempts,run.run_family_max_total_tokens,
     (run.resumed_from_run_id IS NULL) AS root_run,(run.claimed_by IS NULL AND run.claimed_at IS NULL AND run.lease_expires_at IS NULL) AS claim_cleared,
     (run.completed_at IS NOT NULL AND run.admitted_manifest_sha256 IS NULL) AS completion_marked,
     (run.authorization_snapshot->'compositionCheckpointProtocol'=$6::jsonb AND run.authorization_snapshot->'reservedChildHostContinuation'=$7::jsonb
       AND run.authorization_snapshot->'reservedAnswerFinalization'=$8::jsonb) AS opted_in,
     ${authorizationProjection} AS authorization_metadata,(run.authorization_snapshot=${authorizationProjection}) AS exact_authorization,
     transaction_timestamp() AS snapshot_at
     FROM ${sql.runs} run JOIN ${sql.versions} version ON version.id=run.workflow_version_id
     WHERE run.id=$1 AND run.owner_kind=$2 AND run.owner_principal_id IS NOT DISTINCT FROM $3
       AND run.owner_account_id IS NOT DISTINCT FROM $4 AND run.owner_assistant_session_id IS NOT DISTINCT FROM $5
       AND run.workflow_version_id=$9 AND version.definition_sha256=$10 LIMIT 1`,
     [runId,...owner,JSON.stringify(r),JSON.stringify(c),JSON.stringify(f),r.definitionId,r.definitionHash])).rows[0];
    await fresh();boundedRows(run?[run]:[],p);check(['queued','running','completed','failed','cancelled','blocked'].includes(run?.status));check(run&&run.id===runId&&run.workflow_version_id===r.definitionId&&run.definition_sha256===r.definitionHash);
    const base={schemaVersion:1,kind:'checkpoint-status.v1',runId,definitionId:r.definitionId,definitionHash:r.definitionHash,
      inspectionDescriptorHash:descriptorHash,snapshotAt:new Date(run.snapshot_at).toISOString(),recordedRunStatus:run.status,
      state:'unavailable',
      telemetry:'unknown',executionAuthority:false,outputBytesVerified:false};
    if(!run.opted_in||!run.exact_authorization||hash(run.authorization_metadata.assignedRuntime)!==r.runtimeBindingHash||!run.root_run||run.run_family_budget_version===3)return base;
    // No table scan/fanout. Every relation is bounded and scoped to this run.
    const assignments=boundedRows((await client.query(`SELECT ${boundedTextSql('id')},run_id,${boundedTextSql('parent_assignment_id')},${boundedTextSql('step_key',2048)},execution,status,${boundedTextSql('agent_thread_id')},identity_sha256,finish_sha256,(failure IS NULL) AS no_failure,
      (completed_at IS NOT NULL AND finished_at IS NOT NULL) AS completion_marked,
      (SELECT count(*)=1 FROM ${sql.assignments} WHERE run_id=$1) AS sole_root FROM ${sql.assignments} WHERE run_id=$1 AND parent_assignment_id IS NULL AND step_key=$2 ORDER BY assignment_ordinal LIMIT 2`,[runId,p.rootStepId])).rows,p);
    const attempts=boundedRows((await client.query(`SELECT ${boundedTextSql('id')},run_id,${boundedTextSql('assignment_id')},attempt_ordinal,${boundedTextSql('thread_id')},prompt_sha256,capability_sha256,
      CASE WHEN octet_length(requested_model)<=200 THEN requested_model ELSE NULL END AS requested_model,CASE WHEN octet_length(requested_reasoning_effort)<=200 THEN requested_reasoning_effort ELSE NULL END AS requested_reasoning_effort,CASE WHEN octet_length(requested_service_tier)<=200 THEN requested_service_tier ELSE NULL END AS requested_service_tier,outcome,result_sha256,finish_sha256,telemetry_status,
      CASE WHEN jsonb_typeof(capability_profile->'deadlineAtMs')='number' AND octet_length(capability_profile->>'deadlineAtMs')<=20 THEN capability_profile->>'deadlineAtMs' ELSE NULL END AS deadline_ms,(completed_at IS NOT NULL) AS completion_marked,
      (resource_reserved_total_tokens IS NULL) AS unreserved,
      (observed_provider IS NULL AND observed_model IS NULL AND observed_reasoning_effort IS NULL AND observed_service_tier IS NULL
       AND input_tokens IS NULL AND cached_input_tokens IS NULL AND cache_write_input_tokens IS NULL AND output_tokens IS NULL
       AND reasoning_output_tokens IS NULL AND total_tokens IS NULL AND price_version_id IS NULL AND api_equivalent_cost IS NULL
       AND api_equivalent_cost_currency IS NULL AND billed_cost IS NULL AND billed_cost_currency IS NULL AND error IS NULL) AS unknown_telemetry
       ,(SELECT count(*)=1 FROM ${sql.attempts} WHERE run_id=$1) AS sole_attempt
      FROM ${sql.attempts} WHERE run_id=$1 AND assignment_id=$2 ORDER BY attempt_ordinal LIMIT 2`,[runId,assignments[0]?.id??null])).rows,p);
    const heads=boundedRows((await client.query(`SELECT run_id,sequence,record_sha256 FROM ${sql.heads} WHERE run_id=$1 LIMIT 2`,[runId])).rows,p);
    const recordProjection=`CASE WHEN sequence=1 THEN ${projectJson('payload',recordBase)} ELSE ${projectJson('payload',acceptedRecord)} END`;
    const records=boundedRows((await client.query(`SELECT run_id,sequence,record_sha256,${recordProjection} AS metadata,(payload=${recordProjection}) AS exact_metadata
      FROM ${sql.records} WHERE run_id=$1 ORDER BY sequence LIMIT 4`,[runId])).rows,p);
    const receiptRows=async(key,spec)=>{const proj=projectJson('payload',spec);return boundedRows((await client.query(`SELECT run_id,receipt_sha256,${key==='completions'?'accepted_sha256,':''}
      ${proj} AS metadata,(payload=${proj}) AS exact_metadata FROM ${sql[key]} WHERE run_id=$1 LIMIT 2`,[runId])).rows,p);};
    const acceptances=await receiptRows('acceptances',acceptanceSpec),completions=await receiptRows('completions',completionSpec);
    await fresh();
    try{
     boundedRows([run,assignments,attempts,heads,records,acceptances,completions],p);
     if(run.status==='queued'&&assignments.length===0&&attempts.length===0&&heads.length===0&&records.length===0&&acceptances.length===0&&completions.length===0)return {...base,state:'pending'};
     check(assignments.length===1&&attempts.length===1&&heads.length<=1&&records.length<=3&&acceptances.length<=1&&completions.length<=1);
     const a=assignments[0],x=attempts[0];check(identity(a.id)&&identity(x.id)&&identity(a.agent_thread_id)&&a.sole_root===true&&x.sole_attempt===true&&a.run_id===runId&&x.run_id===runId&&a.parent_assignment_id===null&&a.step_key===p.rootStepId&&a.execution==='current-agent'
      &&x.assignment_id===a.id&&x.attempt_ordinal===1&&x.thread_id===a.agent_thread_id&&x.unreserved);
     check(records.length===0?heads.length===0:heads.length===1&&heads[0].sequence===records.length&&heads[0].run_id===runId);
     let previous=null,last;
     for(let i=0;i<records.length;i++){
      const row=records[i],v=row.metadata;validRecord(v);check(row.exact_metadata===true&&row.run_id===runId&&row.sequence===i+1&&row.record_sha256===hash(v)
       &&v.schemaVersion===1&&v.runId===runId&&v.sequence===i+1&&v.descriptorHash===hash(r)&&v.previousRecordHash===previous
       &&v.kind===['child-started','child-accepted-cursor-reserved','successor-started'][i]&&v.claim.runId===runId
       &&v.parent.partId===p.rootPartId&&v.parent.assignmentId===a.id&&v.parent.attemptId===x.id&&v.parent.threadId===x.thread_id
       &&v.limits.maximumDecisions===r.maximumDecisions&&v.limits.maximumContextBytes===r.maximumContextBytes&&String(v.limits.deadlineAtMs)===x.deadline_ms);
      if(i===0)check(v.reservation===null&&v.cursor.decisionsConsumed===0&&v.cursor.contextBytes===0&&v.cursor.completedActions===0&&v.cursor.observations.length===0);
      if(i>0){check(hash(v.parent)===hash(last.parent)&&hash(v.limits)===hash(last.limits)&&v.cursor.parentInvocationId===v.parent.invocationId
        &&v.cursor.observations.length===1&&hash(v.cursor.observations[0])===hash({kind:'child',invocationId:v.child.invocationId,partId:v.child.partId,outputReferenceHash:hash(v.child.outputReference)})
        &&v.cursor.completedActions===1&&v.reservation.kind==='provider-decision'&&v.reservation.partId===p.rootPartId&&v.reservation.cursorHash===hash(v.cursor)
        &&v.reservation.nextDecisionOrdinal===v.cursor.decisionsConsumed+1&&v.reservation.nextDecisionOrdinal<=r.maximumDecisions
        &&v.reservation.status===(i===1?'reserved':'started')&&v.cursor.contextBytes>=v.child.outputReference.byteLength&&v.cursor.contextBytes<=r.maximumContextBytes);
        if(i===1){const {retirementId,outputReference,...childIdentity}=v.child;check(hash(childIdentity)===hash(last.child));}
        else check(hash({...v,kind:last.kind,reservation:{...v.reservation,status:'reserved'},sequence:last.sequence,requestId:last.requestId,requestHash:last.requestHash,previousRecordHash:last.previousRecordHash,claim:last.claim})===hash(last));
      }
      previous=row.record_sha256;last=v;
     }
     if(last)check(heads[0].record_sha256===previous);
     const acceptance=acceptances[0],completion=completions[0];
     const root={partId:p.rootPartId,assignmentId:a.id,attemptId:x.id};
     const checkpoint=last?{sequence:records.length,recordHash:previous}:null;
     const counters=last?{decisionsConsumed:last.cursor.decisionsConsumed,contextBytes:last.cursor.contextBytes,maximumDecisions:last.limits.maximumDecisions,
       maximumContextBytes:last.limits.maximumContextBytes,deadlineAtMs:last.limits.deadlineAtMs,nextDecisionOrdinal:last.reservation?.nextDecisionOrdinal??null}:null;
     let state=['pending','child-started-uncertain','successor-reserved','successor-started-uncertain'][records.length],acceptedAnswerHash=null,completionHash=null;
     if(acceptance){
      const v=acceptance.metadata;check(records.length===3&&acceptance.run_id===runId&&acceptance.exact_metadata===true&&acceptance.receipt_sha256===hash(v)
       &&v.schemaVersion===1&&v.runId===runId&&v.descriptorHash===hash(f)&&v.acceptedRecordHash===records[1].record_sha256&&v.startedRecordHash===previous
       &&hash(v.parent)===hash(last.parent)&&hash(v.dispatchClaim)===hash(last.claim)&&hash(v.reservation)===hash(last.reservation)&&v.decisionOrdinal===last.reservation.nextDecisionOrdinal);
      const b=v.binding,t=v.restoration;check(b.runIdentityHash===run.identity_sha256&&b.inputHash===run.input_sha256&&b.authorizationHash===hash(run.authorization_metadata)&&b.assignmentIdentityHash===a.identity_sha256
       &&b.attemptPromptHash===x.prompt_sha256&&b.attemptCapabilityHash===x.capability_sha256
       &&hash(b.requested)===hash([x.requested_model,x.requested_reasoning_effort,x.requested_service_tier])
       &&hash(b.budget)===hash({version:run.run_family_budget_version,descendants:run.run_family_max_descendant_runs,attempts:run.run_family_max_total_attempts,tokens:run.run_family_max_total_tokens})
       &&t.recordHash===v.acceptedRecordHash&&t.decisionsConsumed===last.cursor.decisionsConsumed&&t.contextBytes===last.cursor.contextBytes
       &&t.maximumDecisions===last.limits.maximumDecisions&&t.maximumContextBytes===last.limits.maximumContextBytes&&t.deadlineAtMs===last.limits.deadlineAtMs
       &&digest(t.inputHash)&&Number.isSafeInteger(v.restoredInputBytes)&&v.restoredInputBytes>0&&v.restoredInputBytes<=t.maximumContextBytes&&digest(v.outputValueHash)
       &&v.outputReference.ownerId===f.outputOwnerId&&v.outputReference.contractId===f.outputContractId&&identity(v.outputReference.referenceId)&&digest(v.outputReference.sha256)
       &&Number.isSafeInteger(v.outputReference.byteLength)&&v.outputReference.byteLength>0&&v.outputReference.byteLength<=32000);
      state='retired-answer-accepted';acceptedAnswerHash=acceptance.receipt_sha256;
     }
     if(completion){
      const v=completion.metadata,A=acceptance?.metadata;check(A&&completion.run_id===runId&&completion.exact_metadata===true&&completion.receipt_sha256===hash(v)
       &&completion.accepted_sha256===acceptedAnswerHash&&v.schemaVersion===1&&v.runId===runId&&v.descriptorHash===hash(f)&&v.acceptedAnswerHash===acceptedAnswerHash
       &&run.status==='completed'&&run.claim_cleared&&run.completion_marked&&a.status==='completed'&&a.completion_marked&&a.no_failure===true
       &&x.outcome==='completed'&&x.completion_marked&&x.telemetry_status==='incomplete'&&x.unknown_telemetry
       &&digest(v.resultHash)&&v.resultHash===x.result_sha256&&digest(v.attemptFinishHash)&&v.attemptFinishHash===x.finish_sha256
       &&v.assignmentFinishHash===hash({status:'completed',failure:null})&&v.assignmentFinishHash===a.finish_sha256);
      const metadata={mode:f.mode,descriptorHash:hash(f),acceptedAnswerHash,parent:A.parent,acceptedRecordHash:A.acceptedRecordHash,startedRecordHash:A.startedRecordHash,
       reservation:A.reservation,decisionOrdinal:A.decisionOrdinal,restoration:A.restoration,restoredInputBytes:A.restoredInputBytes,outputReference:A.outputReference,outputValueHash:A.outputValueHash};
      const summary={checkpointFinalization:metadata,telemetryStatus:'incomplete',usage:null,apiEquivalentCost:null,billedCost:null};check(v.finalSummaryHash===hash(summary));
      // Only booleans cross this query. No output body or arbitrary summary JSON.
      const verified=(await client.query(`SELECT (final_summary=$2::jsonb) AS summary_matches FROM ${sql.runs} WHERE id=$1 LIMIT 1`,[runId,JSON.stringify(summary)])).rows[0];
      const verifiedAttempt=(await client.query(`SELECT (result_payload->'checkpointFinalization'=$2::jsonb AND result_payload->'telemetryIssues'=$3::jsonb
       AND jsonb_typeof(result_payload->'output')='object' AND (SELECT count(*) FROM jsonb_object_keys(result_payload))=3) AS metadata_matches
       FROM ${sql.attempts} WHERE id=$1 AND run_id=$4 LIMIT 1`,[x.id,JSON.stringify(metadata),JSON.stringify(['provider-usage-unobserved','reviewed-price-unobserved','costs-unobserved']),runId])).rows[0];
      check(verified?.summary_matches===true&&verifiedAttempt?.metadata_matches===true);state='workflow-finalized';completionHash=completion.receipt_sha256;
     }else check(run.status==='running'&&a.status==='running'&&x.outcome===null);
     if(counters?.nextDecisionOrdinal===null)delete counters.nextDecisionOrdinal;return {...base,state,root,...(checkpoint?{checkpoint}:{}),...(counters?{counters}:{}),...(acceptedAnswerHash?{acceptedAnswerHash}:{}),...(completionHash?{completionHash}:{})};
    }catch{if(authorityFailure)throw refuse();return base;}
   });
   await permit('disclose');current();signal?.throwIfAborted();check(Buffer.byteLength(JSON.stringify(projected))<=p.maximumResultBytes);return freeze(projected);
  }catch{throw refuse();}
 }
 current();return Object.freeze({descriptorForTool:()=>copy(p),inspect});
}

/** Existing assignment adapter supplies context; model arguments supply no authority. */
export async function createAssignedCheckpointStatusTool({reader,adapter,toolId='composition.checkpoint-status'}={}){
 check(typeof reader?.inspect==='function'&&typeof reader?.descriptorForTool==='function');const p=normalizePolicy(reader.descriptorForTool());
 const implementationDigest=createHash('sha256').update(await readFile(new URL(import.meta.url))).update(hash(p)).digest('hex');
 const registration=createToolRegistration({schemaVersion:1,toolId,schemaDialect:'value-schema.v1',implementation:{id:'bodyless-checkpoint-status',version:'1',digest:implementationDigest},adapter,
  inputSchema:{type:'object',description:'Inspect one explicitly authorized run of the host-pinned composition. Recorded progress only; no output bodies, recovery permission or replay.',properties:{runId:{type:'string',minLength:1,maxLength:200}},required:['runId'],additionalProperties:false},
  resultSchema:{type:'object',properties:{schemaVersion:{type:'integer',const:1},kind:{type:'string',const:'checkpoint-status.v1'},runId:{type:'string',maxLength:200},definitionId:{type:'string',maxLength:200},definitionHash:{type:'string',maxLength:64},inspectionDescriptorHash:{type:'string',maxLength:64},snapshotAt:{type:'string',maxLength:40},recordedRunStatus:{type:'string',maxLength:40},
   state:{type:'string',enum:['pending','child-started-uncertain','successor-reserved','successor-started-uncertain','retired-answer-accepted','workflow-finalized','unavailable']},
   root:{type:'object',properties:{partId:{type:'string',maxLength:200},assignmentId:{type:'string',maxLength:200},attemptId:{type:'string',maxLength:200}},required:['partId','assignmentId','attemptId'],additionalProperties:false},checkpoint:{type:'object',properties:{sequence:{type:'integer',minimum:1,maximum:3},recordHash:{type:'string',maxLength:64}},required:['sequence','recordHash'],additionalProperties:false},counters:{type:'object',properties:Object.fromEntries(['decisionsConsumed','contextBytes','maximumDecisions','maximumContextBytes','deadlineAtMs','nextDecisionOrdinal'].map(key=>[key,{type:'integer',minimum:0,maximum:Number.MAX_SAFE_INTEGER}])),required:['decisionsConsumed','contextBytes','maximumDecisions','maximumContextBytes','deadlineAtMs'],additionalProperties:false},acceptedAnswerHash:{type:'string',maxLength:64},completionHash:{type:'string',maxLength:64},telemetry:{type:'string',const:'unknown'},executionAuthority:{type:'boolean',const:false},outputBytesVerified:{type:'boolean',const:false}},
   required:['schemaVersion','kind','runId','definitionId','definitionHash','inspectionDescriptorHash','snapshotAt','recordedRunStatus','state','telemetry','executionAuthority','outputBytesVerified'],additionalProperties:false}});
 const pending=new Map();
 return Object.freeze({registration,maxInputBytes:1024,maxResultBytes:p.maximumResultBytes,
  async handler({arguments:input,context}){
   try{check(context?.phase==='invoke'&&context.toolId===toolId&&context.bindingId===registration.bindingId&&sourceIdentity(context.partId));
    const work=reader.inspect(input,{context,signal:context.signal});const calls=pending.get(context)??new Set();pending.set(context,calls);calls.add(work);
    try{return await work;}finally{calls.delete(work);if(!calls.size)pending.delete(context);}
   }catch{throw refuse();}
  },async drain(context){await Promise.allSettled([...(pending.get(context)??[])]);}});
}
