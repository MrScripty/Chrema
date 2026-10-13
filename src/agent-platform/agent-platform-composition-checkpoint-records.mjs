import {createHash} from 'node:crypto';
import {resolvePostgresRelations} from '../shared/postgres-relations.mjs';
import {normalizeExpectedClaim, assertPostgresExpectedClaim,claimLostError} from './agent-platform-claim-bound-journal.mjs';
import {workflowDefinitionHash, renderPrompt} from './agent-platform-prompt.mjs';
import {resolveWorkflowToolAssignment} from './agent-platform-definition-tool-assignment.mjs';

const protocol='composition-checkpoint-records.v1';
const keys=['runs','versions','assignments','attempts','events','heads','records'];
const schemaUrl=new URL('../../schemas/agent-platform/composition-checkpoint-records-v1.sql',import.meta.url);
export function getCompositionCheckpointRecordsSchemaUrl(){return new URL(schemaUrl);}
export function compositionCheckpointRecordHash(value) {
  const stable=v=>Array.isArray(v)?`[${v.map(stable).join(',')}]`:v&&typeof v==='object'
    ?`{${Object.keys(v).sort().map(k=>`${JSON.stringify(k)}:${stable(v[k])}`).join(',')}}`:JSON.stringify(v);
  return createHash('sha256').update(stable(value)).digest('hex');
}
const hash=compositionCheckpointRecordHash;
const copy=v=>structuredClone(v);
function refuse(code='CHECKPOINT_RECORDS_CONFLICT') {return Object.assign(new Error('Composition checkpoint records are unavailable for this transition.'),{code,statusCode:409});}
function check(test){if(!test)throw refuse();}
function exact(v,fields){check(v&&Object.getPrototypeOf(v)===Object.prototype&&Object.keys(v).sort().join('|')===[...fields].sort().join('|'));}
function id(v){check(typeof v==='string'&&/^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$/.test(v));return v;}
function digest(v){check(typeof v==='string'&&/^[a-f0-9]{64}$/.test(v));return v;}
function integer(v,min,max){check(Number.isSafeInteger(v)&&v>=min&&v<=max);return v;}
function freeze(v){if(v&&typeof v==='object'){Object.values(v).forEach(freeze);Object.freeze(v);}return v;}

/** PostgreSQL-only, host-attested records. No executor, output loader or grant. */
export function createCompositionCheckpointRecords({database,relations,definition,parts,registrations,hostTools,runtimeBinding,
  policy,getPolicy,authorize,outputReferences,attestRetirement}={}) {
  check(typeof database?.transaction==='function');
  for(const fn of [getPolicy,authorize,outputReferences?.verify,attestRetirement])check(typeof fn==='function');
  const sql=resolvePostgresRelations(relations,keys,'Composition checkpoint records');
  const approved=copy({definition,parts,registrations,hostTools,runtimeBinding});
  check(approved.definition.definitionHash===workflowDefinitionHash(approved.definition));
  const root=approved.parts.find(p=>p.parentPartId===null);
  check(root&&approved.parts.filter(p=>p.parentPartId===null).length===1);
  const resolve=partId=>resolveWorkflowToolAssignment({...approved,partId});
  const rootResolved=resolve(root.partId);
  check(runtimeBinding.definitionId===definition.id&&runtimeBinding.definitionHash===definition.definitionHash
    &&runtimeBinding.assignmentHash===rootResolved.assignment.assignmentHash);
  function selected(partId){
    const part=approved.parts.find(p=>p.partId===partId);check(part);
    const selected=resolve(partId),step=copy(approved.definition.steps.find(s=>s.id===part.stepId));check(step);
    check(step.prompt.variables.length===0&&step.executionProfile===undefined);
    if(part.stage!==null){check(Object.hasOwn(step.prompt.stageTemplates??{},part.stage));step.prompt.template=step.prompt.stageTemplates[part.stage];
      if(Object.hasOwn(step.prompt.stageSettings??{},part.stage))Object.assign(step.prompt,step.prompt.stageSettings[part.stage]);}
    const prompt=renderPrompt(step,{}, {toolAssignment:selected.selection});
    check(prompt.workspaceAccess==='read-only'&&prompt.networkAccess===false);
    return{part,selected,prompt,capabilityHash:hash({selection:selected.selection,assignment:selected.assignment,promptHash:hash(prompt.text),settings:{model:prompt.model,reasoningEffort:prompt.reasoningEffort,serviceTier:prompt.serviceTier}})};
  }
  const rootSelection=selected(root.partId);
  function normalizePolicy(value){
    exact(value,['schemaVersion','protocol','policyId','policyVersion','writerId','outputOwnerId','outputContractId','maximumDecisions','maximumContextBytes']);
    check(value.schemaVersion===1&&value.protocol===protocol);['policyId','writerId','outputOwnerId','outputContractId'].forEach(k=>id(value[k]));
    integer(value.policyVersion,1,1000000);integer(value.maximumDecisions,2,32);integer(value.maximumContextBytes,1024,128000);return copy(value);
  }
  const pinned=normalizePolicy(policy);
  check(outputReferences.ownerId===pinned.outputOwnerId&&outputReferences.contractId===pinned.outputContractId);
  const descriptor=freeze({...pinned,definitionId:approved.definition.id,definitionHash:approved.definition.definitionHash,
    runtimeBindingHash:hash(approved.runtimeBinding),assignmentHash:rootResolved.assignment.assignmentHash});
  const descriptorHash=hash(descriptor);
  const current=()=>check(hash(normalizePolicy(getPolicy()))===hash(pinned));
  function normalize(input,operation){
    const request=copy(input);
    const fields=operation==='beginChild'?['runId','expectedClaim','requestId','previousRecordHash','parent','child']
      :operation==='acceptChildAndReserve'?['runId','expectedClaim','requestId','previousRecordHash','outputReference','retirementId','decisionsConsumed','contextBytes','reservationId']
        :operation==='markSuccessorStarted'?['runId','expectedClaim','requestId','previousRecordHash','reservationId']:['runId','expectedClaim'];
    exact(request,fields);id(request.runId);
    check(request.expectedClaim!==null&&request.expectedClaim!==undefined);
    request.expectedClaim=normalizeExpectedClaim(request.expectedClaim);check(request.expectedClaim.runId===request.runId);
    if(operation!=='inspect'){id(request.requestId);if(operation==='beginChild')check(request.previousRecordHash===null);else digest(request.previousRecordHash);}
    if(operation==='beginChild'){
      exact(request.parent,['invocationId','partId','assignmentId','attemptId','threadId']);Object.values(request.parent).forEach(id);
      check(request.parent.partId===root.partId);
      exact(request.child,['invocationId','partId','threadId','taskHash']);['invocationId','partId','threadId'].forEach(k=>id(request.child[k]));digest(request.child.taskHash);
      check(request.child.invocationId!==request.parent.invocationId&&request.child.threadId!==request.parent.threadId);
      const child=selected(request.child.partId);check(child.part.parentPartId===root.partId);
    }
    if(operation==='acceptChildAndReserve'){
      exact(request.outputReference,['ownerId','contractId','referenceId','sha256','byteLength']);
      const ref=request.outputReference;check(ref.ownerId===pinned.outputOwnerId&&ref.contractId===pinned.outputContractId);id(ref.referenceId);digest(ref.sha256);
      integer(ref.byteLength,1,pinned.maximumContextBytes);id(request.retirementId);id(request.reservationId);
      integer(request.decisionsConsumed,1,pinned.maximumDecisions-1);integer(request.contextBytes,ref.byteLength,pinned.maximumContextBytes);
    }
    if(operation==='markSuccessorStarted')id(request.reservationId);
    return freeze(request);
  }
  async function guarded(callback,target){
    try{check(await callback(freeze(copy(target)))===true);}catch{throw refuse('CHECKPOINT_RECORDS_FORBIDDEN');}
    current();
  }
  async function load(client,runId){
    const head=(await client.query(`SELECT * FROM ${sql.heads} WHERE run_id=$1 FOR UPDATE`,[runId])).rows[0];
    const rows=(await client.query(`SELECT * FROM ${sql.records} WHERE run_id=$1 ORDER BY sequence`,[runId])).rows;
    check((!head&&rows.length===0)||(head&&head.sequence===rows.length&&rows.length>=1&&rows.length<=3));
    let previous=null;
    for(let index=0;index<rows.length;index++){
      const r=rows[index];check(r.sequence===index+1&&r.record_sha256===hash(r.payload)&&r.payload.previousRecordHash===previous
        &&r.payload.descriptorHash===descriptorHash&&r.payload.sequence===r.sequence&&r.payload.runId===runId);
      check(r.payload.kind===['child-started','child-accepted-cursor-reserved','successor-started'][index]);previous=r.record_sha256;
    }
    if(head)check(head.record_sha256===previous);
    return{head,rows,last:rows.at(-1)?.payload};
  }
  async function lineage(client,request,parent){
    const run=(await client.query(`SELECT * FROM ${sql.runs} WHERE id=$1 FOR UPDATE`,[request.runId])).rows[0];
    check(run&&run.status==='running'&&run.resumed_from_run_id===null&&run.workflow_version_id===approved.definition.id
      &&run.workflow_type===approved.definition.workflowType&&run.authorization_snapshot.compositionCheckpointProtocol
      &&hash(run.authorization_snapshot.compositionCheckpointProtocol)===descriptorHash
      &&hash(run.authorization_snapshot.assignedRuntime)===hash(approved.runtimeBinding));
    const version=(await client.query(`SELECT definition,definition_sha256 FROM ${sql.versions} WHERE id=$1`,[run.workflow_version_id])).rows[0];
    check(version&&version.definition_sha256===approved.definition.definitionHash&&workflowDefinitionHash(version.definition)===version.definition_sha256);
    await assertPostgresExpectedClaim(client,sql,request.runId,request.expectedClaim);
    if(!parent)return run;
    const a=(await client.query(`SELECT * FROM ${sql.assignments} WHERE id=$1`,[parent.assignmentId])).rows[0];
    const x=(await client.query(`SELECT * FROM ${sql.attempts} WHERE id=$1`,[parent.attemptId])).rows[0];
    check(a&&x&&a.run_id===request.runId&&x.run_id===request.runId&&x.assignment_id===a.id&&a.parent_assignment_id===null
      &&a.step_key===root.stepId&&a.execution==='current-agent'&&a.status==='running'&&x.outcome===null&&x.attempt_ordinal===1
      &&x.thread_id===parent.threadId&&a.agent_thread_id===parent.threadId&&x.prompt_sha256===hash(rootSelection.prompt.text)
      &&hash(x.capability_profile.selectedToolPart)===hash(rootResolved.selection)
      &&hash(x.capability_profile.toolAssignment)===hash(rootResolved.assignment)
      &&x.requested_model===rootSelection.prompt.model&&x.requested_reasoning_effort===rootSelection.prompt.reasoningEffort
      &&x.requested_service_tier===rootSelection.prompt.serviceTier
      &&x.capability_profile.promptVersionId===rootSelection.prompt.promptVersionId
      &&x.capability_profile.workspaceAccess==='read-only'&&x.capability_profile.networkAccess===false);
    const counts=(await client.query(`SELECT (SELECT count(*) FROM ${sql.assignments} WHERE run_id=$1)::int AS assignments,(SELECT count(*) FROM ${sql.attempts} WHERE run_id=$1)::int AS attempts`,[request.runId])).rows[0];
    check(counts.assignments===1&&counts.attempts===1);
    const deadline=x.capability_profile.deadlineAtMs;integer(deadline,1,Number.MAX_SAFE_INTEGER);
    await custody(client,request,deadline);
    return{run,deadline};
  }
  async function custody(client,request,deadline){
    current();await assertPostgresExpectedClaim(client,sql,request.runId,request.expectedClaim);
    const state=(await client.query(`SELECT clock_timestamp() AS checked_at,EXISTS(SELECT 1 FROM ${sql.events} WHERE run_id=$1 AND source_key='workflow.cancellation-requested.v1') AS cancelled`,[request.runId])).rows[0];
    // PostgreSQL drivers return Date objects. Date.parse(Date) stringifies
    // them without milliseconds and can incorrectly extend a short deadline.
    const checkedAt=state.checked_at instanceof Date?state.checked_at.getTime():Date.parse(state.checked_at);
    check(!state.cancelled&&Number.isFinite(checkedAt)&&checkedAt<deadline);
    current(); // Fence policy withdrawal during either database await above.
  }
  async function transition(input,operation){
    const request=normalize(input,operation);current();
    await guarded(authorize,{operation,request,descriptor});
    if(operation==='acceptChildAndReserve'){
      // These are trusted host verifications only. No body is loaded or copied.
      const prior=await database.transaction(async client=>{
        await lineage(client,request);const history=await load(client,request.runId);check(history.last);return copy(history.rows[0].payload);
      });
      check(prior.kind==='child-started');
      await guarded(outputReferences.verify,{runId:request.runId,descriptor,parent:prior.parent,child:prior.child,outputReference:request.outputReference});
      await guarded(attestRetirement,{runId:request.runId,descriptor,parent:prior.parent,child:prior.child,originalClaim:prior.claim,retirementId:request.retirementId,outputReference:request.outputReference});
    }
    return database.transaction(async client=>{
      await lineage(client,request);
      const history=await load(client,request.runId);
      const parent=operation==='beginChild'?request.parent:history.last?.parent;check(parent);
      const {deadline}=await lineage(client,request,parent);
      const requestIdentity=copy(request);delete requestIdentity.expectedClaim;
      const requestHash=hash({operation,request:requestIdentity});
      const existing=history.rows.find(r=>r.payload.requestId===request.requestId);
      if(existing){check(existing.payload.requestHash===requestHash);await custody(client,request,deadline);
        return freeze({record:copy(existing.payload),recordHash:existing.record_sha256,newlyRecorded:false,newlyStarted:false,continuationImplemented:false});}
      check((history.head?.record_sha256??null)===request.previousRecordHash);
      const sequence=history.rows.length+1;let value;
      if(operation==='beginChild'){
        check(sequence===1);const child=selected(request.child.partId);
        value={kind:'child-started',parent:request.parent,child:{...request.child,stepId:child.part.stepId,stage:child.part.stage,capabilityHash:child.capabilityHash},
          cursor:{parentInvocationId:parent.invocationId,observations:[],completedActions:0,decisionsConsumed:0,contextBytes:0},
          limits:{maximumDecisions:pinned.maximumDecisions,maximumContextBytes:pinned.maximumContextBytes,deadlineAtMs:deadline},reservation:null};
      }else if(operation==='acceptChildAndReserve'){
        check(sequence===2&&history.last.kind==='child-started'&&hash(history.last.claim)===hash(request.expectedClaim));
        const child={...history.last.child,retirementId:request.retirementId,outputReference:request.outputReference};
        const cursor={parentInvocationId:parent.invocationId,observations:[{kind:'child',invocationId:child.invocationId,partId:child.partId,outputReferenceHash:hash(child.outputReference)}],
          completedActions:1,decisionsConsumed:request.decisionsConsumed,contextBytes:request.contextBytes};
        value={kind:'child-accepted-cursor-reserved',parent,child,cursor,limits:history.last.limits,
          reservation:{reservationId:request.reservationId,kind:'provider-decision',partId:parent.partId,nextDecisionOrdinal:request.decisionsConsumed+1,cursorHash:hash(cursor),status:'reserved'}};
      }else{
        check(sequence===3&&history.last.kind==='child-accepted-cursor-reserved'&&history.last.reservation.reservationId===request.reservationId);
        value={...history.last,kind:'successor-started',reservation:{...history.last.reservation,status:'started'}};
      }
      check(value.limits.deadlineAtMs===deadline);
      const record={...value,schemaVersion:1,runId:request.runId,sequence,descriptorHash,requestId:request.requestId,requestHash,previousRecordHash:request.previousRecordHash,claim:request.expectedClaim};
      const recordHash=hash(record);
      await client.query(`INSERT INTO ${sql.records}(run_id,sequence,record_sha256,payload) VALUES($1,$2,$3,$4::jsonb)`,[request.runId,sequence,recordHash,JSON.stringify(record)]);
      if(sequence===1)await client.query(`INSERT INTO ${sql.heads}(run_id,sequence,record_sha256) VALUES($1,$2,$3)`,[request.runId,sequence,recordHash]);
      else await client.query(`UPDATE ${sql.heads} SET sequence=$2,record_sha256=$3 WHERE run_id=$1`,[request.runId,sequence,recordHash]);
      await custody(client,request,deadline);
      return freeze({record,recordHash,newlyRecorded:true,newlyStarted:operation==='markSuccessorStarted',continuationImplemented:false});
    });
  }
  const safe=fn=>async(...args)=>{try{return await fn(...args);}catch(error){
    // Host callbacks and database adapters can use any code. Never forward
    // their message, cause or other fields across this public boundary.
    if(error?.code==='WORKFLOW_CLAIM_LOST')throw claimLostError();
    if(['CHECKPOINT_RECORDS_CONFLICT','CHECKPOINT_RECORDS_FORBIDDEN'].includes(error?.code))throw refuse(error.code);
    throw refuse('CHECKPOINT_RECORDS_UNAVAILABLE');
  }};
  return Object.freeze({descriptorForRun:()=>copy(descriptor),
    beginChild:safe(input=>transition(input,'beginChild')),
    acceptChildAndReserve:safe(input=>transition(input,'acceptChildAndReserve')),
    markSuccessorStarted:safe(input=>transition(input,'markSuccessorStarted')),
    inspect:safe(async input=>{
      const request=normalize(input,'inspect');current();await guarded(authorize,{operation:'inspect',request,descriptor});
      return database.transaction(async client=>{
        await lineage(client,request);const history=await load(client,request.runId);
        if(history.last){const {deadline}=await lineage(client,request,history.last.parent);await custody(client,request,deadline);}
        await assertPostgresExpectedClaim(client,sql,request.runId,request.expectedClaim);current();
        return freeze({state:history.last?.kind??'absent',record:copy(history.last??null),recordHash:history.head?.record_sha256??null,continuationImplemented:false});
      });
    })});
}
