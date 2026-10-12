import {createHash} from 'node:crypto';
import {resolvePostgresRelations} from '../shared/postgres-relations.mjs';
import {compositionCheckpointRecordHash as hash} from './agent-platform-composition-checkpoint-records.mjs';
import {reservedChildHostContinuationDescriptor} from './agent-platform-reserved-child-host-continuation.mjs';
import {normalizeExpectedClaim,assertPostgresExpectedClaim,claimLostError} from './agent-platform-claim-bound-journal.mjs';
import {workflowDefinitionHash} from './agent-platform-prompt.mjs';
import {completeWorkflowRunInTransaction} from './agent-platform-workflow-completion.mjs';

const copy=value=>structuredClone(value);
const freeze=value=>{if(value&&typeof value==='object'){Object.values(value).forEach(freeze);Object.freeze(value);}return value;};
const sha=text=>createHash('sha256').update(text).digest('hex');
const millis=value=>value instanceof Date?value.getTime():Date.parse(value);
function refuse(){return Object.assign(new Error('Reserved answer finalization is unavailable.'),{code:'RESERVED_ANSWER_FINALIZATION_UNAVAILABLE',statusCode:409});}
function check(value){if(!value)throw refuse();}
function exact(value,keys){check(value&&Object.getPrototypeOf(value)===Object.prototype&&Object.keys(value).sort().join('|')===[...keys].sort().join('|'));}
function policy(value){
  exact(value,['schemaVersion','mode','policyId','policyVersion','writerId','writerVersion','completionPolicyId','completionPolicyVersion','outputOwnerId','outputContractId','telemetryMode']);
  check(value.schemaVersion===1&&value.mode==='reserved-answer-finalization.v1'&&value.telemetryMode==='unknown.v1');
  for(const key of ['policyId','writerId','completionPolicyId','outputOwnerId','outputContractId'])check(typeof value[key]==='string'&&/^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$/.test(value[key]));
  for(const key of ['policyVersion','writerVersion','completionPolicyVersion'])check(Number.isSafeInteger(value[key])&&value[key]>0);
  return copy(value);
}
export function reservedAnswerFinalizationDescriptor({recordDescriptor,continuationDescriptor,policy:input}={}) {
  check(recordDescriptor?.protocol==='composition-checkpoint-records.v1'&&continuationDescriptor?.recordDescriptorHash===hash(recordDescriptor));
  return freeze({...policy(input),recordDescriptorHash:hash(recordDescriptor),continuationDescriptorHash:hash(continuationDescriptor),
    definitionId:recordDescriptor.definitionId,definitionHash:recordDescriptor.definitionHash,
    runtimeBindingHash:recordDescriptor.runtimeBindingHash,assignmentHash:recordDescriptor.assignmentHash});
}
export function getReservedAnswerFinalizationSchemaUrl(){return new URL('../../schemas/agent-platform/reserved-answer-finalization-v1.sql',import.meta.url);}

/** Host-owned acceptance capability and provider-free finalizer. attestAnswer is
 * an independent output-custody/retirement check, never the caller's self-report.
 */
export function createReservedAnswerFinalization({database,relations,journalFactory,records,continuationDescriptor,
  policy:inputPolicy,getPolicy,getRecordPolicy,getContinuationPolicy,authorize,writeAnswer,attestAnswer,resolveAnswer}={}) {
  for(const fn of [database?.transaction,journalFactory,records?.descriptorForRun,records?.inspect,getPolicy,getRecordPolicy,getContinuationPolicy,authorize,writeAnswer,attestAnswer,resolveAnswer])check(typeof fn==='function');
  const sql=resolvePostgresRelations(relations,['runs','versions','assignments','attempts','events','heads','records','acceptances','completions'],'Reserved answer finalization');
  const pinnedPolicy=policy(inputPolicy);
  const recordDescriptor=copy(records.descriptorForRun()),continuation=copy(continuationDescriptor);
  const descriptor=reservedAnswerFinalizationDescriptor({recordDescriptor,continuationDescriptor:continuation,policy:inputPolicy});
  const descriptorHash=hash(descriptor),recordHash=hash(recordDescriptor);
  const recordPolicyKeys=['schemaVersion','protocol','policyId','policyVersion','writerId','outputOwnerId','outputContractId','maximumDecisions','maximumContextBytes'];
  const recordPolicy=Object.fromEntries(recordPolicyKeys.map(key=>[key,recordDescriptor[key]]));
  function current(){
    check(hash(policy(getPolicy()))===hash(pinnedPolicy)&&hash(getRecordPolicy())===hash(recordPolicy)
      &&hash(reservedChildHostContinuationDescriptor({recordDescriptor,policy:getContinuationPolicy()}))===hash(continuation)
      &&hash(records.descriptorForRun())===recordHash);
  }
  const safe=work=>async(...args)=>{try{return await work(...args);}catch(error){
    let code;try{code=error?.code;}catch{} // Host errors may have hostile accessors.
    if(code==='WORKFLOW_CLAIM_LOST')throw claimLostError();throw refuse();
  }};
  function request(input){exact(input,['runId','expectedClaim']);check(input.expectedClaim!=null);const claim=normalizeExpectedClaim(input.expectedClaim);check(claim.runId===input.runId);current();return {runId:input.runId,expectedClaim:claim};}
  async function permit(phase,target){current();check(await authorize(freeze({phase,descriptor:copy(descriptor),...copy(target)}))===true);current();}
  async function clock(client,runId){const row=(await client.query(`SELECT clock_timestamp() AS checked_at,EXISTS(SELECT 1 FROM ${sql.events} WHERE run_id=$1 AND source_key='workflow.cancellation-requested.v1') AS cancelled`,[runId])).rows[0];current();check(!row.cancelled);return millis(row.checked_at);}
  async function custody(client,input,deadline){current();await assertPostgresExpectedClaim(client,sql,input.runId,input.expectedClaim);const time=await clock(client,input.runId);check(Number.isFinite(time)&&(deadline===undefined||time<deadline));current();}
  async function load(client,runId){
    // Match the journal's assignment -> attempt -> run order. Never acquire an
    // assignment lock while holding only a run lock in this integration.
    const assignments=(await client.query(`SELECT * FROM ${sql.assignments} WHERE run_id=$1 ORDER BY assignment_ordinal FOR UPDATE`,[runId])).rows;
    const attempts=(await client.query(`SELECT * FROM ${sql.attempts} WHERE run_id=$1 ORDER BY attempt_ordinal FOR UPDATE`,[runId])).rows;
    const run=(await client.query(`SELECT * FROM ${sql.runs} WHERE id=$1 FOR UPDATE`,[runId])).rows[0];
    check(run&&assignments.length===1&&attempts.length===1&&run.resumed_from_run_id===null&&run.run_family_budget_version!==3);
    const assignment=assignments[0],attempt=attempts[0];check(attempt.resource_reserved_total_tokens==null
      &&hash(run.input_payload)===run.input_sha256&&hash(attempt.prompt)===attempt.prompt_sha256
      &&hash(attempt.capability_profile)===attempt.capability_sha256);
    const rows=(await client.query(`SELECT * FROM ${sql.records} WHERE run_id=$1 ORDER BY sequence`,[runId])).rows;
    const head=(await client.query(`SELECT * FROM ${sql.heads} WHERE run_id=$1`,[runId])).rows[0];
    check(rows.length===3&&head?.sequence===3);let previous=null;
    for(let i=0;i<3;i++){const row=rows[i];check(row.sequence===i+1&&row.record_sha256===hash(row.payload)&&row.payload.previousRecordHash===previous
      &&row.payload.runId===runId&&row.payload.descriptorHash===recordHash&&row.payload.sequence===i+1
      &&row.payload.kind===['child-started','child-accepted-cursor-reserved','successor-started'][i]);previous=row.record_sha256;}
    check(head.record_sha256===previous);
    const accepted=rows[1].payload,started=rows[2].payload,parent=started.parent;
    check(hash({...started,kind:accepted.kind,reservation:{...started.reservation,status:'reserved'},sequence:accepted.sequence,
      requestId:accepted.requestId,requestHash:accepted.requestHash,previousRecordHash:accepted.previousRecordHash,claim:accepted.claim})===hash(accepted));
    check(started.reservation.status==='started'&&parent.assignmentId===assignment.id&&parent.attemptId===attempt.id
      &&parent.threadId===attempt.thread_id&&parent.threadId===assignment.agent_thread_id&&attempt.assignment_id===assignment.id
      &&assignment.parent_assignment_id===null&&attempt.attempt_ordinal===1&&assignment.execution==='current-agent');
    check(run.workflow_version_id===descriptor.definitionId&&hash(run.authorization_snapshot.compositionCheckpointProtocol)===recordHash
      &&hash(run.authorization_snapshot.reservedChildHostContinuation)===hash(continuation)
      &&hash(run.authorization_snapshot.reservedAnswerFinalization)===descriptorHash
      &&hash(run.authorization_snapshot.assignedRuntime)===descriptor.runtimeBindingHash
      &&attempt.capability_profile.deadlineAtMs===started.limits.deadlineAtMs);
    const version=(await client.query(`SELECT * FROM ${sql.versions} WHERE id=$1`,[run.workflow_version_id])).rows[0];
    check(version?.definition_sha256===descriptor.definitionHash&&workflowDefinitionHash(version.definition)===descriptor.definitionHash);
    const acceptance=(await client.query(`SELECT * FROM ${sql.acceptances} WHERE run_id=$1 FOR UPDATE`,[runId])).rows[0];
    const completion=(await client.query(`SELECT * FROM ${sql.completions} WHERE run_id=$1 FOR UPDATE`,[runId])).rows[0];
    if(acceptance)check(acceptance.receipt_sha256===hash(acceptance.payload));
    if(completion)check(completion.receipt_sha256===hash(completion.payload)&&completion.accepted_sha256===acceptance?.receipt_sha256);
    current();return {run,assignment,attempt,accepted,started,acceptedHash:rows[1].record_sha256,startedHash:previous,acceptance,completion};
  }
  function binding(state){return {runIdentityHash:state.run.identity_sha256,inputHash:state.run.input_sha256,authorizationHash:hash(state.run.authorization_snapshot),
    assignmentIdentityHash:state.assignment.identity_sha256,attemptPromptHash:state.attempt.prompt_sha256,attemptCapabilityHash:state.attempt.capability_sha256,
    requested: [state.attempt.requested_model,state.attempt.requested_reasoning_effort,state.attempt.requested_service_tier],
    budget:{version:state.run.run_family_budget_version,descendants:state.run.run_family_max_descendant_runs,attempts:state.run.run_family_max_total_attempts,tokens:state.run.run_family_max_total_tokens}};}
  function validateAcceptance(state){
    const a=state.acceptance?.payload;check(a&&a.schemaVersion===1&&a.runId===state.run.id&&a.descriptorHash===descriptorHash
      &&a.acceptedRecordHash===state.acceptedHash&&a.startedRecordHash===state.startedHash&&hash(a.parent)===hash(state.started.parent)
      &&hash(a.dispatchClaim)===hash(state.started.claim)&&hash(a.binding)===hash(binding(state))
      &&a.decisionOrdinal===state.started.reservation.nextDecisionOrdinal&&hash(a.reservation)===hash(state.started.reservation));
    const r=a.restoration,s=state.started;check(r.recordHash===state.acceptedHash&&r.decisionsConsumed===s.cursor.decisionsConsumed&&r.contextBytes===s.cursor.contextBytes
      &&r.maximumDecisions===s.limits.maximumDecisions&&r.maximumContextBytes===s.limits.maximumContextBytes&&r.deadlineAtMs===s.limits.deadlineAtMs
      &&a.restoredInputBytes<=r.maximumContextBytes&&a.decisionOrdinal<=r.maximumDecisions);
    exact(r,['recordHash','inputHash','decisionsConsumed','contextBytes','maximumDecisions','maximumContextBytes','deadlineAtMs']);
    check(/^[a-f0-9]{64}$/.test(r.inputHash)&&Number.isSafeInteger(a.restoredInputBytes)&&a.restoredInputBytes>0);
    check(/^[a-f0-9]{64}$/.test(a.outputValueHash));reference(a.outputReference);return a;
  }
  function reference(ref){exact(ref,['ownerId','contractId','referenceId','sha256','byteLength']);check(ref.ownerId===descriptor.outputOwnerId&&ref.contractId===descriptor.outputContractId
    &&typeof ref.referenceId==='string'&&/^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$/.test(ref.referenceId)
    &&/^[a-f0-9]{64}$/.test(ref.sha256)&&Number.isSafeInteger(ref.byteLength)&&ref.byteLength>0&&ref.byteLength<=32000);return copy(ref);}
  function body(text,ref){check(typeof text==='string'&&Buffer.from(text).toString('utf8')===text&&Buffer.byteLength(text)<=32000);
    const output=JSON.parse(text);check(output&&Object.getPrototypeOf(output)===Object.prototype&&JSON.stringify(output)===text);
    if(ref)check(Buffer.byteLength(text)===ref.byteLength&&sha(text)===ref.sha256);return output;}
  function shape(a,acceptedHash,output){
    const checkpointFinalization={mode:descriptor.mode,descriptorHash,acceptedAnswerHash:acceptedHash,parent:copy(a.parent),acceptedRecordHash:a.acceptedRecordHash,
      startedRecordHash:a.startedRecordHash,reservation:copy(a.reservation),decisionOrdinal:a.decisionOrdinal,restoration:copy(a.restoration),restoredInputBytes:a.restoredInputBytes,outputReference:copy(a.outputReference),outputValueHash:a.outputValueHash};
    const result={output,checkpointFinalization,telemetryIssues:['provider-usage-unobserved','reviewed-price-unobserved','costs-unobserved']};
    const finish={outcome:'completed',observed:null,usage:null,telemetryStatus:'incomplete',result,error:null,price:null,apiEquivalentCost:null,billedCost:null};
    const summary={checkpointFinalization,telemetryStatus:'incomplete',usage:null,apiEquivalentCost:null,billedCost:null};
    return {result,finish,summary};
  }
  function confirmation(state){
    const a=validateAcceptance(state),c=state.completion?.payload,x=state.attempt,r=state.run,s=state.assignment;
    check(c&&c.schemaVersion===1&&c.runId===r.id&&c.descriptorHash===descriptorHash&&c.acceptedAnswerHash===state.acceptance.receipt_sha256
      &&r.status==='completed'&&s.status==='completed'&&x.outcome==='completed'&&r.claimed_by===null&&r.claimed_at===null&&r.lease_expires_at===null);
    const expected=shape(a,state.acceptance.receipt_sha256,x.result_payload?.output);
    check(c.resultHash===hash(expected.result)&&x.result_sha256===c.resultHash&&hash(x.result_payload)===c.resultHash
      &&c.attemptFinishHash===hash(expected.finish)&&x.finish_sha256===c.attemptFinishHash
      &&c.assignmentFinishHash===hash({status:'completed',failure:null})&&s.finish_sha256===c.assignmentFinishHash
      &&c.finalSummaryHash===hash(expected.summary)&&hash(r.final_summary)===c.finalSummaryHash
      &&x.telemetry_status==='incomplete'&&x.observed_model===null&&x.observed_provider===null&&x.input_tokens===null&&x.total_tokens===null
      &&['observed_reasoning_effort','observed_service_tier','cached_input_tokens','cache_write_input_tokens','output_tokens','reasoning_output_tokens',
        'price_version_id','api_equivalent_cost','api_equivalent_cost_currency','billed_cost','billed_cost_currency','error'].every(key=>x[key]===null)&&s.failure===null);
    check(s.completed_at!=null&&s.finished_at!=null&&x.completed_at!=null&&r.completed_at!=null&&r.admitted_manifest_sha256===null);
    check(hash(x.result_payload.output)===a.outputValueHash); // JSONB preserves values, not owner byte ordering.
    return freeze({runId:r.id,acceptedAnswerHash:state.acceptance.receipt_sha256,completionHash:state.completion.receipt_sha256,workflowCompleted:true,telemetryStatus:'incomplete'});
  }
  // This capability is installed only in the host's post-retirement return path.
  // It is not an HTTP/worker receipt admission endpoint. The independent owner
  // attester must reject fabricated local answers and missing retirement.
  const acceptRetiredHostAnswer=safe(async (receipt,expectedClaim)=>{
    receipt=freeze(copy(receipt)); // One attested snapshot across every await.
    const input=request({runId:receipt?.runId,expectedClaim});
    exact(receipt,['output','runId','assignmentId','attemptId','threadId','acceptedRecordHash','startedRecordHash','restoration','decisionOrdinal','restoredInputBytes','workflowCompleted','executionMode']);
    check(receipt.workflowCompleted===false&&receipt.executionMode==='reserved-child-answer.v1');
    const text=JSON.stringify(receipt.output);check(hash(body(text))===hash(receipt.output));
    async function fresh(){current();const state=await records.inspect(input);check(state.state==='successor-started'&&state.recordHash===receipt.startedRecordHash
      &&hash(state.record.claim)===hash(input.expectedClaim));
      await database.transaction(async client=>{const stored=await load(client,input.runId);
        check(stored.run.status==='running'&&stored.assignment.status==='running'&&stored.attempt.outcome===null&&!stored.completion
          &&stored.startedHash===receipt.startedRecordHash&&hash(stored.started.claim)===hash(input.expectedClaim));
        await custody(client,input,state.record.limits.deadlineAtMs);
      });current();return state.record;}
    const started=await fresh();check(receipt.acceptedRecordHash===started.previousRecordHash&&receipt.assignmentId===started.parent.assignmentId
      &&receipt.attemptId===started.parent.attemptId&&receipt.threadId===started.parent.threadId);
    const target={...input,receipt:copy(receipt),outputHash:sha(text),outputBytes:Buffer.byteLength(text)};
    await permit('persist-answer',target);await fresh();
    const ref=reference(await writeAnswer(freeze({...copy(target),text,idempotencyKey:'reserved-answer:'+hash({runId:input.runId,startedRecordHash:receipt.startedRecordHash})})));
    await fresh();check(ref.sha256===sha(text)&&ref.byteLength===Buffer.byteLength(text));
    check(await attestAnswer(freeze({...copy(target),outputReference:copy(ref)}))===true);await fresh();
    return database.transaction(async client=>{
      const state=await load(client,input.runId);check(state.run.status==='running'&&state.assignment.status==='running'&&state.attempt.outcome===null&&!state.completion);
      await custody(client,input,started.limits.deadlineAtMs);check(hash(state.started.claim)===hash(input.expectedClaim));
      const payload={schemaVersion:1,runId:input.runId,descriptorHash,parent:copy(started.parent),binding:binding(state),dispatchClaim:copy(input.expectedClaim),
        acceptedRecordHash:receipt.acceptedRecordHash,startedRecordHash:receipt.startedRecordHash,reservation:copy(started.reservation),decisionOrdinal:receipt.decisionOrdinal,
        restoration:copy(receipt.restoration),restoredInputBytes:receipt.restoredInputBytes,outputReference:ref,outputValueHash:hash(receipt.output)};
      if(state.acceptance)check(state.acceptance.receipt_sha256===hash(payload));
      else await client.query(`INSERT INTO ${sql.acceptances}(run_id,receipt_sha256,payload) VALUES($1,$2,$3::jsonb)`,[input.runId,hash(payload),JSON.stringify(payload)]);
      validateAcceptance({...state,acceptance:{payload}});await custody(client,input,started.limits.deadlineAtMs);
      return freeze({runId:input.runId,acceptedAnswerHash:hash(payload),workflowCompleted:false});
    });
  });
  const finalize=safe(async supplied=>{
    const input=request(supplied);
    let snapshot=await database.transaction(async client=>{const state=await load(client,input.runId);validateAcceptance(state);
      if(state.run.status!=='completed')await custody(client,input);return state;});
    if(snapshot.run.status==='completed'){
      await permit('confirm-completion',{...input,acceptedAnswerHash:snapshot.acceptance.receipt_sha256,completionHash:snapshot.completion?.receipt_sha256});
      return database.transaction(async client=>{const state=await load(client,input.runId);check(state.acceptance.receipt_sha256===snapshot.acceptance.receipt_sha256);const result=confirmation(state);await clock(client,input.runId);return result;});
    }
    const a=validateAcceptance(snapshot),target={...input,acceptedAnswerHash:snapshot.acceptance.receipt_sha256,acceptance:copy(a)};
    async function freshOpen(){return database.transaction(async client=>{const state=await load(client,input.runId);
      check(state.run.status==='running'&&state.acceptance.receipt_sha256===snapshot.acceptance.receipt_sha256);validateAcceptance(state);await custody(client,input);});}
    await permit('resolve-answer',target);await freshOpen();
    const text=await resolveAnswer(freeze(copy(target)));current();const output=body(text,a.outputReference);check(hash(output)===a.outputValueHash);await freshOpen();
    await permit('complete-workflow',target);
    return database.transaction(async client=>{
      const state=await load(client,input.runId);check(state.acceptance.receipt_sha256===snapshot.acceptance.receipt_sha256);validateAcceptance(state);
      if(state.run.status==='completed'){
        await permit('confirm-completion',{...input,acceptedAnswerHash:state.acceptance.receipt_sha256,completionHash:state.completion?.receipt_sha256});
        const result=confirmation(state);await clock(client,input.runId);return result;
      }
      check(state.run.status==='running'&&state.assignment.status==='running'&&state.attempt.outcome===null&&!state.completion);
      await custody(client,input);const leaseDeadline=millis(state.run.lease_expires_at),expected=shape(a,state.acceptance.receipt_sha256,output);
      await completeWorkflowRunInTransaction({client,journalFactory,attempt:{attemptId:state.attempt.id,...expected.finish,expectedClaim:input.expectedClaim},
        assignment:{assignmentId:state.assignment.id,status:'completed',failure:null,expectedClaim:input.expectedClaim},
        run:{runId:input.runId,status:'completed',finalSummary:expected.summary,expectedClaim:input.expectedClaim}});
      // transitionRun clears our own claim. The locked original epoch remains
      // exclusive; check its captured lease after ALL host callbacks instead.
      check(await clock(client,input.runId)<leaseDeadline);current();
      const payload={schemaVersion:1,runId:input.runId,descriptorHash,acceptedAnswerHash:state.acceptance.receipt_sha256,
        resultHash:hash(expected.result),attemptFinishHash:hash(expected.finish),assignmentFinishHash:hash({status:'completed',failure:null}),finalSummaryHash:hash(expected.summary)};
      await client.query(`INSERT INTO ${sql.completions}(run_id,accepted_sha256,receipt_sha256,payload) VALUES($1,$2,$3,$4::jsonb)`,[input.runId,state.acceptance.receipt_sha256,hash(payload),JSON.stringify(payload)]);
      const finished=await load(client,input.runId),result=confirmation(finished);check(await clock(client,input.runId)<leaseDeadline);current();return result;
    });
  });
  current();return Object.freeze({descriptorForRun:()=>copy(descriptor),acceptRetiredHostAnswer,finalize});
}
