import {createHash} from 'node:crypto';
import {compositionCheckpointRecordHash as hash} from './agent-platform-composition-checkpoint-records.mjs';
import {normalizeExpectedClaim,claimLostError} from './agent-platform-claim-bound-journal.mjs';

const copy=value=>structuredClone(value);
const freeze=value=>{if(value&&typeof value==='object'){Object.values(value).forEach(freeze);Object.freeze(value);}return value;};
function refuse(){return Object.assign(new Error('Reserved child host continuation is unavailable.'),{code:'RESERVED_CHILD_CONTINUATION_UNAVAILABLE',statusCode:409});}
function check(value){if(!value)throw refuse();}
function exact(value,keys){check(value&&Object.getPrototypeOf(value)===Object.prototype&&Object.keys(value).sort().join('|')===[...keys].sort().join('|'));}
function normalizePolicy(value){
  exact(value,['schemaVersion','mode','policyId','policyVersion','resolverId','resolverVersion','contextContractId','contextContractVersion']);
  check(value.schemaVersion===1&&value.mode==='reserved-child-answer.v1');
  for(const key of ['policyId','resolverId','contextContractId'])check(typeof value[key]==='string'&&/^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$/.test(value[key]));
  for(const key of ['policyVersion','resolverVersion','contextContractVersion'])check(Number.isSafeInteger(value[key])&&value[key]>0);
  return copy(value);
}
export function reservedChildHostContinuationDescriptor({recordDescriptor,policy}={}){
  check(recordDescriptor?.protocol==='composition-checkpoint-records.v1');
  return freeze({...normalizePolicy(policy),recordDescriptorHash:hash(recordDescriptor)});
}

/** Separate host admission; never accepts a caller-provided dispatch receipt. */
export function createReservedChildHostContinuation({configuration,definition,assignment,limits,rootPrompt,inputForOutput,validateAttempt,execute,acceptRetiredAnswer}={}){
  exact(configuration,['records','journal','runtimeBinding','policy','getPolicy','authorize','resolveOutput','restoreContext']);
  const {records,journal,getPolicy,authorize,resolveOutput,restoreContext}=configuration;
  for(const fn of [getPolicy,authorize,resolveOutput,restoreContext,records?.descriptorForRun,records?.inspect,records?.markSuccessorStarted,journal?.inspectRun,inputForOutput,validateAttempt,execute])check(typeof fn==='function');
  check(acceptRetiredAnswer===undefined||typeof acceptRetiredAnswer==='function');
  const recordDescriptor=copy(records.descriptorForRun());
  check(recordDescriptor.definitionId===definition.id&&recordDescriptor.definitionHash===definition.definitionHash
    &&recordDescriptor.assignmentHash===assignment.assignmentHash&&recordDescriptor.runtimeBindingHash===hash(configuration.runtimeBinding)
    &&recordDescriptor.maximumDecisions===limits.maximumDecisions&&recordDescriptor.maximumContextBytes===limits.maximumContextBytes);
  const descriptor=reservedChildHostContinuationDescriptor({recordDescriptor,policy:configuration.policy});
  const current=()=>{
    check(hash(reservedChildHostContinuationDescriptor({recordDescriptor,policy:getPolicy()}))===hash(descriptor));
    check(hash(records.descriptorForRun())===hash(recordDescriptor));
  };
  return Object.freeze({descriptorForRun:()=>copy(descriptor),async continueReservedChild(input){
    try{
      check(input&&Object.getPrototypeOf(input)===Object.prototype);input={signal:undefined,...input};
      exact(input,['runId','expectedClaim','signal']);
      check(input.expectedClaim!==null&&input.expectedClaim!==undefined);
      const expectedClaim=normalizeExpectedClaim(input.expectedClaim),runId=input.runId,signal=input.signal;
      check(expectedClaim.runId===runId);signal?.throwIfAborted();current();
      let accepted;
      async function fresh(expectedHash){
        signal?.throwIfAborted();current();
        const state=await records.inspect({runId,expectedClaim});
        const run=await journal.inspectRun({runId});
        check(run?.runId===runId&&hash(run.authorization.reservedChildHostContinuation)===hash(descriptor)
          &&hash(run.authorization.assignedRuntime)===hash(configuration.runtimeBinding));
        if(state.record)validateAttempt(run,state.record);
        const final=await records.inspect({runId,expectedClaim});
        check(final.recordHash===state.recordHash&&(!expectedHash||final.recordHash===expectedHash));
        signal?.throwIfAborted();current();return final;
      }
      async function permit(phase,target){
        check(await authorize(freeze({phase,runId,expectedClaim:copy(expectedClaim),descriptor:copy(descriptor),...copy(target)}))===true);
        await fresh(accepted.recordHash);
      }
      accepted=await fresh();
      const record=accepted.record;
      check(accepted.state==='child-accepted-cursor-reserved'&&record.sequence===2&&record.reservation.status==='reserved'
        &&record.reservation.kind==='provider-decision'&&record.reservation.partId===record.parent.partId
        &&record.cursor.parentInvocationId===record.parent.invocationId&&record.cursor.completedActions===1
        &&record.cursor.observations.length===1&&record.cursor.observations[0].invocationId===record.child.invocationId
        &&record.cursor.observations[0].outputReferenceHash===hash(record.child.outputReference)
        &&record.reservation.cursorHash===hash(record.cursor)
        &&record.reservation.nextDecisionOrdinal===record.cursor.decisionsConsumed+1
        &&record.reservation.nextDecisionOrdinal<=record.limits.maximumDecisions);
      const target={record:copy(record),recordHash:accepted.recordHash};
      await permit('resolve',target);
      const text=await resolveOutput(freeze({runId,descriptor:copy(descriptor),...copy(target)}),{signal});
      await fresh(accepted.recordHash);
      check(typeof text==='string'&&Buffer.byteLength(text)===record.child.outputReference.byteLength
        &&Buffer.byteLength(text)<=record.limits.maximumContextBytes
        &&Buffer.from(text).toString('utf8')===text
        &&createHash('sha256').update(text).digest('hex')===record.child.outputReference.sha256);
      const output=JSON.parse(text);
      check(output&&Object.getPrototypeOf(output)===Object.prototype&&JSON.stringify(output)===text);
      const restoredInput=inputForOutput(copy(record),copy(output));
      const serialized=JSON.stringify(restoredInput);
      check(typeof serialized==='string'&&Buffer.byteLength(serialized)<=record.limits.maximumContextBytes
        &&restoredInput.task===rootPrompt&&restoredInput.observations.length===1
        &&hash(restoredInput.observations[0])===hash({kind:'child',partId:record.child.partId,output}));
      const context={recordHash:accepted.recordHash,inputHash:hash(restoredInput),decisionsConsumed:record.cursor.decisionsConsumed,
        contextBytes:record.cursor.contextBytes,maximumDecisions:record.limits.maximumDecisions,
        maximumContextBytes:record.limits.maximumContextBytes,deadlineAtMs:record.limits.deadlineAtMs};
      await permit('restore',{...target,context});
      const verified=await restoreContext(freeze({runId,descriptor:copy(descriptor),...copy(target),context:copy(context)}),{signal});
      await fresh(accepted.recordHash);check(hash(verified)===hash(context));
      const authorizeOperation=async operation=>{
        await permit(operation.phase==='answer'?'answer':'dispatch',{...target,context,adapterPhase:operation.phase});return true;
      };
      const fence=async()=>{
        await fresh(accepted.recordHash);
        const started=await records.markSuccessorStarted({runId,expectedClaim,requestId:'host-start:'+hash({runId,reservationId:record.reservation.reservationId}),
          previousRecordHash:accepted.recordHash,reservationId:record.reservation.reservationId});
        check(started.newlyStarted===true);
        accepted=await fresh(started.recordHash);check(accepted.state==='successor-started');
      };
      const result=await execute({record:copy(record),input:copy(restoredInput),signal,authorizeOperation,fence,current});
      await fresh(accepted.recordHash);
      const receipt=freeze({output:copy(result),runId,assignmentId:record.parent.assignmentId,attemptId:record.parent.attemptId,
        threadId:record.parent.threadId,acceptedRecordHash:target.recordHash,
        startedRecordHash:accepted.recordHash,restoration:context,decisionOrdinal:record.reservation.nextDecisionOrdinal,
        restoredInputBytes:Buffer.byteLength(serialized),workflowCompleted:false,executionMode:'reserved-child-answer.v1'});
      if(!acceptRetiredAnswer)return receipt;
      signal?.throwIfAborted();
      const answerAcceptance=await acceptRetiredAnswer(receipt,copy(expectedClaim));
      return freeze({...receipt,answerAcceptance});
    }catch(error){if(error?.code==='WORKFLOW_CLAIM_LOST')throw claimLostError();throw refuse();}
  }});
}
