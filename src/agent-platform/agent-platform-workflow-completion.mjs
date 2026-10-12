/** Borrow a host-configured generic journal without opening nested transactions.
 * The caller owns locks, rollback and post-callback claim/policy checks. All host
 * hooks must use this client or pure checks; external effects are unsupported.
 */
export async function completeWorkflowRunInTransaction({client,journalFactory,attempt,assignment,run}={}) {
  if(typeof client?.query!=='function'||typeof journalFactory!=='function')throw new TypeError('Owned transaction client and journal factory are required.');
  const journal=journalFactory({database:{transaction:work=>work(client),query:(...args)=>client.query(...args)}});
  const finishedAttempt=await journal.finishAttempt(attempt);
  const finishedAssignment=await journal.finishAssignment(assignment);
  const finishedRun=await journal.transitionRun(run);
  return {attempt:finishedAttempt,assignment:finishedAssignment,run:finishedRun};
}
