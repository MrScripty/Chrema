/** Trusted process lifecycle, not an agent tool. Construction and stop are synchronous.
 * Register each acquired resource immediately; constructors own any partial
 * acquisition that throws before returning. Signals are handled after construction.
 */
export function createWorkerRuntimeFactory({ buildWorker } = {}) {
  if (typeof buildWorker !== 'function') throw new TypeError('Worker runtime buildWorker must be callable.');
  return async function runWorkerRuntime(options = {}) {
    const resources = [];
    const cleanupErrors = [];
    const stopErrors = [];
    const executionErrors = [];
    let acquiring = true;
    const own = (value, dispose) => {
      if (!acquiring) throw new TypeError('Worker resources must be registered during synchronous construction.');
      if (typeof dispose !== 'function') throw new TypeError('An owned resource requires a disposer.');
      resources.push(() => dispose(value));
      return value;
    };
    const signals = options.signals ?? process;
    const abort = new AbortController();
    let worker;
    let stopping = false;
    let installingSignals = false;
    const stop = () => {
      if (stopping) return;
      stopping = true;
      try { worker.stop(); }
      catch (error) { stopErrors.push(error); }
      finally { abort.abort(); }
    };
    try {
      try { worker = buildWorker(options, Object.freeze({ own })); }
      finally { acquiring = false; }
      if (worker && typeof worker.then === 'function') {
        // Reject asynchronous constructors without leaving their rejection unhandled.
        Promise.resolve(worker).catch(() => {});
        throw new TypeError('Worker runtime construction must be synchronous.');
      }
      if (typeof worker?.stop !== 'function' || typeof worker?.runForever !== 'function') {
        throw new TypeError('Worker runtime requires stop and runForever methods.');
      }
      installingSignals = true;
      signals.once('SIGINT', stop);
      signals.once('SIGTERM', stop);
      await worker.runForever({ signal: abort.signal });
    } catch (error) {
      executionErrors.push(error);
    } finally {
      if (installingSignals) {
        for (const name of ['SIGINT', 'SIGTERM']) {
          try { signals.removeListener(name, stop); }
          catch (error) { cleanupErrors.push(error); }
        }
      }
      for (const dispose of resources.reverse()) {
        try { await dispose(); }
        catch (error) { cleanupErrors.push(error); }
      }
    }
    // Preserve original error objects, ordered execution, stop, then cleanup.
    const errors = [...executionErrors, ...stopErrors, ...cleanupErrors];
    if (errors.length === 1) throw errors[0];
    if (errors.length > 1) throw new AggregateError(errors, 'Worker runtime encountered multiple failures.');
  };
}
