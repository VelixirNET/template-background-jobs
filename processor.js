// The example job. Replace the body of processJob with your own work: sending email through
// an HTTPS API, resizing an image, calling a slow third-party service, building a report.
//
// This one simulates a multi-step task. Each step waits a second, the way a real job waits on
// an API or a database, and reports progress so the dashboard can draw it. The outcome chosen
// on the form decides how it ends, which is how the retry and failure paths get exercised.

const { UnrecoverableError } = require('bullmq');

const STEP_MS = 1000;

const OUTCOMES = {
  succeed: 'Succeed',
  'fail-once': 'Fail once, then succeed',
  'fail-always': 'Fail every attempt',
  'fail-permanently': 'Fail permanently (no retry)',
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function processJob(job) {
  const { label, steps, outcome } = job.data;
  // attemptsMade counts the attempts that have already failed, so this is 1 on the first run.
  const attempt = job.attemptsMade + 1;
  const started = Date.now();
  const failAt = Math.ceil(steps / 2);

  // A retry starts from the beginning, so its progress should too. attemptsStarted also counts
  // a run that was cut off by a restart, which BullMQ hands out again without counting it as
  // a failure.
  if (job.attemptsStarted > 1) await job.updateProgress(0);

  for (let step = 1; step <= steps; step++) {
    await sleep(STEP_MS);

    if (step === failAt) {
      // Throwing is how a job reports failure. BullMQ records the message, and retries the
      // job after the backoff delay until its attempts run out.
      if (outcome === 'fail-once' && attempt === 1) {
        throw new Error(`Simulated failure at step ${step} of ${steps} on attempt 1. The retry will succeed.`);
      }
      if (outcome === 'fail-always') {
        throw new Error(`Simulated failure at step ${step} of ${steps} on attempt ${attempt}.`);
      }
      // Some failures will not get better by trying again: bad input, a record that no
      // longer exists. UnrecoverableError fails the job at once, whatever attempts remain.
      if (outcome === 'fail-permanently') {
        throw new UnrecoverableError(`Simulated permanent failure at step ${step} of ${steps}. Not retried.`);
      }
    }

    await job.updateProgress(Math.round((step / steps) * 100));
  }

  // Whatever the function returns is stored with the job as its result.
  const took = ((Date.now() - started) / 1000).toFixed(1);
  return {
    summary: `Finished all ${steps} steps of "${label}" in ${took} s on attempt ${attempt}.`,
    steps,
    attempt,
    tookMs: Date.now() - started,
  };
}

module.exports = { processJob, OUTCOMES };
