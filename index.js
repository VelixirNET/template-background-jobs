// Background jobs on managed Valkey: a web dashboard and a BullMQ worker in one process.
//
// The web side answers requests at once and hands slow work to a queue in Valkey. The worker
// takes jobs off that queue and runs them, reporting progress, retrying failures with backoff,
// and keeping a bounded history. Both halves start here, because that is the shape most apps
// begin with: one process, one deploy. ROLE splits them apart later without a code change.
//
// Deploy it from the velixir gallery and a managed Valkey is created and bound for you, so
// REDIS_URL is in the environment on the first boot.

const { Queue, Worker } = require('bullmq');
const { QUEUE_NAME, defaultJobOptions, createConnection } = require('./queue');
const { processJob } = require('./processor');
const { setupApp, dashboardApp, workerApp } = require('./web');

const port = process.env.PORT || 8080;
const redisUrl = process.env.REDIS_URL;

// ROLE=web serves the dashboard and API and runs no jobs. ROLE=worker runs jobs and serves only
// a status page. ROLE=all (the default) does both in this one process.
const ROLES = ['all', 'web', 'worker'];
let role = (process.env.ROLE || 'all').trim().toLowerCase();
if (!ROLES.includes(role)) {
  console.warn(`ROLE=${process.env.ROLE} is not one of ${ROLES.join(', ')}; running as all.`);
  role = 'all';
}
const runsWeb = role !== 'worker';
const runsWorker = role !== 'web';

// How many jobs one process runs at the same time. These jobs mostly wait, so a few in
// parallel costs almost nothing. CPU-heavy jobs want 1 per process, and more processes.
const concurrency = Math.max(Number.parseInt(process.env.WORKER_CONCURRENCY, 10) || 2, 1);

// How long a SIGTERM waits for running jobs. velixir, like Kubernetes, allows 30 seconds
// between SIGTERM and a hard kill, so stay under that.
const SHUTDOWN_TIMEOUT_MS = 25_000;

// A stray rejection from a library should be logged, not take the worker (and every job it is
// running) down with it.
process.on('unhandledRejection', (err) => console.error('unhandled rejection:', err));

// ─── Setup mode ─────────────────────────────────────────────────────────────

// No Valkey bound yet. Rather than exiting (a crash loop reads as "the platform is broken"),
// serve a page that says exactly what is missing, and keep the health check green so the
// page stays reachable.
if (!redisUrl) {
  console.log(
    'REDIS_URL is not set, so this app is serving its setup page.\n' +
    'On velixir: create a managed Valkey, bind it to this app as REDIS_URL, then redeploy.\n' +
    'Locally: REDIS_URL=redis://localhost:6379 npm start'
  );
  const server = setupApp().listen(port, '0.0.0.0', () => console.log(`setup page listening on ${port}`));
  process.on('SIGTERM', () => server.close(() => process.exit(0)));
  process.on('SIGINT', () => server.close(() => process.exit(0)));
  return; // CommonJS allows a top-level return; nothing below runs without Valkey.
}

// ─── Valkey ─────────────────────────────────────────────────────────────────

const state = { ready: false, lastError: null };

// During an outage the same error arrives many times over: from every retry of every
// connection, and again through BullMQ's own error events. Log each distinct message at most
// once a minute, so the outage is visible in the logs without drowning them.
const lastLogged = new Map();
function logThrottled(message) {
  const now = Date.now();
  if (now - (lastLogged.get(message) ?? 0) < 60_000) return;
  if (lastLogged.size > 100) lastLogged.clear();
  lastLogged.set(message, now);
  console.error(message);
}

const onReady = () => {
  if (!state.ready || state.lastError) console.log('Valkey ready');
  state.ready = true;
  state.lastError = null;
  lastLogged.clear();
};

const onError = (err) => {
  state.lastError = err.message;
  logThrottled(`Valkey not reachable, retrying: ${err.message}`);
};

const connections = [];
let queue = null;
let worker = null;
const stats = { active: 0, completed: 0, failed: 0 };

if (runsWeb) {
  const redis = createConnection(redisUrl, { forWorker: false, onReady, onError });
  connections.push(redis);
  queue = new Queue(QUEUE_NAME, { connection: redis, defaultJobOptions });
  // BullMQ re-emits connection errors here. Without a listener Node treats them as fatal.
  queue.on('error', (err) => logThrottled(`queue: ${err.message}`));
}

if (runsWorker) {
  // In ROLE=all the web connection already reports status; one voice is enough.
  const redis = createConnection(redisUrl, runsWeb ? { forWorker: true } : { forWorker: true, onReady, onError });
  connections.push(redis);

  worker = new Worker(QUEUE_NAME, processJob, {
    connection: redis,
    concurrency,
    // Shows up in CLIENT LIST, so you can tell which instance holds a connection.
    name: process.env.HOSTNAME || `worker-${process.pid}`,
  });

  worker.on('active', (job) => {
    stats.active += 1;
    console.log(`job ${job.id} started (attempt ${job.attemptsStarted} of ${job.opts.attempts ?? 1})`);
  });
  worker.on('completed', (job) => {
    stats.active = Math.max(stats.active - 1, 0);
    stats.completed += 1;
    console.log(`job ${job.id} completed`);
  });
  worker.on('failed', (job, err) => {
    stats.active = Math.max(stats.active - 1, 0);
    stats.failed += 1;
    const retrying = job && job.attemptsMade < (job.opts.attempts ?? 1) && err.name !== 'UnrecoverableError';
    console.log(`job ${job?.id} failed: ${err.message}${retrying ? ' (will retry)' : ''}`);
  });
  // Connection trouble lands here too. BullMQ keeps retrying on its own; without a listener
  // Node would treat the event as fatal and the worker would stop.
  worker.on('error', (err) => logThrottled(`worker: ${err.message}`));
}

const primary = connections[0];

// ─── HTTP ───────────────────────────────────────────────────────────────────

const app = runsWeb
  ? dashboardApp({ queue, redis: primary, state, workerInProcess: runsWorker })
  : workerApp({ state, ping: () => primary.ping(), stats, concurrency });

// Listen at once, even before Valkey answers: the waiting page needs a server to be seen on.
// 0.0.0.0, not localhost: the platform's traffic comes from outside the container.
const server = app.listen(port, '0.0.0.0', () => {
  console.log(`listening on ${port} (role: ${role}${runsWorker ? `, concurrency ${concurrency}` : ''})`);
});

// ─── Graceful shutdown ──────────────────────────────────────────────────────

// A deploy or restart sends SIGTERM. Stop taking new work, let running jobs finish, then close
// the connections. A job still running when the timeout fires is not lost: its lock in Valkey
// expires, BullMQ marks it stalled, and the next worker to start picks it up again. That is
// also why jobs should be safe to run twice.
let shuttingDown = false;

async function shutdown(signal) {
  if (shuttingDown) {
    console.log(`${signal} again: exiting now`);
    process.exit(1);
  }
  shuttingDown = true;
  console.log(`${signal} received: finishing running jobs (up to ${SHUTDOWN_TIMEOUT_MS / 1000} s), then exiting`);

  setTimeout(() => {
    console.error('jobs still running at the shutdown deadline; exiting anyway. They will be retried.');
    process.exit(1);
  }, SHUTDOWN_TIMEOUT_MS).unref();

  server.close();
  // Never connected means nothing can be running, and closing would wait on a connection
  // that is not coming.
  if (state.ready) {
    try {
      if (worker) await worker.close(); // waits for the jobs in progress
      if (queue) await queue.close();
    } catch (err) {
      console.error(`error while closing: ${err.message}`);
    }
  }
  for (const connection of connections) connection.disconnect();
  console.log('shut down cleanly');
  process.exit(0);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
