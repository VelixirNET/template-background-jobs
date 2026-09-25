// The HTTP side: the dashboard, the JSON API, the setup page, and the small status page a
// worker-only instance serves. None of it runs jobs; that is the worker's business.

const express = require('express');
const { OUTCOMES } = require('./processor');

// Refuse new jobs past this many unfinished ones. The dashboard has no login, so this is what
// stops a script hammering the form from filling Valkey with work nobody asked for.
const MAX_BACKLOG = 200;
const MAX_STEPS = 10;

// Listed from earliest to latest, so a job that moves on between two reads keeps its newer state.
const LIST_STATES = ['waiting', 'prioritized', 'delayed', 'active', 'completed', 'failed'];

// Everything that reaches the browser as HTML goes through this. Job labels are user input.
const escapeHtml = (s) =>
  String(s).replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  })[c]);

const withTimeout = (promise, ms) =>
  Promise.race([promise, new Promise((_, reject) => setTimeout(() => reject(new Error('timed out')), ms))]);

// ─── Setup mode ─────────────────────────────────────────────────────────────

function setupApp() {
  const app = express();
  app.get('/healthz', (_req, res) => res.type('text/plain').send('ok (setup mode: no Valkey bound)'));
  app.use('/api', (_req, res) =>
    res.status(503).json({ error: 'No Valkey is bound to this app yet. Bind a managed Valkey as REDIS_URL and redeploy.' }));
  app.use((_req, res) => res.type('text/html').send(page('Connect Valkey', setupBody())));
  return app;
}

// ─── Health and the waiting page ────────────────────────────────────────────

// Liveness AND readiness, as in the notes template: a queue app without its queue is useless,
// so an unreachable Valkey takes the instance out of rotation instead of serving errors.
function health(app, { state, ping }) {
  app.get('/healthz', async (_req, res) => {
    if (!state.ready) return res.status(503).type('text/plain').send('waiting for Valkey');
    try {
      await withTimeout(ping(), 2000);
      res.type('text/plain').send('ok');
    } catch {
      res.status(503).type('text/plain').send('Valkey unreachable');
    }
  });

  // Until the first successful connection, every other route explains the wait.
  app.use((req, res, next) => {
    if (state.ready) return next();
    if (req.path.startsWith('/api/')) {
      return res.status(503).json({ error: 'Waiting for Valkey.', detail: state.lastError });
    }
    res.status(503).type('text/html').send(page('Waiting for Valkey', waitingBody(state.lastError)));
  });
}

// ─── Dashboard and API ──────────────────────────────────────────────────────

function dashboardApp({ queue, redis, state, workerInProcess }) {
  const app = express();
  app.use(express.urlencoded({ extended: false, limit: '10kb' }));
  app.use(express.json({ limit: '10kb' }));
  health(app, { state, ping: () => redis.ping() });

  // Express 5 hands a rejected promise from an async handler to the error handler at the
  // bottom, so a Valkey hiccup mid-request becomes a 503 rather than an unhandled rejection.

  app.get('/api/jobs', async (_req, res) => {
    const [counts, jobs, workers, valkey] = await Promise.all([
      queue.getJobCounts(...LIST_STATES),
      recentJobs(queue),
      // Counts every worker connected to this queue, in this process or another app. It reads
      // CLIENT LIST, which a few hosted Redis services block, hence the fallback.
      queue.getWorkersCount().catch(() => null),
      valkeyMemory(redis),
    ]);
    res.json({ counts, jobs, workers, valkey });
  });

  app.get('/api/jobs/:id', async (req, res) => {
    const job = await queue.getJob(req.params.id);
    if (!job) return res.status(404).json({ error: 'No such job. Finished jobs are kept for a while, then removed.' });
    res.json(toJson(job, await job.getState()));
  });

  app.post('/api/jobs', async (req, res) => {
    const result = await enqueue(queue, req.body);
    if (result.error) return res.status(429).json({ error: result.error });
    res.status(201).json(toJson(result.job, 'waiting'));
  });

  // The same thing for the form when JavaScript is off.
  app.post('/jobs', async (req, res) => {
    const result = await enqueue(queue, req.body);
    if (result.error) return res.status(429).type('text/plain').send(result.error);
    res.redirect(303, '/');
  });

  app.get('/', (_req, res) => {
    res.type('text/html').send(page('Background jobs', dashboardBody(workerInProcess), dashboardScript));
  });

  app.use(errorHandler(redis));
  return app;
}

async function enqueue(queue, body) {
  const label = String(body?.label ?? '').trim().slice(0, 80) || 'Untitled job';
  const steps = Math.min(Math.max(Number.parseInt(body?.steps, 10) || 5, 1), MAX_STEPS);
  const outcome = Object.hasOwn(OUTCOMES, body?.outcome) ? body.outcome : 'succeed';

  const pending = await queue.getJobCountByTypes('waiting', 'prioritized', 'delayed', 'active');
  if (pending >= MAX_BACKLOG) {
    return { error: `The queue already holds ${pending} unfinished jobs. Wait for some to finish, then try again.` };
  }
  // Job data is stored as JSON in Valkey. Keep it small and serialisable: ids and parameters,
  // not whole files. A worker in another app reads exactly what is written here.
  const job = await queue.add('simulated-task', { label, steps, outcome });
  return { job };
}

async function recentJobs(queue, limit = 25) {
  const byId = new Map();
  for (const state of LIST_STATES) {
    for (const job of await queue.getJobs([state], 0, limit - 1)) {
      if (job) byId.set(job.id, toJson(job, state));
    }
  }
  return [...byId.values()].sort((a, b) => b.createdAt - a.createdAt).slice(0, limit);
}

function toJson(job, state) {
  const data = job.data || {};
  return {
    id: job.id,
    name: job.name,
    state,
    label: data.label,
    steps: data.steps,
    outcome: data.outcome,
    progress: typeof job.progress === 'number' ? job.progress : 0,
    // attemptsStarted counts every run, including the one in progress; attemptsMade counts
    // the runs that have finished.
    attemptsStarted: job.attemptsStarted ?? 0,
    attemptsMade: job.attemptsMade,
    maxAttempts: job.opts?.attempts ?? 1,
    result: job.returnvalue ?? null,
    failedReason: job.failedReason || null,
    createdAt: job.timestamp,
    processedAt: job.processedOn ?? null,
    finishedAt: job.finishedOn ?? null,
  };
}

// How full the cache is, for the dashboard footer. Cached briefly: it changes slowly and the
// page polls every second while jobs are running.
let memoryCache = { at: 0, value: null };
async function valkeyMemory(redis) {
  if (Date.now() - memoryCache.at < 10_000) return memoryCache.value;
  let value = null;
  try {
    const info = await redis.info('memory');
    const field = (name) => info.match(new RegExp(`^${name}:(.*)$`, 'm'))?.[1]?.trim();
    value = {
      usedBytes: Number(field('used_memory')) || 0,
      maxBytes: Number(field('maxmemory')) || 0,
      policy: field('maxmemory_policy') || null,
    };
  } catch {
    // Informational only. The job list still works without it.
  }
  memoryCache = { at: Date.now(), value };
  return value;
}

function errorHandler(redis) {
  // The dashboard keeps polling through an outage, so the same failure repeats every few
  // seconds. Say it once a minute.
  let lastOutageLog = 0;

  return (err, req, res, _next) => {
    // A malformed JSON body and the like: the client's mistake, said plainly.
    if (err.status >= 400 && err.status < 500) {
      return res.status(err.status).json({ error: err.expose ? err.message : 'Bad request.' });
    }
    // With the offline queue off, a command sent while Valkey is unreachable fails at once.
    // That is expected during an outage and the connection is already retrying, so it gets a
    // clear 503 and one log line rather than a stack trace per request.
    const unreachable = redis.status !== 'ready';
    if (!unreachable) {
      console.error(err);
    } else if (Date.now() - lastOutageLog > 60_000) {
      lastOutageLog = Date.now();
      console.error(`requests are failing while Valkey is unreachable: ${err.message}`);
    }
    const message = unreachable
      ? 'Valkey is unreachable right now. The app keeps retrying on its own; try again in a moment.'
      : 'Something went wrong.';
    const status = unreachable ? 503 : 500;
    if (req.path.startsWith('/api/')) return res.status(status).json({ error: message });
    res.status(status).type('text/plain').send(message);
  };
}

// ─── Worker-only instance ───────────────────────────────────────────────────

// With ROLE=worker there is no dashboard, but the app still answers on $PORT: a health check
// and a page that says what this instance is, so its URL does not look broken.
function workerApp({ state, ping, stats, concurrency }) {
  const app = express();
  health(app, { state, ping });
  app.get('/', (_req, res) => res.type('text/html').send(page('Worker', workerBody(stats, concurrency))));
  app.use((_req, res) => res.status(404).type('text/plain').send('This instance only runs jobs. The dashboard is in the web app.'));
  return app;
}

// ─── Pages ──────────────────────────────────────────────────────────────────

function dashboardBody(workerInProcess) {
  const outcomes = Object.entries(OUTCOMES)
    .map(([value, text]) => `<option value="${value}">${escapeHtml(text)}</option>`).join('');
  const steps = [3, 5, 8, 10]
    .map((n) => `<option value="${n}"${n === 5 ? ' selected' : ''}>${n} steps</option>`).join('');

  return `
  <h1>Background jobs</h1>
  <p class="sub">Queued in managed Valkey through <code>REDIS_URL</code>, run by a BullMQ worker
    ${workerInProcess ? 'in this same process' : 'in a separate worker app'}.</p>

  <div class="counts" id="counts">
    <div><b data-count="waiting">0</b>waiting</div>
    <div><b data-count="active">0</b>active</div>
    <div><b data-count="delayed">0</b>retrying</div>
    <div><b data-count="completed">0</b>completed</div>
    <div><b data-count="failed">0</b>failed</div>
  </div>

  <form class="new" id="enqueue" method="post" action="/jobs">
    <input type="text" name="label" value="Monthly report" maxlength="80" aria-label="Job name" required>
    <select name="steps" aria-label="Steps">${steps}</select>
    <select name="outcome" aria-label="Outcome">${outcomes}</select>
    <button type="submit">Enqueue</button>
  </form>

  <p class="notice" id="notice" hidden></p>
  <ul class="jobs" id="jobs"><li class="empty">Loading jobs&hellip;</li></ul>
  <p class="note" id="meta"></p>`;
}

// Runs in the browser. Built with DOM methods and textContent rather than HTML strings, so a
// job label can never turn into markup.
const dashboardScript = `
(function () {
  var list = document.getElementById('jobs');
  var notice = document.getElementById('notice');
  var meta = document.getElementById('meta');
  var timer = null;
  var UNFINISHED = ['waiting', 'prioritized', 'delayed', 'active'];
  var LABELS = { prioritized: 'waiting', delayed: 'retrying' };

  function el(tag, cls, text) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text != null) e.textContent = text;
    return e;
  }

  function seconds(ms) { return (ms / 1000).toFixed(1) + ' s'; }

  function mb(bytes) { return (bytes / 1048576).toFixed(1) + ' MB'; }

  function detail(job) {
    if (job.state === 'completed' && job.result) return job.result.summary || JSON.stringify(job.result);
    if (job.state === 'failed') return job.failedReason || 'Failed.';
    if (job.state === 'delayed') return (job.failedReason || 'The last attempt failed.') + ' Retrying after a backoff delay.';
    if (job.state === 'active') return 'Step ' + Math.round(job.progress / 100 * job.steps) + ' of ' + job.steps + ' done.';
    if (job.failedReason) return job.failedReason + ' Waiting for a worker to retry.';
    return 'Waiting for a worker.';
  }

  function render(data) {
    Object.keys(data.counts).forEach(function (state) {
      var node = document.querySelector('[data-count="' + state + '"]');
      if (node) node.textContent = data.counts[state] + (state === 'waiting' ? data.counts.prioritized || 0 : 0);
    });

    list.replaceChildren();
    if (!data.jobs.length) list.append(el('li', 'empty', 'No jobs yet. Enqueue one above.'));
    data.jobs.forEach(function (job) {
      var li = el('li', 'job');
      var row = el('div', 'row');
      row.append(el('span', 'badge st-' + job.state, LABELS[job.state] || job.state));
      row.append(el('strong', 'label', job.label));
      var info = '#' + job.id;
      if (job.attemptsStarted) info += ' \\u00b7 attempt ' + job.attemptsStarted + ' of ' + job.maxAttempts;
      if (job.finishedAt) info += ' \\u00b7 done ' + seconds(job.finishedAt - job.createdAt) + ' after queueing';
      row.append(el('span', 'meta', info));
      li.append(row);

      var bar = el('div', 'bar bar-' + job.state);
      var fill = el('i');
      fill.style.width = (job.state === 'completed' ? 100 : job.progress) + '%';
      bar.append(fill);
      li.append(bar);
      li.append(el('p', 'detail', detail(job)));
      list.append(li);
    });

    var parts = [];
    if (data.workers === 0) parts.push('No worker is connected, so jobs will wait until one starts.');
    else if (data.workers) parts.push(data.workers + (data.workers === 1 ? ' worker' : ' workers') + ' connected.');
    if (data.valkey && data.valkey.usedBytes) {
      parts.push('Valkey is using ' + mb(data.valkey.usedBytes) + (data.valkey.maxBytes ? ' of ' + mb(data.valkey.maxBytes) : '') + '.');
    }
    meta.textContent = parts.join(' ');
  }

  function say(text) {
    notice.textContent = text;
    notice.hidden = !text;
  }

  // A restart or deploy can briefly answer with an HTML error page instead of JSON.
  function readJson(res) {
    return res.json().catch(function () { return {}; }).then(function (data) {
      if (!res.ok) throw new Error(data.error || 'the server answered ' + res.status);
      return data;
    });
  }

  function refresh() {
    clearTimeout(timer);
    var busy = false;
    fetch('/api/jobs', { headers: { accept: 'application/json' } })
      .then(readJson)
      .then(function (data) {
        render(data);
        say('');
        busy = data.jobs.some(function (j) { return UNFINISHED.indexOf(j.state) >= 0; });
      })
      .catch(function (err) { say('Could not load jobs: ' + err.message); })
      .then(function () {
        // Poll quickly while something is running, slowly when idle, and not at all in a
        // background tab.
        if (!document.hidden) timer = setTimeout(refresh, busy ? 1000 : 5000);
      });
  }

  document.addEventListener('visibilitychange', function () { if (!document.hidden) refresh(); });

  document.getElementById('enqueue').addEventListener('submit', function (event) {
    event.preventDefault();
    var body = Object.fromEntries(new FormData(event.target));
    fetch('/api/jobs', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) })
      .then(readJson)
      .then(function () { say(''); refresh(); })
      .catch(function (err) { say('Could not enqueue: ' + err.message); });
  });

  refresh();
})();`;

function workerBody(stats, concurrency) {
  return `
  <h1>Worker</h1>
  <p class="sub">This instance runs jobs from the queue and serves no dashboard (<code>ROLE=worker</code>).</p>
  <ul class="facts">
    <li><b>${stats.active}</b> running now, up to ${concurrency} at a time</li>
    <li><b>${stats.completed}</b> completed and <b>${stats.failed}</b> failed attempts since this instance started</li>
  </ul>
  <p class="note">The dashboard lives in the web app, which shares the same Valkey.</p>`;
}

function setupBody() {
  return `
  <h1>Connect Valkey</h1>
  <p class="sub">This app keeps its job queue in Valkey, and none is bound to it yet.</p>
  <ol class="steps">
    <li><strong>Create a managed Valkey</strong> from the Caches page, or run
      <code>velixir cache create</code>.</li>
    <li><strong>Bind it to this app</strong> as <code>REDIS_URL</code>: on the cache's page
      under Bound apps, or <code>velixir cache bind &lt;cache-id&gt; --app &lt;app-id&gt;</code>.</li>
    <li><strong>Redeploy</strong> with the Redeploy button on the live release (Deploys tab).</li>
  </ol>
  <p class="note">Deploying this template from the velixir gallery does all three for you.</p>`;
}

function waitingBody(lastError) {
  return `
  <h1>Waiting for Valkey</h1>
  <p class="sub">The app is running and retrying the connection every few seconds.</p>
  ${lastError ? `<p class="note">Last error: <code>${escapeHtml(lastError)}</code></p>` : ''}
  <p class="note">If the cache password was rotated, redeploy the app to pick up the new one.</p>`;
}

function page(title, body, script = '') {
  return `<!doctype html>
<html lang="en">
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>${escapeHtml(title)}</title>
<style>
  :root { color-scheme: dark; }
  * { box-sizing: border-box; }
  body { margin:0; min-height:100vh; background:#0b0b10; color:#e8e8ef;
         font:15px/1.6 ui-sans-serif,system-ui,-apple-system,Segoe UI,sans-serif; }
  .wrap { max-width:44rem; margin:0 auto; padding:3rem 1.25rem 4rem; }
  h1 { font-size:1.5rem; margin:0 0 .25rem; letter-spacing:-.02em; }
  .sub { color:#7a7a8c; font-size:.8125rem; margin:0 0 2rem; }
  code { background:#16161f; border:1px solid #26263a; border-radius:5px;
         padding:.1rem .35rem; color:#c7d2fe; font-size:.8125rem; overflow-wrap:anywhere; }
  .counts { display:grid; grid-template-columns:repeat(5,1fr); gap:.5rem; margin-bottom:1.5rem; }
  .counts div { border:1px solid #1f1f2e; border-radius:10px; padding:.6rem .75rem;
                color:#7a7a8c; font-size:.75rem; }
  .counts b { display:block; color:#e8e8ef; font-size:1.25rem; font-variant-numeric:tabular-nums; }
  form.new { display:flex; flex-wrap:wrap; gap:.5rem; margin-bottom:1.5rem; }
  input[type=text], select { padding:.7rem .85rem; border-radius:8px; background:#13131c;
                             border:1px solid #26263a; color:#e8e8ef; font:inherit; }
  input[type=text] { flex:1 1 12rem; min-width:0; }
  select { flex:0 1 auto; max-width:100%; }
  input[type=text]:focus, select:focus { outline:none; border-color:#6366f1; }
  button { padding:.7rem 1.1rem; border-radius:8px; border:0; background:#6366f1;
           color:#fff; font:inherit; font-weight:600; cursor:pointer; }
  .notice { border:1px solid #5b2a2a; background:#1d1215; color:#fca5a5; border-radius:8px;
            padding:.6rem .85rem; font-size:.8125rem; }
  ul.jobs { list-style:none; margin:0; padding:0; }
  li.job { border:1px solid #1f1f2e; border-radius:10px; padding:.8rem 1rem; margin-bottom:.5rem; }
  li.empty { color:#4b4b5c; text-align:center; border:1px dashed #1f1f2e; border-radius:10px; padding:.875rem 1rem; }
  .row { display:flex; align-items:baseline; gap:.6rem; flex-wrap:wrap; }
  .label { overflow-wrap:anywhere; }
  .meta { color:#4b4b5c; font-size:.75rem; font-variant-numeric:tabular-nums; margin-left:auto; }
  .badge { font-size:.6875rem; font-weight:600; text-transform:uppercase; letter-spacing:.04em;
           border-radius:5px; padding:.1rem .45rem; background:#1f1f2e; color:#a1a1b5; }
  .st-active { background:rgba(99,102,241,.16); color:#a5b4fc; }
  .st-delayed { background:rgba(245,158,11,.14); color:#fcd34d; }
  .st-completed { background:rgba(34,197,94,.13); color:#86efac; }
  .st-failed { background:rgba(239,68,68,.14); color:#fca5a5; }
  .bar { height:4px; background:#1f1f2e; border-radius:2px; margin:.55rem 0 .4rem; overflow:hidden; }
  .bar i { display:block; height:100%; background:#6366f1; transition:width .4s ease; }
  .bar-completed i { background:#22c55e; }
  .bar-failed i { background:#ef4444; }
  .bar-delayed i { background:#f59e0b; }
  .detail { margin:0; color:#a1a1b5; font-size:.8125rem; overflow-wrap:anywhere; }
  ul.facts { padding-left:1.25rem; }
  ol.steps { padding-left:1.25rem; margin:0 0 1.5rem; }
  ol.steps li { margin-bottom:.9rem; }
  .note { color:#7a7a8c; font-size:.8125rem; }
  footer { color:#4b4b5c; font-size:.75rem; margin-top:2.5rem; text-align:center; }
  footer a { color:#7a7a8c; }
  @media (max-width:520px) { .counts { grid-template-columns:repeat(3,1fr); } }
</style>
<div class="wrap">
  ${body}
  <footer>
    <a href="/api/jobs">JSON API</a> &middot; running on <a href="https://velixir.net">velixir</a>
  </footer>
</div>
${script ? `<script>${script}</script>` : ''}
</html>`;
}

module.exports = { setupApp, dashboardApp, workerApp };
