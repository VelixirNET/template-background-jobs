# Background jobs

[![Deploy on velixir](https://velixir.net/img/deploy-on-velixir.svg)](https://velixir.net/new?template=background-jobs)

Answer every request straight away and do the slow part in the background. A dashboard hands
work to a queue in managed Valkey, and a worker runs it with live progress, retries and a
tidy history. Node, Express and BullMQ, in one process to start with.

[Deploy it on velixir](https://velixir.net/new?template=background-jobs).

The example job is a stand-in that counts through a few one-second steps. Swap in your own
(sending email through an HTTPS API, resizing images, calling a slow third-party service) and
everything around it stays the same.

## What it shows

- **A queue that outlives the process.** Jobs, results and history live in Valkey. Deploy,
  restart or crash, and they are all still there when the app comes back.
- **Progress you can watch.** The worker reports progress as it goes, and the dashboard
  refreshes itself from a JSON API you can use too.
- **Retries with exponential backoff.** A job that throws is tried three times, 2 s and then
  4 s apart. A failure that retrying cannot fix fails at once instead. The form lets you pick
  either path, and a job that fails once and then succeeds.
- **Memory that stays bounded.** Only the newest 100 completed and 500 failed jobs are kept,
  and the form refuses new work past about 200 unfinished jobs, so Valkey never slowly fills
  up with history.
- **Graceful shutdown.** On a deploy or restart the worker stops taking jobs and finishes the
  ones it is running (for up to 25 seconds) before it exits. A job cut off anyway is handed
  out again on its own.
- **One process now, two apps later.** `ROLE` switches the same code between the dashboard,
  the worker, or both. See [splitting the worker](#splitting-the-worker-into-its-own-app).
- **A setup page instead of a crash.** Start it without Valkey and it serves a page saying
  exactly what to do. Lose Valkey later and it retries, explains the wait, and recovers by
  itself, including from a connection that dies without closing.

## Setting it up on velixir

Deploy it from the gallery. velixir creates a managed Valkey, binds it to the app as
`REDIS_URL`, and holds the first deploy until the cache is running, so the app starts
connected.

**Choose an always-on plan** (Hobby or above). The Free plan sleeps after 15 minutes without
web traffic, and a sleeping worker runs nothing: jobs would wait in Valkey until the next
visit to the dashboard woke it.

By hand, for example after deploying your own copy with `velixir deploy`:

1. Create a managed Valkey with **New cache** on the project page, or run
   `velixir cache create`.
2. Bind it to the app as `REDIS_URL`: on the cache's page under Bound apps, or
   `velixir cache bind <cache-id> --app <app-id>`.
3. Redeploy, with the Redeploy button on the live release in the Deploys tab.

## Running it locally

```bash
docker run -d --name valkey -p 6379:6379 valkey/valkey:8
npm install
REDIS_URL=redis://localhost:6379 npm start
```

Then open http://localhost:8080.

## Deploying

```bash
velixir deploy
```

## Making it yours

- **The job** is `processJob` in `processor.js`. Whatever it returns is stored as the result;
  whatever it throws is a failure, retried until its attempts run out. Throw
  `UnrecoverableError` for failures that retrying will not fix.
- **Enqueueing** is the `queue.add(...)` call in `web.js`. Job data is stored as JSON in
  Valkey, so pass ids and parameters, not whole files.
- **Retries and retention** are `defaultJobOptions` in `queue.js`.

| Variable | Default | Meaning |
| --- | --- | --- |
| `REDIS_URL` | set by velixir | The Valkey to use. Without it the app serves its setup page. |
| `ROLE` | `all` | `web` (dashboard and API only), `worker` (jobs only), or `all` |
| `WORKER_CONCURRENCY` | `2` | How many jobs one process runs at the same time |

## Splitting the worker into its own app

One process is the right place to start. Split it when jobs begin to slow the dashboard down,
or when you want more workers without more web servers. Nothing in the code changes: the same
repository is deployed twice with a different `ROLE`.

1. **Create a second app** in the same project, for example `my-jobs-worker`.
2. **Bind the same Valkey** to it as `REDIS_URL` (the cache's page, under Bound apps).
3. **Set `ROLE`** on each app's Environment tab: `worker` on the new app, `web` on the
   original one.
4. **Deploy the same code to both.** From your checkout, `velixir deploy` updates the app it
   is linked to, and `velixir deploy --app <worker-app-id>` updates the worker.

Put the worker on an always-on plan. The web app only enqueues and reads, so it can live on
any plan, including Free: if it sleeps, the next visit wakes it and nothing is lost. The
dashboard shows how many workers are connected, and says so plainly when there are none.

Each replica of the worker app is another worker on the same queue, and BullMQ gives every job
to exactly one of them. When you change the shape of a job's data, deploy the worker first, so
it understands the jobs the web side is about to send.

## Things to know

**Jobs can run more than once.** A process that dies mid-job (out of memory, or a job still
running when the 25 second shutdown window closes) leaves the job locked in Valkey. About 30
seconds later BullMQ notices and hands it out again. Write jobs so that running twice is
harmless: check whether the email was already sent, upsert rather than insert.

**Long jobs and deploys.** A deploy waits up to 25 seconds for running jobs. Longer work gets
cut off and retried from the start, so split it into smaller jobs, or save progress as you go
and pick up from there.

**The smallest Valkey plan is not persistent.** It keeps everything in memory, so if the cache
restarts, waiting jobs and history are gone. For work you cannot afford to lose, use a cache
plan with persistence, or keep the source of truth in Postgres and use the queue to schedule
work on it.

**BullMQ warns about eviction on startup.** The logs show `IMPORTANT! Eviction policy is
allkeys-lru. It should be "noeviction"`. velixir's managed Valkey is a cache first: when it
reaches its memory limit it drops the least recently used keys, which could include a queued
job. The retention limits above keep this template at a couple of megabytes, far below any
plan's limit. Keep job data small and do not share the cache with a heavy caching workload,
and the warning stays theoretical.

**The dashboard has no login.** Anyone with the URL can read job names and enqueue the demo
job. That is fine for a demo, and the backlog cap limits the damage, but put it behind
authentication (or remove the form and the POST routes) before real work goes through it.

## API

| Method | Path | Does |
| --- | --- | --- |
| `GET` | `/` | The dashboard |
| `GET` | `/api/jobs` | Job counts, the newest 25 jobs, connected workers and Valkey memory use |
| `GET` | `/api/jobs/:id` | One job |
| `POST` | `/api/jobs` | Enqueue one, `{"label": "...", "steps": 5, "outcome": "succeed"}` |
| `GET` | `/healthz` | 200 when Valkey answers (or none is bound yet), 503 while it is unreachable |

`outcome` is `succeed`, `fail-once`, `fail-always` or `fail-permanently`; `steps` is 1 to 10.

## Licence

MIT.
