// What the web side and the worker side have to agree on: the queue's name, how to reach
// Valkey, and what happens to a job after it runs. Split the worker into its own app later and
// both apps load this same file, so they cannot drift apart.

const IORedis = require('ioredis');

const QUEUE_NAME = 'jobs';

const defaultJobOptions = {
  // Three tries in total. A job that throws is retried after 2 s, then 4 s (exponential
  // backoff), which rides out the usual reasons a job fails: an API that was briefly down,
  // a rate limit, a deploy happening at the same moment.
  attempts: 3,
  backoff: { type: 'exponential', delay: 2000 },

  // Finished jobs stay in Valkey so the dashboard can show them, but only the newest few
  // hundred. Without a cap every job ever run stays in memory, and a cache that fills up
  // starts evicting keys, including the ones BullMQ needs.
  removeOnComplete: { count: 100 },
  removeOnFail: { count: 500 },
};

// REDIS_URL arrives as redis://default:pass@host:6379/0: plaintext inside velixir's private
// network, with the database number as the path. ioredis reads all of that from the URL.
//
// Each role gets its own connection because BullMQ wants different things from each:
//
//   - The worker parks a blocking command on Valkey while it waits for work. BullMQ refuses
//     to start unless maxRetriesPerRequest is null, so a command waits out a reconnect instead
//     of failing, and the worker simply resumes when Valkey is back.
//   - The web side should do the opposite. With the offline queue off, a request made while
//     Valkey is unreachable fails at once with an error the dashboard can show, instead of
//     hanging until the browser gives up.
function createConnection(url, { forWorker, onReady, onError }) {
  const connection = new IORedis(url, {
    maxRetriesPerRequest: forWorker ? null : 1,
    enableOfflineQueue: forWorker,
    // Keep retrying forever, at most every five seconds. A restarting or briefly unreachable
    // Valkey is a wait, not a reason to exit: exiting turns a blip into a crash loop.
    retryStrategy: (times) => Math.min(times * 500, 5000),
    // A connection can also die without closing: a lost node, a network partition. The socket
    // then looks healthy while every command waits forever, and the app never reconnects.
    // socketTimeout drops a connection that stops answering and starts a fresh one. The
    // worker's is longer than the 10 s BullMQ may legitimately wait for the next job.
    socketTimeout: forWorker ? 15_000 : 5_000,
  });

  // Without an error listener ioredis prints a stack trace for every failed reconnect.
  connection.on('error', (err) => onError?.(err));
  connection.on('ready', () => onReady?.());
  return connection;
}

module.exports = { QUEUE_NAME, defaultJobOptions, createConnection };
