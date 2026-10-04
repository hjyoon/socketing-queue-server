# socketing-queue-server

Go/Gin WebSocket queue service.

The queue worker retries Redis subscription and consumer-group initialization
with delays from 1 second up to 30 seconds. Stream read failures use the same
backoff, and a missing consumer group is recreated without skipping queued
requests. Initialization and processing failures are logged.

`/liveness` checks the HTTP server. `/readiness` also requires a working queue
consumer, Redis, and PostgreSQL. The `healthcheck` command uses `/readiness` so a
stalled queue worker is not reported as healthy.

Run `make test` and `make coverage` to validate changes. To include the Redis
integration tests, set `SOCKETING_TEST_REDIS_ADDR` to an isolated Redis instance
with no `queue-messages` stream in database 15. CI starts Redis and runs these
tests automatically.
