# learn-redis-pubsub

A hands-on sandbox for learning **Redis Pub/Sub** properly — built as a small
real-time notification system with NestJS 10 + ioredis + TypeScript.

Nothing here is mocked. Every claim in this README was verified against a real
Redis from `docker-compose`.

The project deliberately mirrors the conventions of a production NestJS codebase:
TraceCodes instead of inline log strings, env-prefixed channel constants,
relative imports, controller → service layering, `strictNullChecks: false`.

---

## The one idea

> **Redis Pub/Sub is a message router with no memory.**

`PUBLISH` writes bytes into the sockets of whoever is subscribed *at that
instant*, returns how many it wrote to, and forgets. No key is created, no
offset stored, no retry, no acknowledgement that anyone *processed* anything.

That single fact explains everything else:

| | |
|---|---|
| Delivery guarantee | **at-most-once** |
| Stored in Redis | **nothing** (`DBSIZE` stays `0`) |
| Subscriber offline | message is **gone forever** |
| Retries | none — there are no acks |
| Needs idempotency? | **no** — and it can't have it (see Phase 5) |

Use it only when something *else* is authoritative — a poll, a cron, a DB read.
If you can't name that something else, use SQS.

---

## Quick start

```bash
docker compose up -d          # redis on :6379, container "learn-redis"
npm install
npm run start:local           # instance A on :3001
```

Then open <http://localhost:3001/learn-notifications/demo> and publish:

```bash
curl -s -X POST localhost:3001/learn-notifications/publish \
  -H 'Content-Type: application/json' \
  -d '{"userId":42,"title":"Your order shipped"}'
```

The browser updates instantly, with no refresh and no polling.

> **Docker not running?** `Cannot connect to the Docker daemon at .../docker.sock`
> means Docker Desktop is closed. `open -a Docker`, wait ~15s, retry.

### Scripts

| Script | What it does |
|---|---|
| `npm run start:local` | instance A, `:3001`, `NODE_ENV=local` |
| `npm run start:prod` | instance B, `:3002`, `NODE_ENV=prod` |
| `npm run start:misconfigured` | `:3003`, named PROD but `NODE_ENV=local` — the collision demo |
| `npm run redis:cli` | `redis-cli` inside the container |
| `npm run redis:spy` | `PSUBSCRIBE '*'` — see every message on the server |
| `npm run redis:channels` | `PUBSUB CHANNELS` — who is listening right now |

### Endpoints

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/learn-notifications/publish` | broadcast to the env-wide channel |
| `POST` | `/learn-notifications/publish-to-user` | targeted, arrives via the pattern sub |
| `GET` | `/learn-notifications/stream/:userId` | **SSE** stream (Redis-backed) |
| `GET` | `/learn-notifications/demo` | browser demo page |
| `GET` | `/learn-notifications/stats` | SSE connections vs Redis connections |
| `POST` | `/learn-notifications/in-memory/publish` | Phase 4: the **wrong** version |
| `GET` | `/learn-notifications/in-memory/stream/:userId` | Phase 4: the **wrong** version |
| `POST` | `/learn-notifications/broken-publish` | Phase 1: subscriber-mode error |

---

## Phase 0 — Fire-and-forget, at-most-once

No code. Two terminals and `redis-cli`.

```bash
# T1
docker exec -it learn-redis redis-cli SUBSCRIBE learn:demo
# T2
docker exec -it learn-redis redis-cli
> PUBLISH learn:demo "hello"
```

Then kill T1 and publish again. **Measured result:**

```
=== publish 3 with ZERO subscribers ===
0
0
0
=== start a fresh subscriber and wait ===
subscribe
learn:demo
1
--- keyspace ---
0
```

Three things:

1. **`PUBLISH` returns a routing receipt**, not an acknowledgement. It says "I
   wrote to N sockets." It is already stale by the time you read it.
2. **A late subscriber gets nothing.** Not delayed — never stored.
3. **`DBSIZE` is `0`.** Channel names are not keys. They exist only while a
   subscriber is attached, which is why you can't `KEYS` your way to debugging.

**Being connected isn't sufficient either.** Redis buffers output per client:

```
client-output-buffer-limit → pubsub 33554432 8388608 60
```

32 MB hard / 8 MB sustained over 60s. If your consumer's event loop stalls and
the publisher outruns it past those limits, **Redis disconnects the subscriber
to protect itself.** ioredis reconnects, resubscribes, and everything published
during the gap is gone — silently.

---

## Phase 1 — Two connections

A connection that issues `SUBSCRIBE` enters **subscriber mode**: the socket stops
being request/response and becomes a server-push stream. The client can no longer
tell whether an incoming frame is the reply to its `GET` or an unrelated
broadcast. So Redis only accepts `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`,
`PUNSUBSCRIBE`, `PING`, `QUIT`, `RESET` on it.

**This is forced by the protocol, not a style choice.** Hit it yourself:

```bash
curl -s -X POST localhost:3001/learn-notifications/broken-publish
# {"error":"Connection in subscriber mode, only subscriber commands may be used"}
```

Note that ioredis produced that **client-side**. The command never reached Redis.

### Lifecycle

- **`app.enableShutdownHooks()`** in `main.ts` — without it Nest never calls
  `onModuleDestroy` and the process exits with Redis connections open.
- **`isShuttingDown`** (`src/shutdown.state.ts`) — on SIGTERM, Kubernetes starts a
  ~30s timer before SIGKILL. The pod is still subscribed until `quit()` lands, so
  Redis keeps pushing. Starting work now risks being killed halfway through it.
  At-most-once already permits a drop, so **drop cleanly** rather than
  half-process. The flag is set *before* Nest tears modules down.
- **An `'error'` listener on every ioredis client is mandatory.** An ioredis
  client is an `EventEmitter`; an `'error'` event with no listener becomes an
  uncaught exception and kills the process. Losing your API because Redis
  blinked — on a best-effort push channel — is a terrible trade.
- **Register the `'message'` handler *before* calling `subscribe()`**, or there's
  a window where the subscription is live and nothing is listening.

---

## Phase 2 — Channel design

Channel names live in a **flat global namespace**. There are no folders, and
`SELECT 1` doesn't help either — **pub/sub ignores Redis's numbered databases
entirely.** One namespace per server.

So channels are env-scoped (`src/learn-notifications/constants/`):

```ts
const CHANNEL_PREFIX = `${config.env}:learn-notifications`;

export const LEARN_NOTIFICATION_CHANNELS = {
  ALL: `${CHANNEL_PREFIX}:all`,
  USER: (userId: number): string => `${CHANNEL_PREFIX}:user:${userId}`,
  USER_PATTERN: `${CHANNEL_PREFIX}:user:*`,
};
```

**Measured — two instances, one Redis:**

```
local:learn-notifications:all
prod:learn-notifications:all
```

LOCAL publishes → `receivers: 1`. PROD's received count: `0`.

### The collision

`npm run start:misconfigured` boots a pod named PROD with `NODE_ENV=local` — one
wrong env var, the most common deploy mistake there is:

```
{"receivers":2,"publishedBy":"LOCAL","env":"local"}
[PROD_MISCONFIGURED] LEARN_NOTIFICATION_RECEIVED {"title":"dev deleting test data"}
```

`receivers` went from 1 to **2**. Why this is worse than a cache collision:

- **No audit trail** — nothing is stored, the evidence is gone on delivery
- **The publisher can't tell** — `receivers` is the only signal, and nobody reads it
- **It can't be undone** — no DLQ, no replay, no unsend
- **It fails open** — a wrong prefix reaches *more* listeners, not fewer

> A prefix is a **convention, not a permission**. It stops accidents. It does not
> stop anyone with Redis credentials — `npm run redis:spy` sees everything.

### SUBSCRIBE vs PSUBSCRIBE

| | `SUBSCRIBE` | `PSUBSCRIBE` |
|---|---|---|
| Takes | exact names | glob pattern |
| ioredis event | `'message'` | `'pmessage'` |
| Handler args | `(channel, payload)` | `(pattern, channel, payload)` |
| Matching | hash lookup, O(1) | tested against every pattern, O(N) |

**Different events.** Wire only `'message'` then `psubscribe` and you receive
nothing, with no error.

**Three traps:**

1. **Overlap delivers twice.** Hold `SUBSCRIBE demo:user:42` *and*
   `PSUBSCRIBE demo:user:*`, publish once → `(integer) 2`, handler runs twice.
   This quietly converts at-most-once into at-least-once **without** message IDs
   or dedupe tooling. You get the duplicates and none of the defences.
2. **Greedy patterns swallow other features.** `PSUBSCRIBE demo:*` catches
   `demo:payments:refund-webhook`. It breaks the day a colleague adds a channel
   under a prefix they didn't know you'd claimed.
3. **O(N) matching** on the single-threaded event loop serving everything else.

> **Use `SUBSCRIBE` for delivery. Use `PSUBSCRIBE` only for observation** —
> debugging, monitoring, dev tools — where duplicates don't matter.

---

## Phase 3 — SSE fan-out

**SSE** = the browser makes one ordinary GET, and the server never finishes the
response:

```
id: 1
data: {"userId":42,"title":"Your order shipped","publishedBy":"LOCAL"}
```

Plain HTTP, one direction, built-in browser client (`EventSource`) that
auto-reconnects. No WebSocket upgrade.

### The connection math

The naive design — one Redis subscription per connected browser — dies on
arithmetic. `maxclients` on a default Redis is **10000**, shared by everything.

10,000 users × 5 pods:

| | Naive | Subject (this repo) |
|---|---|---|
| Redis connections | **10,000** | **10** (2 per pod) |
| vs `maxclients 10000` | **at the ceiling** | 0.1% |
| New browser connects | `SUBSCRIBE` round-trip | a closure, zero I/O |

Worse than the headline:

1. **`maxclients` is shared.** At the ceiling, caching, `RedisMutex` and crons
   fail with `ERR max number of clients reached` — on code paths unrelated to
   notifications. One feature takes out everything touching Redis.
2. **Redis is single-threaded.** Fanning out to 10,000 sockets is latency
   charged to every other query.
3. **Connection churn.** Browsers reconnect constantly (tab sleep, wifi, lid).
   Each one is a `SUBSCRIBE`/`UNSUBSCRIBE` round-trip in the naive design.

### The fix

```
Redis ──1 subscription──▶ [ pod ] ──RxJS Subject──▶ N SSE connections
```

**Measured:**

```
3 SSE streams open:   {"activeSseStreams":3,"redisSubscriberConnections":1}
Redis:                connected_clients:3     ← unchanged
one publish:          {"receivers":1}
streams that got it:  3
after disconnect:     {"activeSseStreams":0}  ← no leak
```

> `receivers: 1` with 3 browsers watching. That number counts **Redis**
> subscribers, not humans. In production it should track your *pod* count — if
> it tracks your *user* count, you've built the naive design.

**The teardown is load-bearing:**

```ts
return () => { sub.unsubscribe(); this.activeStreams -= 1; };
```

Omit it and the Subject holds a reference to every browser that ever connected.
It behaves perfectly — until the pod OOMs after a week of uptime.

**An Observable is a recipe, not a running thing.** Its body runs on
`subscribe()`, not on creation. Counting a stream in the enclosing method instead
of inside the Observable is an off-by-N bug — one this repo shipped and fixed.

### Still lossy

SSE doesn't fix at-most-once, it adds a second lossy layer:

```
publisher → Redis → pod → Subject → SSE → browser
                ↑        ↑              ↑
            no retry  drops on      reconnect
                      shutdown        gap
```

`EventSource` auto-reconnects and **silently misses everything published during
the gap**. SSE is an accelerator on top of an authoritative poll, never a
replacement for one.

---

## Phase 4 — Multi-process (the whole point)

`src/learn-notifications/in-memory-notifications.service.ts` is the **wrong**
implementation, kept on purpose. Identical from the SSE layer down — same
Subject, same filter — except `publish()` calls `local$.next()` directly instead
of going through Redis.

**On one process the two are indistinguishable.** Every test passes.

Two instances, same env:

```
=== REDIS path: publish on A, does B receive? ===
{"receivers":2,"publishedBy":"A"}
[B] LEARN_NOTIFICATION_RECEIVED {"title":"cross-process via redis","publishedBy":"A","lagMs":3}

=== IN-MEMORY path: publish on A, does B receive? ===
{"localStreams":0,"publishedBy":"A"}
0          ← B received nothing
```

The in-memory publish still returns **200**. No error, no warning.

This maps directly onto a codebase that deploys one repo as several processes
(API, cron, internal, and priority workers). A verdict computed on a worker pod
has to reach an SSE connection held by an API pod. An `EventEmitter` works
perfectly in dev and on a single pod, then silently serves stale UI to a
fraction of users proportional to your pod count. **Redis is the thing that
crosses the process boundary.**

---

## Phase 5 — Where pub/sub is the WRONG tool

Kill the subscriber, publish 10, restart:

```
PUBSUB CHANNELS → (empty)
publish ×10     → 0 0 0 0 0 0 0 0 0 0
restart, log:   [LOCAL] LEARN_NOTIFICATION_PUBSUB_INIT {...}
```

One line. No gap counter, no warning. **The process has no concept that anything
happened while it was gone, and no API to ask.**

### Same outage, Redis Streams

```
XADD ×10 with nobody consuming → XLEN 10, DBSIZE 1
XRANGE demo:stream - +         → all 10 still there
```

`XADD` writes a **key**; `PUBLISH` writes a **socket**. `DBSIZE` going `0 → 1` is
the entire difference. Entries get IDs (`1791909759748-0`), which is what lets a
consumer say "everything after X."

Consumer groups give you the SQS-like primitive:

```
XREADGROUP → claimed, now "pending"
XPENDING   → 1791909759748-0  worker-1  8069ms  delivery-count 1
XAUTOCLAIM → worker-2 takes over after worker-1 dies
XACK       → done
```

That delivery count becoming `2` is **at-least-once**, and it's the price of
never losing anything.

### Why pub/sub can never be a job queue

Idempotency needs a **stable message identity** and a **record of what's done**.
Pub/sub breaks both:

1. **No message ID.** Streams have `1791909759748-0`, SQS has `MessageId`.
   Pub/sub gives you a byte string. Adding your own UUID deduplicates
   redeliveries that *never happen*.
2. **No acks, so no retries.** The fatal one:

| | Pub/Sub | Streams / SQS |
|---|---|---|
| Handler throws | **nothing happens** | stays pending → redelivered |
| Consumer dies mid-work | **nothing happens** | timeout → redelivered |
| Handler succeeds | nothing happens | `XACK` / `DeleteMessage` |

> Pub/sub is at-most-once, so it never *needs* idempotency. It's at-most-once
> because it has no acks — which is why it can't retry. No retry means no
> delivery guarantee, and a job queue is nothing but a delivery guarantee.

Adding dedupe to pub/sub is installing a smoke detector in a building with no
fire exits.

### The decision boundary

Ask: **if this message vanishes, does anyone notice?**

| | **Pub/Sub** | **Redis Streams** | **SQS** | **AmazonMQ** |
|---|---|---|---|---|
| Delivery | at-most-once | at-least-once | at-least-once | at-least-once |
| Survives downtime | ❌ | ✅ | ✅ | ✅ |
| Acks / retries | ❌ | ✅ `XACK` | ✅ visibility timeout | ✅ ack/nack |
| Dead-letter | ❌ | manual | ✅ native | ✅ native |
| Fan-out to N | ✅ native | ✅ groups | ❌ (needs SNS) | ✅ topics |
| Latency | **sub-ms** | ~ms | 10–100ms | ~ms |
| Ops cost | already there | already there | managed | **you run a broker** |

- **Pub/Sub** — live UI pushes, cache invalidation hints, presence. Loss is
  acceptable *by design*.
- **Streams** — durability without a new system. Catch: **memory-resident**, so
  cap it (`XADD ... MAXLEN ~ 10000`). Nobody pages you for a deep SQS queue.
- **SQS** — the default for anything that must not be lost.
- **AmazonMQ** — real broker semantics (topic exchanges, AMQP/MQTT). You're
  operating a broker; pick it for a reason you can name.

---

## Phase 6 — Mental model

**Pub/sub vs RedisMutex — same Redis, opposite primitives:**

| | Pub/Sub | RedisMutex (`SET key val NX PX`) |
|---|---|---|
| Stores | **nothing** | a key with a TTL |
| Question | "who's listening?" | "who goes first?" |
| Delivers to | everyone | **exactly one** |

Mutex details worth stealing: release via **Lua** (`if get(key) == myId then del`)
so you never delete a lock that expired and was re-acquired by someone else —
get-then-delete isn't atomic. And the **TTL is the safety net**: without it, one
crash deadlocks the key forever.

### The four lines

- **Pub/sub stores nothing.** `DBSIZE 0`, always.
- **`receivers` is a routing receipt**, not an acknowledgement.
- **At-most-once** means a drop is normal, not exceptional.
- **So it's only correct when something else is authoritative.**

### Three questions you should answer cold

1. **Why two Redis connections?**
   A connection that issues `SUBSCRIBE` enters subscriber mode; the protocol
   becomes server-push, so the client can't match replies to commands. Redis
   accepts only subscribe-family commands on it.

2. **Can you make Redis pub/sub idempotent?**
   Wrong question. It's at-most-once — there are no redeliveries to dedupe. It
   can't be made *reliable*, because no acks means no retries. Idempotency
   protects against redelivery; pub/sub's problem is non-delivery.

3. **One Redis subscription per connected user — what breaks?**
   `maxclients` (10,000, shared). Caching, locks and crons start failing with
   `ERR max number of clients reached`. Fix: one subscription per pod, fan out
   in-process with an RxJS Subject.

---

## Project layout

```
config/config.ts                     env, instance name, redis host/port
src/main.ts                          enableShutdownHooks + SIGTERM → markShuttingDown
src/shutdown.state.ts                isShuttingDown flag
src/logging-service/
  constants/tracecodes.ts            no inline log strings
  logging.service.ts                 logInfo / logError (traceCode first)
src/learn-notifications/
  constants/learn-notifications.constants.ts   env-prefixed channels
  constants/sse-demo-page.constant.ts          browser demo page
  dtos/publish-notification.dto.ts             class-validator DTO
  interfaces/notification.interface.ts
  learn-notifications.service.ts               Redis pub/sub + RxJS Subject
  in-memory-notifications.service.ts           Phase 4: the wrong version
  learn-notifications.controller.ts            publish + SSE endpoints
```

## Stack

NestJS 10.3 · ioredis 5.3 · RxJS 7.8 · TypeScript 5.1 (CommonJS, ES2021,
`strictNullChecks: false`) · Redis 8.8 via Docker
