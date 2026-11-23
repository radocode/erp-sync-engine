# Bidirectional ERP Sync Engine

A fault-tolerant TypeScript reference architecture demonstrating bidirectional synchronization between a SaaS CRM/proposal platform and rate-limited external ERPs (such as Procore, Trimble Viewpoint, or Sage 300 CRE).

## Why This Exists

Naive webhook-and-REST integrations between a CRM and external ERPs break under real production load due to five recurring failure modes:

1. **Infinite Webhook Loops:** Outbound CRM updates trigger inbound ERP webhooks, creating endless write cycles.
2. **Duplicate Queue Deliveries:** At-least-once message brokers (e.g., AWS SQS) and webhook retries redeliver identical events during network blips.
3. **API Rate-Limit Bans (`HTTP 429`):** Legacy construction ERPs enforce strict per-tenant quotas (e.g., 60 requests/minute) and throttle burst traffic.
4. **OAuth Refresh Token Race Conditions:** Concurrent workers refreshing an expired single-use OAuth refresh token simultaneously trigger `invalid_grant` errors and disconnect tenants.
5. **Database-to-Queue Drift:** Process crashes between committing a database record and publishing a queue message leave downstream consumers permanently out of sync.

This engine implements deterministic guards in TypeScript, PostgreSQL, and Redis to prevent all five failure modes.

---

## Architecture Overview

```text
  External ERP Webhook / Poller
               │
               ▼
  ┌─────────────────────────┐
  │ 1. Origin Guard         │ ──► Drops self-originated service account echoes
  └────────────┬────────────┘
               ▼
  ┌─────────────────────────┐
  │ 2. Redis Idempotency    │ ──► SET NX per (provider, tenant, eventId)
  └────────────┬────────────┘
               ▼
  ┌─────────────────────────┐
  │ 3. Tenant Rate Limiter  │ ──► Per-tenant sliding window + jittered retry
  └────────────┬────────────┘
               ▼
  ┌─────────────────────────┐
  │ 4. SHA-256 Hash Guard   │ ──► Canonical field normalization; skips no-ops
  └────────────┬────────────┘
               ▼
  ┌─────────────────────────┐
  │ 5. PostgreSQL ACID Tx   │ ──► Upserts mapping + writes outbox_events row
  └─────────────────────────┘

  Concurrent Workers ──► [ Redis SET NX OAuth Lock ] ──► Single Token Refresh
```

---

## Core Patterns & Repository Layout

```text
erp-sync-engine/
├── docker-compose.yml        # PostgreSQL 16 + Redis 7 with readiness healthchecks
├── package.json              # One-command orchestration scripts
├── tsconfig.json             # Strict TypeScript configuration (NodeNext)
└── src/
    ├── rate-limiter.ts       # Per-tenant Redis rate limiter with jittered backoff
    ├── sync-engine.ts        # Origin guard, idempotency, SHA-256 loop guard, OAuth lock & Outbox
    └── demo.ts               # End-to-end simulation covering all 6 failure scenarios
```

| Pattern | Implementation | Problem Solved |
| :--- | :--- | :--- |
| **Origin Tagging** | `src/sync-engine.ts` | Immediately drops inbound webhook echoes triggered by our own integration service account. |
| **Idempotency Keys** | `Redis SET ... NX EX` | Deduplicates repeated webhook or SQS redeliveries per `(provider, tenantId, eventId)`. |
| **Per-Tenant Rate Limiting** | `src/rate-limiter.ts` | Tracks per-tenant API quotas in Redis and returns `deferred` outcomes with randomized jitter to prevent `429` cascades. |
| **Normalized SHA-256 Hashing** | `src/sync-engine.ts` | Canonicalizes mapped fields (trimming whitespace, lowercasing emails) and compares hashes against PostgreSQL `FOR UPDATE` rows to skip no-op webhooks. |
| **Distributed OAuth Lock** | `src/sync-engine.ts` | Uses a short-lived Redis lock (`SET oauth:lock... NX PX 5000`) so parallel workers coordinate around a single OAuth token refresh. |
| **Transactional Outbox** | PostgreSQL `BEGIN/COMMIT` | Atomically upserts `integration_mappings` and inserts `PENDING` events into `outbox_events` in one ACID transaction. |

---

## Quick Start

### Prerequisites
* **Node.js** 20+
* **Docker** & **Docker Compose**

### Run the Entire Simulation in One Command

```bash
npm install
npm start
```

`npm start` boots PostgreSQL and Redis via Docker Compose, waits for both healthchecks to pass, runs strict TypeScript type-checking (`tsc --noEmit`), and executes the full simulation (`src/demo.ts`).

### Additional Scripts

* `npm run demo:ephemeral` — Runs the full stack and automatically tears down the Docker containers when finished.
* `npm run demo` — Runs the TypeScript simulation against already-running containers.
* `npm run typecheck` — Verifies strict TypeScript compilation without emitting files.
* `npm stop` — Stops and removes the local PostgreSQL and Redis containers.

---

## What the Simulation Verifies

Running `npm start` executes six automated scenarios in sequence:

1. **Initial Valid Inbound Event:** Normalizes contact fields, computes a `SHA-256` fingerprint, persists the mapping, and writes a `PENDING` outbox event (`status: 'synced'`).
2. **Duplicate SQS Redelivery:** Replays the exact same `eventId` and blocks duplicate processing via Redis `SET NX` (`reason: 'duplicate_event'`).
3. **Self-Origin Echo Webhook:** Simulates an echo from the integration's own service account and drops it (`reason: 'self_origin'`).
4. **No-Op Payload Webhook:** Sends an event with altered casing/whitespace (`"  JANE.DOE@acmebuilders.com "`) that resolves to the same normalized `SHA-256` hash and skips the write (`reason: 'unchanged_hash'`).
5. **Concurrent Rotating OAuth Refresh:** Fires 5 parallel workers requesting an uncached access token simultaneously, verifying that the distributed Redis lock issues exactly `1` refresh token across all 5 workers.
6. **PostgreSQL Transactional Outbox Check:** Queries `outbox_events` to confirm atomic persistence alongside the mapping upsert.