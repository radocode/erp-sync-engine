# Bidirectional ERP Sync Engine

A reference TypeScript implementation demonstrating fault-tolerant, bidirectional synchronization between a SaaS CRM/proposal platform and rate-limited external ERPs (such as Procore, Viewpoint, or Sage 300 CRE).

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