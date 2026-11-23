import { Redis } from 'ioredis';
import { Pool } from 'pg';
import { BidirectionalSyncEngine, type ExternalContactEvent } from './sync-engine.js';

const db = new Pool({
  connectionString: 'postgresql://sync_user:sync_password@localhost:5432/erp_sync',
});

const redis = new Redis('redis://localhost:6379');

async function bootstrapSchema(): Promise<void> {
  await db.query(`
    CREATE TABLE IF NOT EXISTS integration_mappings (
      tenant_id TEXT NOT NULL,
      provider TEXT NOT NULL,
      external_id TEXT NOT NULL,
      payload_hash TEXT NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (tenant_id, provider, external_id)
    );

    CREATE TABLE IF NOT EXISTS outbox_events (
      id SERIAL PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      aggregate_id TEXT NOT NULL,
      event_type TEXT NOT NULL,
      payload JSONB NOT NULL,
      status TEXT NOT NULL DEFAULT 'PENDING',
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  // Reset state so the demo can be re-run cleanly anytime
  await db.query('TRUNCATE TABLE integration_mappings, outbox_events RESTART IDENTITY');
  await redis.flushdb();
}

async function runDemo(): Promise<void> {
  await bootstrapSchema();
  const engine = new BidirectionalSyncEngine(db, redis);

  const baseEvent: ExternalContactEvent = {
    eventId: 'evt_1001',
    tenantId: 'tenant_acme_builders',
    provider: 'procore',
    externalId: 'contact_99',
    updatedByServiceAccount: false,
    payload: {
      email: 'jane.doe@acmebuilders.com',
      fullName: 'Jane Doe',
      role: 'Senior Estimator',
    },
  };

  console.log('\n--- 1. Initial Valid Inbound Event ---');
  console.log(await engine.processInboundEvent(baseEvent));

  console.log('\n--- 2. Duplicate SQS Redelivery (Same eventId) ---');
  console.log(await engine.processInboundEvent(baseEvent));

  console.log('\n--- 3. Echo Webhook From Our Own Integration Service Account ---');
  console.log(
    await engine.processInboundEvent({
      ...baseEvent,
      eventId: 'evt_1002',
      updatedByServiceAccount: true,
    }),
  );

  console.log('\n--- 4. No-Op Webhook With Unchanged Normalized Payload ---');
  console.log(
    await engine.processInboundEvent({
      ...baseEvent,
      eventId: 'evt_1003',
      payload: {
        email: '  JANE.DOE@acmebuilders.com ', // Normalized to identical SHA-256 hash
        fullName: 'Jane Doe',
        role: 'Senior Estimator',
      },
    }),
  );

  console.log('\n--- 5. Concurrent Rotating OAuth Token Refresh (5 Parallel Workers) ---');
  const tokens = await Promise.all(
    Array.from({ length: 5 }, () => engine.getValidAccessToken('tenant_acme_builders', 'procore')),
  );
  const uniqueTokens = new Set(tokens);
  console.log({
    workersCount: tokens.length,
    uniqueTokensIssued: uniqueTokens.size, // Must be 1 (no race-condition invalidation)
    token: [...uniqueTokens][0],
  });

  console.log('\n--- 6. PostgreSQL Transactional Outbox Verification ---');
  const outbox = await db.query('SELECT id, tenant_id, aggregate_id, event_type, status FROM outbox_events');
  console.table(outbox.rows);
}

runDemo()
  .catch((err) => {
    console.error('Demo failed:', err);
    process.exitCode = 1;
  })
  .finally(async () => {
    await redis.quit();
    await db.end();
  });