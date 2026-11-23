import crypto from 'node:crypto';
import { Redis } from 'ioredis';
import { Pool } from 'pg';
import { TransientErpError } from './errors.js';
import { LegacySoapErpConnector } from './legacy-soap-connector.js';
import { BidirectionalSyncEngine, type ExternalContactEvent } from './sync-engine.js';

const db = new Pool({
  connectionString: 'postgresql://sync_user:sync_password@localhost:5432/erp_sync',
});

const redis = new Redis('redis://localhost:6379');

function computeSeedHash(email: string, fullName: string, role: string): string {
  const canonical = JSON.stringify({
    email: email.trim().toLowerCase(),
    fullName: fullName.trim(),
    role: role.trim(),
  });
  return crypto.createHash('sha256').update(canonical).digest('hex');
}

/**
 * Simulates a total Redis outage so we can verify graceful PostgreSQL fallbacks.
 */
function createOfflineRedisProxy(realRedis: Redis): Redis {
  return new Proxy(realRedis, {
    get(_target, prop) {
      if (prop === 'quit' || prop === 'disconnect') return () => Promise.resolve('OK');
      return () => Promise.reject(new Error('ECONNREFUSED: Simulated Redis outage'));
    },
  });
}

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

    CREATE TABLE IF NOT EXISTS processed_events (
      provider TEXT NOT NULL,
      tenant_id TEXT NOT NULL,
      event_id TEXT NOT NULL,
      processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (provider, tenant_id, event_id)
    );

    CREATE TABLE IF NOT EXISTS oauth_connections (
      tenant_id TEXT NOT NULL,
      provider TEXT NOT NULL,
      access_token TEXT NOT NULL,
      refresh_token TEXT NOT NULL,
      expires_at TIMESTAMPTZ NOT NULL,
      PRIMARY KEY (tenant_id, provider)
    );
  `);

  await db.query(
    'TRUNCATE TABLE integration_mappings, outbox_events, processed_events, oauth_connections RESTART IDENTITY',
  );
  await redis.flushdb();

  // Seed realistic pre-existing synced records before any new webhooks arrive:
  // 1. contact_01: an unrelated existing contact that should remain untouched
  // 2. contact_99: Jane Doe's prior state ("Estimator") before her promotion ("Senior Estimator")
  const markHash = computeSeedHash('mark.vance@acmebuilders.com', 'Mark Vance', 'VP of Preconstruction');
  const janePreviousHash = computeSeedHash('jane.doe@acmebuilders.com', 'Jane Doe', 'Estimator');

  await db.query(
    `INSERT INTO integration_mappings (tenant_id, provider, external_id, payload_hash, updated_at)
     VALUES
       ('tenant_acme_builders', 'procore', 'contact_01', $1, NOW() - INTERVAL '3 days'),
       ('tenant_acme_builders', 'procore', 'contact_99', $2, NOW() - INTERVAL '1 day')`,
    [markHash, janePreviousHash],
  );

  // Seed an expired OAuth connection to test PostgreSQL FOR UPDATE fallback locking
  await db.query(
    `INSERT INTO oauth_connections (tenant_id, provider, access_token, refresh_token, expires_at)
     VALUES ('tenant_acme_builders', 'procore', 'expired_tok', 'seed_refresh_tok', NOW() - INTERVAL '10 minutes')`,
  );
}

async function runDemo(): Promise<void> {
  await bootstrapSchema();
  const engine = new BidirectionalSyncEngine(db, redis);

  console.log('\n--- 0. Pre-Existing Records Already in PostgreSQL ---');
  const initialMappings = await db.query(
    'SELECT tenant_id, provider, external_id, LEFT(payload_hash, 12) AS hash_prefix FROM integration_mappings ORDER BY external_id',
  );
  console.table(initialMappings.rows);

  const baseEvent: ExternalContactEvent = {
    eventId: 'evt_1001',
    tenantId: 'tenant_acme_builders',
    provider: 'procore',
    externalId: 'contact_99',
    updatedByServiceAccount: false,
    payload: {
      email: 'jane.doe@acmebuilders.com',
      fullName: 'Jane Doe',
      role: 'Senior Estimator', // Changed from seeded "Estimator" -> produces a new SHA-256 hash
    },
  };

  console.log('\n--- 1. Inbound Update to Existing Record (Role: Estimator -> Senior Estimator) ---');
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
        email: '  JANE.DOE@acmebuilders.com ',
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
    uniqueTokensIssued: uniqueTokens.size,
    token: [...uniqueTokens][0],
  });

  console.log('\n--- 6. Legacy SOAP/WSDL Anti-Corruption Layer (HTTP 200 Fault + Watermark Poll) ---');
  const soapConnector = new LegacySoapErpConnector(redis);
  try {
    await soapConnector.pushContact('tenant_acme_builders', {
      email: 'carlos.mendez@pacificbuilders.com',
      fullName: 'Carlos Mendez',
      role: 'Project Executive',
    });
  } catch (err) {
    if (err instanceof TransientErpError) {
      console.log('Caught HTTP 200 SOAP session expiry as TransientErpError -> retrying...');
      const retryResult = await soapConnector.pushContact('tenant_acme_builders', {
        email: 'carlos.mendez@pacificbuilders.com',
        fullName: 'Carlos Mendez',
        role: 'Project Executive',
      });
      console.log('SOAP Push succeeded on retry:', retryResult);
    }
  }

  const polledEvents = await soapConnector.pullUpdatedContacts('tenant_acme_builders', new Date());
  for (const polledEvent of polledEvents) {
    console.log('Synced watermark-polled SOAP record:', await engine.processInboundEvent(polledEvent));
  }

  console.log('\n--- 7. Simulated Redis Outage (PostgreSQL Fallbacks) ---');
  const offlineEngine = new BidirectionalSyncEngine(db, createOfflineRedisProxy(redis));
  const outageEvent: ExternalContactEvent = {
    eventId: 'evt_outage_900',
    tenantId: 'tenant_acme_builders',
    provider: 'viewpoint',
    externalId: 'contact_500',
    updatedByServiceAccount: false,
    payload: {
      email: 'ops@acme.com',
      fullName: 'Alex Rivera',
      role: 'Superintendent',
    },
  };

  console.log('First delivery during Redis outage:', await offlineEngine.processInboundEvent(outageEvent));
  console.log('Duplicate delivery during Redis outage:', await offlineEngine.processInboundEvent(outageEvent));

  const fallbackTokens = await Promise.all(
    Array.from({ length: 3 }, () => offlineEngine.getValidAccessToken('tenant_acme_builders', 'procore')),
  );
  console.log('Concurrent OAuth refresh via Postgres FOR UPDATE lock:', {
    workersCount: fallbackTokens.length,
    uniqueTokensIssued: new Set(fallbackTokens).size,
    token: fallbackTokens[0],
  });

  console.log('\n--- 8. Final PostgreSQL State (Mappings & Transactional Outbox) ---');
  const finalMappings = await db.query(
    'SELECT tenant_id, provider, external_id, LEFT(payload_hash, 12) AS hash_prefix FROM integration_mappings ORDER BY external_id',
  );
  console.table(finalMappings.rows);

  const outbox = await db.query('SELECT id, tenant_id, aggregate_id, event_type, status FROM outbox_events ORDER BY id');
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