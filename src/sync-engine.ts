import crypto from 'node:crypto';
import { Redis } from 'ioredis';
import { Pool } from 'pg';
import { TenantRateLimiter } from './rate-limiter.js';

export interface ExternalContactEvent {
  readonly eventId: string;
  readonly tenantId: string;
  readonly provider: 'procore' | 'viewpoint' | 'legacy_soap_erp';
  readonly externalId: string;
  readonly updatedByServiceAccount: boolean;
  readonly payload: {
    readonly email: string;
    readonly fullName: string;
    readonly role: string;
  };
}

export type SyncOutcome =
  | { status: 'synced'; hash: string; storageMode: 'redis+postgres' | 'postgres_fallback' }
  | { status: 'skipped'; reason: 'self_origin' | 'duplicate_event' | 'unchanged_hash' }
  | { status: 'deferred'; retryAfterMs: number };

export class BidirectionalSyncEngine {
  private readonly rateLimiter: TenantRateLimiter;

  constructor(
    private readonly db: Pool,
    private readonly redis: Redis,
  ) {
    this.rateLimiter = new TenantRateLimiter(redis);
  }

  private computeNormalizedHash(payload: ExternalContactEvent['payload']): string {
    const canonical = JSON.stringify({
      email: payload.email.trim().toLowerCase(),
      fullName: payload.fullName.trim(),
      role: payload.role.trim(),
    });
    return crypto.createHash('sha256').update(canonical).digest('hex');
  }

  /**
   * Prevents concurrent workers from invalidating single-use rotating OAuth refresh tokens.
   * Fast path: Redis SET NX distributed lock.
   * Outage fallback: PostgreSQL row-level lock (SELECT ... FOR UPDATE) on oauth_connections.
   */
  async getValidAccessToken(tenantId: string, provider: string): Promise<string> {
    const tokenKey = `oauth:token:${provider}:${tenantId}`;
    const lockKey = `oauth:lock:${provider}:${tenantId}`;

    try {
      const cached = await this.redis.get(tokenKey);
      if (cached) return cached;

      const acquired = await this.redis.set(lockKey, 'locked', 'PX', 5000, 'NX');
      if (!acquired) {
        await new Promise((r) => setTimeout(r, 150));
        return this.getValidAccessToken(tenantId, provider);
      }

      try {
        const freshToken = `tok_${crypto.randomBytes(8).toString('hex')}`;
        await this.redis.set(tokenKey, freshToken, 'EX', 3500);
        return freshToken;
      } finally {
        await this.redis.del(lockKey);
      }
    } catch {
      return this.refreshOAuthTokenWithDbLock(tenantId, provider);
    }
  }

  private async refreshOAuthTokenWithDbLock(tenantId: string, provider: string): Promise<string> {
    const client = await this.db.connect();
    try {
      await client.query('BEGIN');

      const row = await client.query<{ access_token: string; is_fresh: boolean }>(
        `SELECT access_token, (expires_at > NOW() + INTERVAL '5 minutes') AS is_fresh
         FROM oauth_connections
         WHERE tenant_id = $1 AND provider = $2
         FOR UPDATE`,
        [tenantId, provider],
      );

      const existing = row.rows[0];
      if (existing?.is_fresh) {
        await client.query('COMMIT');
        return existing.access_token;
      }

      const rotatedAccessToken = `tok_pg_${crypto.randomBytes(6).toString('hex')}`;
      const rotatedRefreshToken = `ref_pg_${crypto.randomBytes(6).toString('hex')}`;

      await client.query(
        `UPDATE oauth_connections
         SET access_token = $1, refresh_token = $2, expires_at = NOW() + INTERVAL '1 hour'
         WHERE tenant_id = $3 AND provider = $4`,
        [rotatedAccessToken, rotatedRefreshToken, tenantId, provider],
      );

      await client.query('COMMIT');
      return rotatedAccessToken;
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  }

  async processInboundEvent(event: ExternalContactEvent): Promise<SyncOutcome> {
    // 1. Origin check: drop echoes from our own integration service account
    if (event.updatedByServiceAccount) {
      return { status: 'skipped', reason: 'self_origin' };
    }

    // 2. Idempotency check (Redis fast path, with PostgreSQL processed_events fallback)
    const idempotencyKey = `idem:${event.provider}:${event.tenantId}:${event.eventId}`;
    let redisHealthy = true;

    try {
      const isFirstDelivery = await this.redis.set(idempotencyKey, '1', 'EX', 86_400, 'NX');
      if (!isFirstDelivery) {
        return { status: 'skipped', reason: 'duplicate_event' };
      }
    } catch {
      redisHealthy = false;
    }

    // 3. Per-tenant rate limit check (fails open to conservative local memory if Redis is down)
    const quota = await this.rateLimiter.acquire(event.tenantId, event.provider);
    if (!quota.allowed) {
      if (redisHealthy) {
        await this.redis.del(idempotencyKey).catch(() => undefined);
      }
      return { status: 'deferred', retryAfterMs: quota.retryAfterMs };
    }

    const newHash = this.computeNormalizedHash(event.payload);
    const client = await this.db.connect();

    try {
      await client.query('BEGIN');

      // PostgreSQL idempotency fallback when Redis is unreachable
      if (!redisHealthy) {
        const dedupInsert = await client.query(
          `INSERT INTO processed_events (provider, tenant_id, event_id)
           VALUES ($1, $2, $3)
           ON CONFLICT (provider, tenant_id, event_id) DO NOTHING`,
          [event.provider, event.tenantId, event.eventId],
        );

        if (dedupInsert.rowCount === 0) {
          await client.query('COMMIT');
          return { status: 'skipped', reason: 'duplicate_event' };
        }
      }

      const existing = await client.query<{ payload_hash: string }>(
        `SELECT payload_hash FROM integration_mappings
         WHERE tenant_id = $1 AND provider = $2 AND external_id = $3
         FOR UPDATE`,
        [event.tenantId, event.provider, event.externalId],
      );

      // 4. Content-hash loop prevention
      if (existing.rows[0]?.payload_hash === newHash) {
        await client.query('COMMIT');
        return { status: 'skipped', reason: 'unchanged_hash' };
      }

      // 5. Atomic upsert + Transactional Outbox write
      await client.query(
        `INSERT INTO integration_mappings (tenant_id, provider, external_id, payload_hash, updated_at)
         VALUES ($1, $2, $3, $4, NOW())
         ON CONFLICT (tenant_id, provider, external_id)
         DO UPDATE SET payload_hash = EXCLUDED.payload_hash, updated_at = NOW()`,
        [event.tenantId, event.provider, event.externalId, newHash],
      );

      await client.query(
        `INSERT INTO outbox_events (tenant_id, aggregate_id, event_type, payload)
         VALUES ($1, $2, 'CONTACT_SYNCED', $3)`,
        [event.tenantId, event.externalId, JSON.stringify(event.payload)],
      );

      await client.query('COMMIT');
      return {
        status: 'synced',
        hash: newHash,
        storageMode: redisHealthy ? 'redis+postgres' : 'postgres_fallback',
      };
    } catch (err) {
      await client.query('ROLLBACK');
      if (redisHealthy) {
        await this.redis.del(idempotencyKey).catch(() => undefined);
      }
      throw err;
    } finally {
      client.release();
    }
  }
}