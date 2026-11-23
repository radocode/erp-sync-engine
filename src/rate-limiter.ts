import { Redis } from 'ioredis';

export interface RateLimitResult {
  readonly allowed: boolean;
  readonly retryAfterMs: number;
  readonly degradedFallback: boolean;
}

export class TenantRateLimiter {
  private readonly localFallbackCounters = new Map<string, { windowKey: number; count: number }>();

  constructor(private readonly redis: Redis) {}

  /**
   * Per-tenant fixed-window rate limiter in Redis.
   * Tradeoff: If Redis is unreachable, fails open to a conservative in-memory
   * per-worker cap (25% of normal quota) so syncing degrades gracefully without
   * halting all customer traffic or flooding the ERP.
   */
  async acquire(tenantId: string, provider: string, limitPerMin = 60): Promise<RateLimitResult> {
    const windowKey = Math.floor(Date.now() / 60_000);
    const key = `ratelimit:${provider}:${tenantId}:${windowKey}`;
    const msUntilNextMinute = 60_000 - (Date.now() % 60_000);
    const jitterMs = Math.floor(Math.random() * 500);

    try {
      const count = await this.redis.incr(key);
      if (count === 1) {
        await this.redis.expire(key, 65);
      }

      if (count <= limitPerMin) {
        return { allowed: true, retryAfterMs: 0, degradedFallback: false };
      }

      return { allowed: false, retryAfterMs: msUntilNextMinute + jitterMs, degradedFallback: false };
    } catch {
      const localKey = `${provider}:${tenantId}`;
      const conservativeWorkerCap = Math.max(1, Math.floor(limitPerMin / 4));
      const existing = this.localFallbackCounters.get(localKey);

      const currentCount = existing && existing.windowKey === windowKey ? existing.count + 1 : 1;
      this.localFallbackCounters.set(localKey, { windowKey, count: currentCount });

      if (currentCount <= conservativeWorkerCap) {
        return { allowed: true, retryAfterMs: 0, degradedFallback: true };
      }

      return { allowed: false, retryAfterMs: msUntilNextMinute + jitterMs, degradedFallback: true };
    }
  }
}