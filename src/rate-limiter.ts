import { Redis } from 'ioredis';

export class TenantRateLimiter {
  constructor(private readonly redis: Redis) {}

  /**
   * Fixed-window counter per tenant & provider (e.g., 60 req/min for legacy ERPs).
   * Returns allowed status and recommended retry delay with jitter.
   */
  async acquire(tenantId: string, provider: string, limitPerMin = 60): Promise<{ allowed: boolean; retryAfterMs: number }> {
    const windowKey = Math.floor(Date.now() / 60_000);
    const key = `ratelimit:${provider}:${tenantId}:${windowKey}`;

    const count = await this.redis.incr(key);
    if (count === 1) {
      await this.redis.expire(key, 65);
    }

    if (count <= limitPerMin) {
      return { allowed: true, retryAfterMs: 0 };
    }

    const msUntilNextMinute = 60_000 - (Date.now() % 60_000);
    const jitterMs = Math.floor(Math.random() * 500);
    return { allowed: false, retryAfterMs: msUntilNextMinute + jitterMs };
  }
}