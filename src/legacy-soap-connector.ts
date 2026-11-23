import crypto from 'node:crypto';
import { Redis } from 'ioredis';
import { PermanentErpError, TransientErpError } from './errors.js';
import type { ExternalContactEvent } from './sync-engine.js';

export interface NormalizedContact {
  readonly externalId?: string;
  readonly email: string;
  readonly fullName: string;
  readonly role: string;
}

export interface ErpConnector {
  readonly provider: string;
  pullUpdatedContacts(tenantId: string, lastWatermark: Date): Promise<ExternalContactEvent[]>;
  pushContact(tenantId: string, contact: NormalizedContact): Promise<{ externalId: string }>;
}

/**
 * Anti-Corruption Layer for legacy SOAP/WSDL ERPs (e.g., Sage 300 CRE, Viewpoint).
 * Isolates stateful session logins, XML envelopes, HTTP 200 <soap:Fault> traps,
 * and watermark delta polling from the core sync engine.
 */
export class LegacySoapErpConnector implements ErpConnector {
  readonly provider = 'legacy_soap_erp';
  private simulateExpiredSessionOnce = true;

  constructor(private readonly redis: Redis) {}

  /**
   * Many legacy ERPs cap an integration user at 1 active session.
   * Uses a Redis distributed lock so parallel workers share one SessionId.
   */
  async getOrCreateSoapSession(tenantId: string): Promise<string> {
    const sessionKey = `soap:session:${tenantId}`;
    const lockKey = `soap:lock:${tenantId}`;

    try {
      const cached = await this.redis.get(sessionKey);
      if (cached) return cached;

      const acquired = await this.redis.set(lockKey, '1', 'PX', 5000, 'NX');
      if (!acquired) {
        await new Promise((r) => setTimeout(r, 100));
        return this.getOrCreateSoapSession(tenantId);
      }

      try {
        const sessionId = `SOAP_SESS_${crypto.randomBytes(6).toString('hex')}`;
        await this.redis.set(sessionKey, sessionId, 'EX', 1200); // 20-min ERP session TTL
        return sessionId;
      } finally {
        await this.redis.del(lockKey);
      }
    } catch {
      // Fallback if Redis is unavailable: issue direct session login
      return `SOAP_SESS_DIRECT_${crypto.randomBytes(6).toString('hex')}`;
    }
  }

  /**
   * Pushes a normalized contact via SOAP XML and catches the classic "HTTP 200 Fault" trap.
   */
  async pushContact(tenantId: string, contact: NormalizedContact): Promise<{ externalId: string }> {
    const sessionId = await this.getOrCreateSoapSession(tenantId);

    const soapRequestEnvelope = `
      <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
        <soapenv:Header><SessionId>${sessionId}</SessionId></soapenv:Header>
        <soapenv:Body>
          <UpsertContact>
            <Email>${contact.email}</Email>
            <FullName>${contact.fullName}</FullName>
          </UpsertContact>
        </soapenv:Body>
      </soapenv:Envelope>`.trim();

    const rawResponse = await this.mockLegacySoapTransport(soapRequestEnvelope);

    // Legacy SOAP servers frequently return HTTP 200 OK even on failures
    if (rawResponse.httpStatus === 200 && rawResponse.xmlBody.includes('<soap:Fault>')) {
      if (rawResponse.xmlBody.includes('SESSION_EXPIRED')) {
        await this.redis.del(`soap:session:${tenantId}`).catch(() => undefined);
        throw new TransientErpError('Legacy SOAP session expired; invalidated cache for retry');
      }
      throw new PermanentErpError(`Legacy ERP schema rejection: ${rawResponse.xmlBody}`);
    }

    const match = rawResponse.xmlBody.match(/<ContactId>(.*?)<\/ContactId>/);
    if (!match?.[1]) {
      throw new PermanentErpError('Missing <ContactId> in SOAP response');
    }

    return { externalId: match[1] };
  }

  /**
   * Delta polling for ERPs without webhooks: subtracts a 5-minute clock-drift buffer
   * from the watermark; downstream Redis/SHA-256 guards deduplicate any overlap.
   */
  async pullUpdatedContacts(tenantId: string, lastWatermark: Date): Promise<ExternalContactEvent[]> {
    const safeQuerySince = new Date(lastWatermark.getTime() - 5 * 60_000);

    // Simulated XML delta records returned by SOAP GetModifiedContacts(ModifiedSince >= safeQuerySince)
    const xmlRecords = [
      {
        id: 'soap_contact_77',
        updatedAt: new Date(safeQuerySince.getTime() + 60_000).toISOString(),
        email: 'carlos.mendez@pacificbuilders.com',
        fullName: 'Carlos Mendez',
        role: 'Project Executive',
      },
    ];

    return xmlRecords.map((record) => ({
      eventId: `${record.id}:${record.updatedAt}`,
      tenantId,
      provider: 'legacy_soap_erp',
      externalId: record.id,
      updatedByServiceAccount: false,
      payload: {
        email: record.email,
        fullName: record.fullName,
        role: record.role,
      },
    }));
  }

  private async mockLegacySoapTransport(_xmlEnvelope: string): Promise<{ httpStatus: number; xmlBody: string }> {
    if (this.simulateExpiredSessionOnce) {
      this.simulateExpiredSessionOnce = false;
      return {
        httpStatus: 200,
        xmlBody: '<soap:Envelope><soap:Body><soap:Fault><faultcode>SESSION_EXPIRED</faultcode></soap:Fault></soap:Body></soap:Envelope>',
      };
    }

    return {
      httpStatus: 200,
      xmlBody: '<soap:Envelope><soap:Body><UpsertContactResponse><ContactId>soap_contact_77</ContactId></UpsertContactResponse></soap:Body></soap:Envelope>',
    };
  }
}