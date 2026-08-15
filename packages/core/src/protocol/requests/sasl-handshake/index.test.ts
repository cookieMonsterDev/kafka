import { describe, expect, it } from 'vitest';
import { SaslHandshake } from './index.js';

describe('protocol/requests/sasl-handshake', () => {
  it('implements versions 0 and 1', () => {
    expect(SaslHandshake.versions).toEqual([0, 1]);
  });

  it('builds a request for the requested version', () => {
    const { request } = SaslHandshake.protocol({ version: 1 })({ mechanism: 'SCRAM-SHA-256' });
    expect(request.apiVersion).toBe(1);
  });
});
