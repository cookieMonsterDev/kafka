import { describe, expect, it } from 'vitest';
import { SaslAuthenticate } from './index';

describe('protocol/requests/sasl-authenticate', () => {
  it('implements versions 0 and 1', () => {
    expect(SaslAuthenticate.versions).toEqual([0, 1]);
  });

  it('builds a request for the requested version', () => {
    const { request } = SaslAuthenticate.protocol({ version: 1 })({ authBytes: Buffer.from('x') });
    expect(request.apiVersion).toBe(1);
  });
});
