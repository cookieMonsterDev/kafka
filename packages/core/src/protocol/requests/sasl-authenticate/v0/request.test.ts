import { describe, expect, it } from 'vitest';
import plainBytesFixture from '../fixtures/plain-bytes.json' with { type: 'json' };
import scram256BytesFixture from '../fixtures/scram256-bytes.json' with { type: 'json' };
import v0RequestPlainFixture from '../fixtures/v0-request-plain.json' with { type: 'json' };
import v0RequestScram256Fixture from '../fixtures/v0-request-scram256.json' with { type: 'json' };
import { saslAuthenticateRequestV0 } from './request.js';

describe('protocol/requests/sasl-authenticate/v0/request', () => {
  it('encodes PLAIN auth bytes with no length prefix, matching the real fixture', async () => {
    const definition = saslAuthenticateRequestV0({ authBytes: Buffer.from(plainBytesFixture.data) });
    const encoder = await definition.encode();
    expect(encoder.buffer).toEqual(Buffer.from(v0RequestPlainFixture.data));
  });

  it('encodes SCRAM-SHA-256 auth bytes with no length prefix, matching the real fixture', async () => {
    const definition = saslAuthenticateRequestV0({ authBytes: Buffer.from(scram256BytesFixture.data) });
    const encoder = await definition.encode();
    expect(encoder.buffer).toEqual(Buffer.from(v0RequestScram256Fixture.data));
  });
});
