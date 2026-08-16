import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { saslHandshakeRequestV0 } from './request';

describe('protocol/requests/sasl-handshake/v0/request', () => {
  it('encodes the mechanism as a string', async () => {
    const definition = saslHandshakeRequestV0({ mechanism: 'PLAIN' });
    const encoder = await definition.encode();

    expect(definition.apiKey).toBe(17);
    expect(definition.apiVersion).toBe(0);
    expect(encoder.buffer).toEqual(new Encoder().writeString('PLAIN').buffer);
  });
});
