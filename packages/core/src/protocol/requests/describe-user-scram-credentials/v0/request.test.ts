import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { describeUserScramCredentialsRequestV0, requestSchema } from './request';

describe('protocol/requests/describe-user-scram-credentials/v0/request', () => {
  it('encodes a compact user list', async () => {
    const encoder = await describeUserScramCredentialsRequestV0({ users: [{ name: 'alice' }] }).encode();
    const expected = new Encoder().writeUVarInt(2).writeUVarIntString('alice').writeUVarInt(0).writeUVarInt(0);
    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual({ users: [{ name: 'alice' }] });
  });

  it('encodes null users as compact null (describe all)', async () => {
    const encoder = await describeUserScramCredentialsRequestV0({ users: null }).encode();
    expect(encoder.buffer).toEqual(new Encoder().writeUVarInt(0).writeUVarInt(0).buffer);
  });
});
