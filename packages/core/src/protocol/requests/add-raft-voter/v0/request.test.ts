import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { addRaftVoterRequestV0, requestSchema } from './request';

describe('protocol/requests/add-raft-voter/v0/request', () => {
  it('round-trips a flexible v0 request', async () => {
    const value = {
      clusterId: 'MkU3OEVBNTcwNTJENDM2Qg',
      timeoutMs: 60_000,
      voterId: 4,
      voterDirectoryId: Buffer.alloc(16, 7),
      listeners: [{ name: 'CONTROLLER', host: 'localhost', port: 9093 }],
    };

    const encoder = await addRaftVoterRequestV0(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
