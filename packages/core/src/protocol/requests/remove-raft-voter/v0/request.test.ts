import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { removeRaftVoterRequestV0, requestSchema } from './request';

describe('protocol/requests/remove-raft-voter/v0/request', () => {
  it('round-trips a flexible v0 request', async () => {
    const value = {
      clusterId: 'cluster-1',
      voterId: 4,
      voterDirectoryId: Buffer.alloc(16, 3),
    };

    const encoder = await removeRaftVoterRequestV0(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
