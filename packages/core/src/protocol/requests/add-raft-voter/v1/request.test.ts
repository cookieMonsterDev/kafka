import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { addRaftVoterRequestV1, requestSchema } from './request';

describe('protocol/requests/add-raft-voter/v1/request', () => {
  it('round-trips a flexible v1 request with ackWhenCommitted', async () => {
    const value = {
      clusterId: null,
      timeoutMs: 30_000,
      voterId: 4,
      voterDirectoryId: Buffer.alloc(16, 1),
      listeners: [{ name: 'CONTROLLER', host: '127.0.0.1', port: 9093 }],
      ackWhenCommitted: false,
    };

    const encoder = await addRaftVoterRequestV1(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
