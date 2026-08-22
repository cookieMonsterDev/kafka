import { describe, expect, it } from 'vitest';
import { AddRaftVoter } from './index';

describe('protocol/requests/add-raft-voter', () => {
  it('registers versions 0 and 1', () => {
    expect(AddRaftVoter.versions).toEqual([0, 1]);
  });

  it('defaults ackWhenCommitted to true on v1', () => {
    const { request } = AddRaftVoter.protocol({ version: 1 })({
      voterId: 1,
      voterDirectoryId: Buffer.alloc(16),
      listeners: [{ name: 'CONTROLLER', host: 'localhost', port: 9093 }],
    });
    expect(request.apiVersion).toBe(1);
  });
});
