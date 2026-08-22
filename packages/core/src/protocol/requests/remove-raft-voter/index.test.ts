import { describe, expect, it } from 'vitest';
import { RemoveRaftVoter } from './index';

describe('protocol/requests/remove-raft-voter', () => {
  it('registers version 0', () => {
    expect(RemoveRaftVoter.versions).toEqual([0]);
  });
});
