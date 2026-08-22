import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { removeRaftVoterResponseV0, responseSchema } from './response';

describe('protocol/requests/remove-raft-voter/v0/response', () => {
  it('round-trips a flexible v0 response and remaps throttleTime', async () => {
    const value = {
      throttleTime: 2,
      errorCode: 0,
      errorMessage: null,
    };

    const encoder = new Encoder();
    responseSchema.write(encoder, value);
    const data = await removeRaftVoterResponseV0.decode(encoder.buffer);
    expect(data).toEqual({
      ...value,
      throttleTime: 0,
      clientSideThrottleTime: 2,
    });
    await expect(removeRaftVoterResponseV0.parse(data)).resolves.toEqual(data);
  });
});
