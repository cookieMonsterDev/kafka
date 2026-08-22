import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { addRaftVoterResponseV0, responseSchema } from './response';

describe('protocol/requests/add-raft-voter/v0/response', () => {
  it('round-trips a flexible v0 response and remaps throttleTime', async () => {
    const value = {
      throttleTime: 9,
      errorCode: 0,
      errorMessage: null,
    };

    const encoder = new Encoder();
    responseSchema.write(encoder, value);
    const data = await addRaftVoterResponseV0.decode(encoder.buffer);
    expect(data).toEqual({
      ...value,
      throttleTime: 0,
      clientSideThrottleTime: 9,
    });
    await expect(addRaftVoterResponseV0.parse(data)).resolves.toEqual(data);
  });
});
