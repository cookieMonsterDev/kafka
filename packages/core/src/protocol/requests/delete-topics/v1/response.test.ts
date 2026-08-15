import { describe, expect, it } from 'vitest';
import v1ResponseFixture from '../fixtures/v1-response.json' with { type: 'json' };
import { deleteTopicsResponseV1 } from './response.js';

describe('protocol/requests/delete-topics/v1/response', () => {
  it('decodes a real fixture, remapping throttleTime to clientSideThrottleTime', async () => {
    const data = await deleteTopicsResponseV1.decode(Buffer.from(v1ResponseFixture.data));

    expect(data).toEqual({
      clientSideThrottleTime: 0,
      throttleTime: 0,
      topicErrors: [
        {
          topic: 'test-topic-386ea404396d663a8042-56298-e6e26331-de25-48d8-90b6-4710cd0b618b',
          errorCode: 0,
        },
        {
          topic: 'test-topic-bb5d4c0c37ae53eb8b53-56298-ac202bf8-78e7-4d8b-ad07-4e01d8148db0',
          errorCode: 0,
        },
      ],
    });
    await expect(deleteTopicsResponseV1.parse(data)).resolves.toBeTruthy();
  });
});
