import { describe, expect, it } from 'vitest';
import v2RequestFixture from '../fixtures/v2-request.json' with { type: 'json' };
import { produceRequestV2 } from './request';

describe('protocol/requests/produce/v2/request', () => {
  it('sets expectResponse to false when acks=0', () => {
    const request = produceRequestV2({
      acks: 0,
      timeout: 30000,
      compression: 0,
      topicData: [{ topic: 't', partitions: [{ partition: 0, messages: [] }] }],
    });
    expect(request.expectResponse?.()).toBe(false);
    expect(request.apiVersion).toBe(2);
  });

  it('encodes a request matching a captured protocol fixture', async () => {
    const encoder = await produceRequestV2({
      acks: -1,
      timeout: 30000,
      compression: 0,
      topicData: [
        {
          topic: 'test-topic-9f825c3f60bb0b4db583',
          partitions: [
            {
              partition: 0,
              messages: [
                {
                  key: 'key-bb252ae5801883c12bbd',
                  value: 'some-value-10340c6329f8bbf5b4a2',
                  timestamp: 1509819296569,
                },
                {
                  key: 'key-8a14e73a88e93f7c3a39',
                  value: 'some-value-4fa91513bffbcc0e34b3',
                  timestamp: 1509819296569,
                },
                {
                  key: 'key-183a2d8eb3683f080b82',
                  value: 'some-value-938afcf1f2ef0439c752',
                  timestamp: 1509819296569,
                },
              ],
            },
          ],
        },
      ],
    }).encode();

    expect(encoder.buffer).toEqual(Buffer.from(v2RequestFixture.data));
  });
});
