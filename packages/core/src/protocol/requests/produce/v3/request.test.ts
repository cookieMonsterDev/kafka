import { describe, expect, it } from 'vitest';
import v3RequestFixture from '../fixtures/v3-request.json' with { type: 'json' };
import { produceRequestV3 } from './request.js';

describe('protocol/requests/produce/v3/request', () => {
  it('sets expectResponse to false when acks=0', () => {
    const request = produceRequestV3({
      acks: 0,
      timeout: 30000,
      transactionalId: null,
      compression: 0,
      topicData: [{ topic: 't', partitions: [{ partition: 0, firstSequence: 0, messages: [] }] }],
    });
    expect(request.expectResponse?.()).toBe(false);
  });

  it('encodes a request matching a real kafkajs fixture', async () => {
    const encoder = await produceRequestV3({
      transactionalId: null,
      acks: -1,
      timeout: 30000,
      compression: 0,
      topicData: [
        {
          topic: 'test-topic-ebba68879c6f5081d8c2',
          partitions: [
            {
              partition: 0,
              firstSequence: 10,
              messages: [
                {
                  key: 'key-9d0f348cb2e730e1edc4',
                  value: 'some-value-a17b4c81f9ecd1e896e3',
                  timestamp: 1509928155660,
                  headers: { a: 'b', c: ['d', 'e'] },
                },
                {
                  key: 'key-c7073e965c34b4cc6442',
                  value: 'some-value-65df422070d7ad73914f',
                  timestamp: 1509928155660,
                  headers: { a: 'b' },
                },
                {
                  key: 'key-1693b184a9b52dbe03bc',
                  value: 'some-value-3fcb65ffca087cba20ad',
                  timestamp: 1509928155660,
                  headers: { a: 'b' },
                },
              ],
            },
          ],
        },
      ],
    }).encode();

    expect(encoder.buffer).toEqual(Buffer.from(v3RequestFixture.data));
  });
});
