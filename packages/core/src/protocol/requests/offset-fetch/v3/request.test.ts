import { describe, expect, it } from 'vitest';
import v3RequestFixture from '../fixtures/v3-request.json' with { type: 'json' };
import { offsetFetchRequestV3 } from './request';

describe('protocol/requests/offset-fetch/v3/request', () => {
  it('encodes matching a real fixture', async () => {
    const definition = offsetFetchRequestV3({
      groupId: 'consumer-group-id-d1492d7a3c14a838a28f-20117-ae82781b-863d-4f23-9377-d165ca585f31',
      topics: [
        {
          topic: 'test-topic-df48241c4bf2fca9d16b-20117-aff9b64c-69a2-4456-be7b-de5bcd78984e',
          partitions: [{ partition: 0 }],
        },
      ],
    });
    const encoder = await definition.encode();
    expect(encoder.buffer).toEqual(Buffer.from(v3RequestFixture.data));
  });

  it('collapses an empty topics array to wire length -1 ("all topics")', async () => {
    const definition = offsetFetchRequestV3({ groupId: 'g', topics: [] });
    const encoder = await definition.encode();
    expect(encoder.buffer.readInt32BE(encoder.buffer.length - 4)).toBe(-1);
  });
});
