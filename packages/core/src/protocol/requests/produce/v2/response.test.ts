import { describe, expect, it } from 'vitest';
import v2ResponseFixture from '../fixtures/v2-response.json' with { type: 'json' };
import v2ResponseGzipFixture from '../fixtures/v2-response-gzip.json' with { type: 'json' };
import { produceResponseV2 } from './response';

describe('protocol/requests/produce/v2/response', () => {
  it('decodes a real fixture, mapping offset/timestamp to baseOffset/logAppendTime', async () => {
    const data = await produceResponseV2.decode(Buffer.from(v2ResponseFixture.data));
    expect(data).toEqual({
      topics: [
        {
          topicName: 'test-topic-919fb44e912ac0dc2693',
          partitions: [{ partition: 0, errorCode: 0, baseOffset: 3n, logAppendTime: -1n, logStartOffset: -1n }],
        },
      ],
      throttleTime: 0,
    });
    await expect(produceResponseV2.parse(data)).resolves.toBeTruthy();
  });

  it('decodes a gzip produce response fixture', async () => {
    const data = await produceResponseV2.decode(Buffer.from(v2ResponseGzipFixture.data));
    expect(data).toEqual({
      topics: [
        {
          topicName: 'test-topic-bc674c30572e8ded886a',
          partitions: [{ partition: 0, errorCode: 0, baseOffset: 3n, logAppendTime: -1n, logStartOffset: -1n }],
        },
      ],
      throttleTime: 0,
    });
  });
});
