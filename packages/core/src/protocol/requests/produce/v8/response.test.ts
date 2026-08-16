import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { produceResponseV8 } from './response';

function encodeV8Response(params: {
  topicName: string;
  partition: number;
  errorCode: number;
  recordErrors: { batchIndex: number; batchIndexErrorMessage: string | null }[];
  errorMessage: string | null;
  throttleTime?: number;
}): Buffer {
  return new Encoder()
    .writeArray([
      new Encoder().writeString(params.topicName).writeArray([
        new Encoder()
          .writeInt32(params.partition)
          .writeInt16(params.errorCode)
          .writeInt64(0n)
          .writeInt64(-1n)
          .writeInt64(0n)
          .writeArray(
            params.recordErrors.map((error) =>
              new Encoder().writeInt32(error.batchIndex).writeString(error.batchIndexErrorMessage),
            ),
          )
          .writeString(params.errorMessage),
      ]),
    ])
    .writeInt32(params.throttleTime ?? 0).buffer;
}

describe('protocol/requests/produce/v8/response', () => {
  it('decodes record_errors and a partition error_message (KIP-467)', async () => {
    const data = await produceResponseV8.decode(
      encodeV8Response({
        topicName: 'test-topic',
        partition: 2,
        errorCode: 87,
        recordErrors: [
          { batchIndex: 1, batchIndexErrorMessage: 'record is invalid' },
          { batchIndex: 3, batchIndexErrorMessage: null },
        ],
        errorMessage: 'one or more records failed validation',
        throttleTime: 15,
      }),
    );

    expect(data).toEqual({
      topics: [
        {
          topicName: 'test-topic',
          partitions: [
            {
              partition: 2,
              errorCode: 87,
              baseOffset: 0n,
              logAppendTime: -1n,
              logStartOffset: 0n,
              recordErrors: [
                { batchIndex: 1, batchIndexErrorMessage: 'record is invalid' },
                { batchIndex: 3, batchIndexErrorMessage: null },
              ],
              errorMessage: 'one or more records failed validation',
            },
          ],
        },
      ],
      throttleTime: 0,
      clientSideThrottleTime: 15,
    });
  });

  it('still throws from the partition error_code when record_errors are present', async () => {
    const data = await produceResponseV8.decode(
      encodeV8Response({
        topicName: 'test-topic',
        partition: 2,
        errorCode: 87,
        recordErrors: [{ batchIndex: 1, batchIndexErrorMessage: 'record is invalid' }],
        errorMessage: 'one or more records failed validation',
      }),
    );

    await expect(produceResponseV8.parse(data)).rejects.toMatchObject({
      type: 'INVALID_RECORD',
      topic: 'test-topic',
      partition: 2,
    });
  });
});
