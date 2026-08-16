import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { fetchResponseV12 } from './response';

describe('protocol/requests/fetch/v12/response', () => {
  it('decodes compact topic names, empty compact records, and tagged fields', async () => {
    const encoded = new Encoder()
      .writeInt32(5)
      .writeInt16(0)
      .writeInt32(42)
      .writeUVarInt(2)
      .writeUVarIntString('orders')
      .writeUVarInt(2)
      .writeInt32(0)
      .writeInt16(0)
      .writeInt64(10n)
      .writeInt64(10n)
      .writeInt64(0n)
      .writeUVarInt(1)
      .writeInt32(-1)
      .writeUVarInt(0)
      .writeUVarInt(0)
      .writeUVarInt(0)
      .writeUVarInt(0);

    const decoded = await fetchResponseV12.decode(encoded.buffer);
    expect(decoded).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 5,
      errorCode: 0,
      sessionId: 42,
      responses: [
        {
          topicName: 'orders',
          partitions: [
            {
              partition: 0,
              errorCode: 0,
              highWatermark: 10n,
              lastStableOffset: 10n,
              logStartOffset: 0n,
              abortedTransactions: [],
              preferredReadReplica: -1,
              messages: [],
            },
          ],
        },
      ],
    });
  });
});
