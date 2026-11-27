import { describe, expect, it } from 'vitest';
import { KafkaAggregateError } from '../../../../errors';
import { Encoder } from '../../../encoder';
import { alterPartitionReassignmentsResponseV1 } from './response';

function encodeResponse(partitionErrorCode: number): Buffer {
  const partition = new Encoder()
    .writeInt32(0)
    .writeInt16(partitionErrorCode)
    .writeUVarIntString(null)
    .writeUVarIntBytes(undefined);
  const topic = new Encoder().writeUVarIntString('orders').writeUVarIntArray([partition]).writeUVarIntBytes(undefined);
  return new Encoder()
    .writeInt32(0)
    .writeBoolean(false)
    .writeInt16(0)
    .writeUVarIntString(null)
    .writeUVarIntArray([topic])
    .writeUVarIntBytes(undefined).buffer;
}

describe('protocol/requests/alter-partition-reassignments/v1/response', () => {
  it('decodes allow_replication_factor_change', async () => {
    const data = await alterPartitionReassignmentsResponseV1.decode(encodeResponse(0));

    expect(data).toEqual({
      throttleTime: 0,
      allowReplicationFactorChange: false,
      errorCode: 0,
      responses: [{ topic: 'orders', partitions: [{ partition: 0, errorCode: 0 }] }],
    });
    await expect(alterPartitionReassignmentsResponseV1.parse(data)).resolves.toEqual(data);
  });

  it('aggregates a refused replication factor change per partition', async () => {
    // INVALID_REPLICATION_FACTOR
    const data = await alterPartitionReassignmentsResponseV1.decode(encodeResponse(38));

    const promise = alterPartitionReassignmentsResponseV1.parse(data);
    await expect(promise).rejects.toThrow(KafkaAggregateError);
    await expect(promise).rejects.toThrow(
      expect.objectContaining({
        errors: [expect.objectContaining({ topic: 'orders', partition: 0 })],
      }),
    );
  });
});
