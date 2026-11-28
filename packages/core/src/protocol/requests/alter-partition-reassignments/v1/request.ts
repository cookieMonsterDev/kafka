import { Encoder } from '../../../encoder';
import type { RequestDefinition } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { type AlterPartitionReassignmentsRequestV0Options, encodeTopic } from '../v0/request';

export interface AlterPartitionReassignmentsRequestV1Options extends AlterPartitionReassignmentsRequestV0Options {
  /** When false, the broker rejects any partition whose replica count would change. Defaults to true. */
  allowReplicationFactorChange?: boolean;
}

/**
 * AlterPartitionReassignments Request (Version: 1) => timeout_ms allow_replication_factor_change [topics] TAG_BUFFER
 *   timeout_ms => INT32
 *   allow_replication_factor_change => BOOLEAN
 *   topics => see v0
 *
 * v1 adds `allow_replication_factor_change` (KIP-860). Hand-written like v0.
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
export const alterPartitionReassignmentsRequestV1: (
  values: AlterPartitionReassignmentsRequestV1Options,
) => RequestDefinition = ({ topics, timeout = 5000, allowReplicationFactorChange = true }) => ({
  apiKey: API_KEYS.AlterPartitionReassignments,
  apiVersion: 1,
  apiName: 'AlterPartitionReassignments',
  encode: () =>
    Promise.resolve(
      new Encoder()
        .writeInt32(timeout)
        .writeBoolean(allowReplicationFactorChange)
        .writeUVarIntArray(topics.map(encodeTopic))
        .writeUVarIntBytes(undefined),
    ),
});
