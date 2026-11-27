import { KafkaNonRetriableError } from '../../../errors';
import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { alterPartitionReassignmentsRequestV0 } from './v0/request';
import { alterPartitionReassignmentsResponseV0 } from './v0/response';
import { alterPartitionReassignmentsRequestV1, type AlterPartitionReassignmentsRequestV1Options } from './v1/request';
import { alterPartitionReassignmentsResponseV1 } from './v1/response';

const VERSIONS = {
  0: ({ allowReplicationFactorChange, ...values }) => {
    if (allowReplicationFactorChange === false) {
      throw new KafkaNonRetriableError(
        'AlterPartitionReassignments v0 cannot disallow replication factor changes; this broker needs AlterPartitionReassignments v1 or newer',
      );
    }
    return {
      request: alterPartitionReassignmentsRequestV0(values),
      response: alterPartitionReassignmentsResponseV0,
    };
  },
  1: (values) => ({
    request: alterPartitionReassignmentsRequestV1(values),
    response: alterPartitionReassignmentsResponseV1,
  }),
} satisfies VersionFactories<AlterPartitionReassignmentsRequestV1Options>;

export const AlterPartitionReassignments = defineRequestFamily(KAFKA_APIS.AlterPartitionReassignments, VERSIONS);
