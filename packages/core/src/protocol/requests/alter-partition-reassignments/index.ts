import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { alterPartitionReassignmentsRequestV0, type AlterPartitionReassignmentsRequestV0Options } from './v0/request';
import { alterPartitionReassignmentsResponseV0 } from './v0/response';

const VERSIONS = {
  0: (values) => ({
    request: alterPartitionReassignmentsRequestV0(values),
    response: alterPartitionReassignmentsResponseV0,
  }),
} satisfies VersionFactories<AlterPartitionReassignmentsRequestV0Options>;

export const AlterPartitionReassignments = defineRequestFamily(KAFKA_APIS.AlterPartitionReassignments, VERSIONS);
