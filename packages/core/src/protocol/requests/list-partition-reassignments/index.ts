import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { listPartitionReassignmentsRequestV0, type ListPartitionReassignmentsRequestV0Options } from './v0/request';
import { listPartitionReassignmentsResponseV0 } from './v0/response';

const VERSIONS = {
  0: (values) => ({
    request: listPartitionReassignmentsRequestV0(values),
    response: listPartitionReassignmentsResponseV0,
  }),
} satisfies VersionFactories<ListPartitionReassignmentsRequestV0Options>;

export const ListPartitionReassignments = defineRequestFamily(KAFKA_APIS.ListPartitionReassignments, VERSIONS);
