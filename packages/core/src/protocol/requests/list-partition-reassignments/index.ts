import type { ProtocolFactory, RequestFamily } from '../index.js';
import { listPartitionReassignmentsRequestV0, type ListPartitionReassignmentsRequestV0Options } from './v0/request.js';
import { listPartitionReassignmentsResponseV0 } from './v0/response.js';

const VERSIONS: Readonly<Record<number, ProtocolFactory<ListPartitionReassignmentsRequestV0Options>>> = {
  0: (values) => ({
    request: listPartitionReassignmentsRequestV0(values),
    response: listPartitionReassignmentsResponseV0,
  }),
};

export const ListPartitionReassignments: RequestFamily<ListPartitionReassignmentsRequestV0Options> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no ListPartitionReassignments protocol for version ${version}`);
    return factory;
  },
});
