import type { ProtocolFactory, RequestFamily } from '../index';
import {
  alterPartitionReassignmentsRequestV0,
  type AlterPartitionReassignmentsRequestV0Options,
} from './v0/request';
import { alterPartitionReassignmentsResponseV0 } from './v0/response';

const VERSIONS: Readonly<Record<number, ProtocolFactory<AlterPartitionReassignmentsRequestV0Options>>> = {
  0: (values) => ({
    request: alterPartitionReassignmentsRequestV0(values),
    response: alterPartitionReassignmentsResponseV0,
  }),
};

export const AlterPartitionReassignments: RequestFamily<AlterPartitionReassignmentsRequestV0Options> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no AlterPartitionReassignments protocol for version ${version}`);
    return factory;
  },
});
