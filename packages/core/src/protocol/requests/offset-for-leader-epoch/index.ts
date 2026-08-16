import type { ProtocolFactory, RequestFamily } from '../index';
import { REPLICA_ID, withCurrentLeaderEpochs, withoutCurrentLeaderEpoch } from './shared';
import { offsetForLeaderEpochRequestV0 } from './v0/request';
import { offsetForLeaderEpochResponseV0 } from './v0/response';
import { offsetForLeaderEpochRequestV1 } from './v1/request';
import { offsetForLeaderEpochResponseV1 } from './v1/response';
import { offsetForLeaderEpochRequestV2 } from './v2/request';
import { offsetForLeaderEpochResponseV2 } from './v2/response';
import { offsetForLeaderEpochRequestV3 } from './v3/request';
import { offsetForLeaderEpochResponseV3 } from './v3/response';
import { offsetForLeaderEpochRequestV4 } from './v4/request';
import { offsetForLeaderEpochResponseV4 } from './v4/response';

export interface OffsetForLeaderEpochOptions {
  replicaId?: number;
  topics: {
    topic: string;
    partitions: { partition: number; currentLeaderEpoch?: number; leaderEpoch: number }[];
  }[];
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<OffsetForLeaderEpochOptions>>> = {
  0: ({ topics }) => ({
    request: offsetForLeaderEpochRequestV0({ topics: withoutCurrentLeaderEpoch(topics) }),
    response: offsetForLeaderEpochResponseV0,
  }),
  1: ({ topics }) => ({
    request: offsetForLeaderEpochRequestV1({ topics: withCurrentLeaderEpochs(topics) }),
    response: offsetForLeaderEpochResponseV1,
  }),
  2: ({ replicaId = REPLICA_ID, topics }) => ({
    request: offsetForLeaderEpochRequestV2({ replicaId, topics: withCurrentLeaderEpochs(topics) }),
    response: offsetForLeaderEpochResponseV2,
  }),
  3: ({ replicaId = REPLICA_ID, topics }) => ({
    request: offsetForLeaderEpochRequestV3({ replicaId, topics: withCurrentLeaderEpochs(topics) }),
    response: offsetForLeaderEpochResponseV3,
  }),
  4: ({ replicaId = REPLICA_ID, topics }) => ({
    request: offsetForLeaderEpochRequestV4({ replicaId, topics: withCurrentLeaderEpochs(topics) }),
    response: offsetForLeaderEpochResponseV4,
  }),
};

export const OffsetForLeaderEpoch: RequestFamily<OffsetForLeaderEpochOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no OffsetForLeaderEpoch protocol for version ${version}`);
    return factory;
  },
});
