import { ISOLATION_LEVEL, type IsolationLevel } from '../../enums/isolation-level';
import type { ProtocolFactory, RequestFamily } from '../index';
import {
  REPLICA_ID,
  type ListOffsetsTopicOptions,
  withDefaultTimestamps,
  withDefaultTimestampsAndMaxOffsets,
} from './shared';
import { listOffsetsRequestV0 } from './v0/request';
import { listOffsetsResponseV0 } from './v0/response';
import { listOffsetsRequestV1 } from './v1/request';
import { listOffsetsResponseV1 } from './v1/response';
import { listOffsetsRequestV2 } from './v2/request';
import { listOffsetsResponseV2 } from './v2/response';
import { listOffsetsRequestV3 } from './v3/request';
import { listOffsetsResponseV3 } from './v3/response';

export interface ListOffsetsOptions {
  replicaId?: number;
  isolationLevel?: IsolationLevel;
  topics: ListOffsetsTopicOptions[];
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<ListOffsetsOptions>>> = {
  0: ({ replicaId = REPLICA_ID, topics }) => ({
    request: listOffsetsRequestV0({ replicaId, topics: withDefaultTimestampsAndMaxOffsets(topics) }),
    response: listOffsetsResponseV0,
  }),
  1: ({ replicaId = REPLICA_ID, topics }) => ({
    request: listOffsetsRequestV1({ replicaId, topics: withDefaultTimestamps(topics) }),
    response: listOffsetsResponseV1,
  }),
  2: ({ replicaId = REPLICA_ID, isolationLevel = ISOLATION_LEVEL.READ_COMMITTED, topics }) => ({
    request: listOffsetsRequestV2({ replicaId, isolationLevel, topics: withDefaultTimestamps(topics) }),
    response: listOffsetsResponseV2,
  }),
  3: ({ replicaId = REPLICA_ID, isolationLevel = ISOLATION_LEVEL.READ_COMMITTED, topics }) => ({
    request: listOffsetsRequestV3({ replicaId, isolationLevel, topics: withDefaultTimestamps(topics) }),
    response: listOffsetsResponseV3,
  }),
};

export const ListOffsets: RequestFamily<ListOffsetsOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no ListOffsets protocol for version ${version}`);
    return factory;
  },
});
