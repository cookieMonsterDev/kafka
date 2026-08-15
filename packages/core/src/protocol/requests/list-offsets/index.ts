import { ISOLATION_LEVEL, type IsolationLevel } from '../../enums/isolation-level.js';
import type { ProtocolFactory, RequestFamily } from '../index.js';
import { REPLICA_ID, type ListOffsetsTopicOptions, withDefaultTimestamps } from './shared.js';
import { listOffsetsRequestV1 } from './v1/request.js';
import { listOffsetsResponseV1 } from './v1/response.js';
import { listOffsetsRequestV2 } from './v2/request.js';
import { listOffsetsResponseV2 } from './v2/response.js';
import { listOffsetsRequestV3 } from './v3/request.js';
import { listOffsetsResponseV3 } from './v3/response.js';

export interface ListOffsetsOptions {
  replicaId?: number;
  isolationLevel?: IsolationLevel;
  topics: ListOffsetsTopicOptions[];
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<ListOffsetsOptions>>> = {
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
