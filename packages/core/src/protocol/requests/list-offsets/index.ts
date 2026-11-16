import { ISOLATION_LEVEL, type IsolationLevel } from '../../enums/isolation-level';
import { defineRequestFamily, type VersionFactories, type VersionResponse } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
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
import { listOffsetsRequestV4 } from './v4/request';
import { listOffsetsResponseV4 } from './v4/response';
import { listOffsetsRequestV5 } from './v5/request';
import { listOffsetsResponseV5 } from './v5/response';
import { listOffsetsRequestV6 } from './v6/request';
import { listOffsetsResponseV6 } from './v6/response';
import { listOffsetsRequestV7 } from './v7/request';
import { listOffsetsResponseV7 } from './v7/response';
import { listOffsetsRequestV8 } from './v8/request';
import { listOffsetsResponseV8 } from './v8/response';

export interface ListOffsetsOptions {
  replicaId?: number;
  isolationLevel?: IsolationLevel;
  topics: ListOffsetsTopicOptions[];
}

const VERSIONS = {
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
  4: ({ replicaId = REPLICA_ID, isolationLevel = ISOLATION_LEVEL.READ_COMMITTED, topics }) => ({
    request: listOffsetsRequestV4({ replicaId, isolationLevel, topics: withDefaultTimestamps(topics) }),
    response: listOffsetsResponseV4,
  }),
  5: ({ replicaId = REPLICA_ID, isolationLevel = ISOLATION_LEVEL.READ_COMMITTED, topics }) => ({
    request: listOffsetsRequestV5({ replicaId, isolationLevel, topics: withDefaultTimestamps(topics) }),
    response: listOffsetsResponseV5,
  }),
  6: ({ replicaId = REPLICA_ID, isolationLevel = ISOLATION_LEVEL.READ_COMMITTED, topics }) => ({
    request: listOffsetsRequestV6({ replicaId, isolationLevel, topics: withDefaultTimestamps(topics) }),
    response: listOffsetsResponseV6,
  }),
  7: ({ replicaId = REPLICA_ID, isolationLevel = ISOLATION_LEVEL.READ_COMMITTED, topics }) => ({
    request: listOffsetsRequestV7({ replicaId, isolationLevel, topics: withDefaultTimestamps(topics) }),
    response: listOffsetsResponseV7,
  }),
  8: ({ replicaId = REPLICA_ID, isolationLevel = ISOLATION_LEVEL.READ_COMMITTED, topics }) => ({
    request: listOffsetsRequestV8({ replicaId, isolationLevel, topics: withDefaultTimestamps(topics) }),
    response: listOffsetsResponseV8,
  }),
} satisfies VersionFactories<ListOffsetsOptions>;

/** A ListOffsets response in one shape for every version. */
export interface ListOffsetsResponse {
  throttleTime: number;
  clientSideThrottleTime: number;
  responses: {
    topic: string;
    partitions: { partition: number; errorCode: number; timestamp: bigint; offset: bigint; leaderEpoch: number }[];
  }[];
}

/**
 * ListOffsets v0 returns `offsets[]`; v1+ returns a single `offset`. Normalizes to `offset`, and
 * fields an older version lacks take their "unknown" value.
 */
function normalizeListOffsets(body: VersionResponse<typeof VERSIONS>): ListOffsetsResponse {
  return {
    throttleTime: 'throttleTime' in body ? body.throttleTime : 0,
    clientSideThrottleTime: 'clientSideThrottleTime' in body ? body.clientSideThrottleTime : 0,
    responses: body.responses.map((response) => ({
      topic: response.topic,
      partitions: response.partitions.map((partition) => ({
        partition: partition.partition,
        errorCode: partition.errorCode,
        timestamp: 'timestamp' in partition ? partition.timestamp : -1n,
        offset: 'offsets' in partition ? (partition.offsets.at(-1) ?? -1n) : partition.offset,
        leaderEpoch: 'leaderEpoch' in partition ? (partition.leaderEpoch ?? -1) : -1,
      })),
    })),
  };
}

export const ListOffsets = defineRequestFamily(KAFKA_APIS.ListOffsets, VERSIONS, {
  parseResponse: async (body, _options, parse) =>
    normalizeListOffsets((await parse(body)) as VersionResponse<typeof VERSIONS>),
});
