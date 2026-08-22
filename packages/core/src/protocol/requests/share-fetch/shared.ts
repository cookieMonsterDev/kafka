import { compactArray, field, flexibleObject, int64, int8 } from '../../schema';

export interface ShareAcknowledgementBatch {
  firstOffset: bigint;
  lastOffset: bigint;
  acknowledgeTypes: number[];
}

export const acknowledgementBatchSchema = flexibleObject([
  field('firstOffset', int64),
  field('lastOffset', int64),
  field('acknowledgeTypes', compactArray(int8)),
]);

export interface ShareLeaderIdAndEpoch {
  leaderId: number;
  leaderEpoch: number;
}

export interface ShareNodeEndpoint {
  nodeId: number;
  host: string;
  port: number;
  rack: string | null;
}

export interface ShareAcquiredRecords {
  firstOffset: bigint;
  lastOffset: bigint;
  deliveryCount: number;
}
