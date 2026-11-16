import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { shareAcknowledgeRequestV1 } from './v1/request';
import { shareAcknowledgeResponseV1 } from './v1/response';
import { shareAcknowledgeRequestV2 } from './v2/request';
import { shareAcknowledgeResponseV2 } from './v2/response';

export type { ShareAcknowledgementBatch, ShareLeaderIdAndEpoch, ShareNodeEndpoint } from './shared';
export type { ShareAcknowledgeResponseV1Body } from './v1/response';
export type { ShareAcknowledgeResponseV2Body } from './v2/response';

export type ShareAcknowledgeResponseBody =
  import('./v1/response').ShareAcknowledgeResponseV1Body | import('./v2/response').ShareAcknowledgeResponseV2Body;

export interface ShareAcknowledgeBatchInput {
  firstOffset: bigint;
  lastOffset: bigint;
  acknowledgeTypes: number[];
}

export interface ShareAcknowledgePartitionInput {
  partitionIndex: number;
  acknowledgementBatches: ShareAcknowledgeBatchInput[];
}

export interface ShareAcknowledgeTopicInput {
  topicId: Buffer;
  partitions: ShareAcknowledgePartitionInput[];
}

export interface ShareAcknowledgeOptions {
  groupId?: string | null;
  memberId?: string | null;
  shareSessionEpoch: number;
  isRenewAck?: boolean;
  topics: ShareAcknowledgeTopicInput[];
}

const fields = (options: ShareAcknowledgeOptions) => ({
  groupId: options.groupId ?? null,
  memberId: options.memberId ?? null,
  shareSessionEpoch: options.shareSessionEpoch,
  topics: options.topics.map(({ topicId, partitions }) => ({
    topicId,
    partitions: partitions.map(({ partitionIndex, acknowledgementBatches }) => ({
      partitionIndex,
      acknowledgementBatches: acknowledgementBatches.map(({ firstOffset, lastOffset, acknowledgeTypes }) => ({
        firstOffset,
        lastOffset,
        acknowledgeTypes,
      })),
    })),
  })),
});

const v2Fields = (options: ShareAcknowledgeOptions) => ({
  ...fields(options),
  isRenewAck: options.isRenewAck ?? false,
});

const VERSIONS = {
  1: (options) => ({
    request: shareAcknowledgeRequestV1(fields(options)),
    response: shareAcknowledgeResponseV1,
  }),
  2: (options) => ({
    request: shareAcknowledgeRequestV2(v2Fields(options)),
    response: shareAcknowledgeResponseV2,
  }),
} satisfies VersionFactories<ShareAcknowledgeOptions>;

export const ShareAcknowledge = defineRequestFamily(KAFKA_APIS.ShareAcknowledge, VERSIONS);
