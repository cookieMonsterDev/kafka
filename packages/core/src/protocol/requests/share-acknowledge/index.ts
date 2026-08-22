import type { ProtocolFactory, RequestFamily } from '../index';
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

const VERSIONS: Readonly<Record<number, ProtocolFactory<ShareAcknowledgeOptions>>> = {
  1: (options) => ({
    request: shareAcknowledgeRequestV1(fields(options)),
    response: shareAcknowledgeResponseV1,
  }),
  2: (options) => ({
    request: shareAcknowledgeRequestV2(v2Fields(options)),
    response: shareAcknowledgeResponseV2,
  }),
};

export const ShareAcknowledge: RequestFamily<ShareAcknowledgeOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no ShareAcknowledge protocol for version ${version}`);
    return factory;
  },
});
