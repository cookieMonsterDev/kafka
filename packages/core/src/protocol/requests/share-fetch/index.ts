import type { ProtocolFactory, RequestFamily } from '../index';
import { shareFetchRequestV1 } from './v1/request';
import { shareFetchResponseV1 } from './v1/response';

export type {
  ShareAcknowledgementBatch,
  ShareAcquiredRecords,
  ShareLeaderIdAndEpoch,
  ShareNodeEndpoint,
} from './shared';
export type { ShareFetchResponseV1Body } from './v1/response';

/** Share-session epoch 0 opens a session (KIP-932). */
export const SHARE_SESSION_INITIAL_EPOCH = 0;
/** Share-session epoch -1 closes a session (KIP-932). */
export const SHARE_SESSION_CLOSE_EPOCH = -1;

export interface ShareFetchAcknowledgementBatchInput {
  firstOffset: bigint;
  lastOffset: bigint;
  acknowledgeTypes: number[];
}

export interface ShareFetchPartitionInput {
  partitionIndex: number;
  acknowledgementBatches?: ShareFetchAcknowledgementBatchInput[];
}

export interface ShareFetchTopicInput {
  topicId: Buffer;
  partitions: ShareFetchPartitionInput[];
}

export interface ShareFetchForgottenTopicInput {
  topicId: Buffer;
  partitions: number[];
}

export interface ShareFetchOptions {
  groupId?: string | null;
  memberId?: string | null;
  shareSessionEpoch: number;
  maxWaitMs: number;
  minBytes: number;
  maxBytes?: number;
  maxRecords: number;
  batchSize: number;
  topics: ShareFetchTopicInput[];
  forgottenTopics?: ShareFetchForgottenTopicInput[];
}

const DEFAULT_MAX_BYTES = 0x7fffffff;

const fields = (options: ShareFetchOptions) => ({
  groupId: options.groupId ?? null,
  memberId: options.memberId ?? null,
  shareSessionEpoch: options.shareSessionEpoch,
  maxWaitMs: options.maxWaitMs,
  minBytes: options.minBytes,
  maxBytes: options.maxBytes ?? DEFAULT_MAX_BYTES,
  maxRecords: options.maxRecords,
  batchSize: options.batchSize,
  topics: options.topics.map(({ topicId, partitions }) => ({
    topicId,
    partitions: partitions.map(({ partitionIndex, acknowledgementBatches }) => ({
      partitionIndex,
      acknowledgementBatches: (acknowledgementBatches ?? []).map(({ firstOffset, lastOffset, acknowledgeTypes }) => ({
        firstOffset,
        lastOffset,
        acknowledgeTypes,
      })),
    })),
  })),
  forgottenTopics: (options.forgottenTopics ?? []).map(({ topicId, partitions }) => ({ topicId, partitions })),
});

const VERSIONS: Readonly<Record<number, ProtocolFactory<ShareFetchOptions>>> = {
  1: (options) => ({
    request: shareFetchRequestV1(fields(options)),
    response: shareFetchResponseV1,
  }),
};

export const ShareFetch: RequestFamily<ShareFetchOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no ShareFetch protocol for version ${version}`);
    return factory;
  },
});
