import type { KafkaMessage } from '../consumer/types';
import type { DecodedRecordBatch } from '../protocol/records/batch';
import type { ShareAcquiredRecords } from '../protocol/requests/share-fetch/shared';

export interface ShareBatchPartitionData {
  partition: number;
  messages: DecodedRecordBatch['records'];
  acquiredRecords: ShareAcquiredRecords[];
}

/**
 * Messages acquired from a share partition via ShareFetch.
 */
export class ShareBatch {
  readonly topic: string;
  readonly partition: number;
  readonly messages: KafkaMessage[];
  readonly acquiredRecords: ShareAcquiredRecords[];

  constructor(topic: string, { partition, messages, acquiredRecords }: ShareBatchPartitionData) {
    this.topic = topic;
    this.partition = partition;
    // ShareFetch returns whole record batches; only offsets inside an acquired range belong to this member.
    this.messages = messages.filter(
      ({ offset, isControlRecord }) =>
        !isControlRecord &&
        acquiredRecords.some(({ firstOffset, lastOffset }) => offset >= firstOffset && offset <= lastOffset),
    );
    this.acquiredRecords = acquiredRecords;
  }

  isEmpty(): boolean {
    return this.messages.length === 0;
  }
}
