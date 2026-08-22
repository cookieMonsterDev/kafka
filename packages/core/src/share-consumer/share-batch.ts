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
    this.messages = messages.filter((message) => !message.isControlRecord);
    this.acquiredRecords = acquiredRecords;
  }

  isEmpty(): boolean {
    return this.messages.length === 0;
  }
}
