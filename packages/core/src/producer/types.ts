import type { PartitionMetadata } from '../cluster/index.js';
import type { CompressionType } from '../protocol/compression/index.js';
import type { RecordHeaders } from '../protocol/records/record.js';

export interface Message {
  key?: Buffer | string | null;
  value: Buffer | string | null;
  partition?: number;
  headers?: RecordHeaders;
  /** Milliseconds since epoch. Defaults to the time the message is sent. */
  timestamp?: number;
}

export interface TopicMessages {
  topic: string;
  messages: readonly Message[];
}

export interface ProducerRecord {
  topic: string;
  messages: readonly Message[];
  acks?: number;
  timeout?: number;
  compression?: CompressionType;
}

export interface ProducerBatch {
  acks?: number;
  timeout?: number;
  compression?: CompressionType;
  topicMessages?: readonly TopicMessages[];
}

/**
 * Every field the Produce response can carry for a partition (`RecordMetadata.offset`/`timestamp`
 * only ever existed on the legacy message-set response, dropped along with message-set v0/v1 -
 * see the port's D3 decision).
 */
export interface RecordMetadata {
  topicName: string;
  partition: number;
  errorCode: number;
  baseOffset: bigint;
  logAppendTime: bigint;
  logStartOffset: bigint;
}

export interface PartitionerArgs {
  topic: string;
  partitionMetadata: readonly PartitionMetadata[];
  message: Message;
}

export type CustomPartitioner = () => (args: PartitionerArgs) => number;
