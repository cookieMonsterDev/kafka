import type { PartitionMetadata } from '../cluster/index';
import type { CompressionType } from '../protocol/compression/index';
import type { RecordHeaders } from '../protocol/records/record';

/** One record to produce. @see https://kafka.apache.org/43/implementation/messages/ */
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

/** Single-topic produce request. @see https://kafka.apache.org/43/configuration/producer-configs/ */
export interface ProducerRecord {
  topic: string;
  messages: readonly Message[];
  /**
   * Acknowledgments required before the request is complete. `0` none, `1` leader, `-1`/`all` ISR.
   * @see https://kafka.apache.org/43/configuration/producer-configs/#acks
   */
  acks?: number;
  timeout?: number;
  compression?: CompressionType;
}

/** Multi-topic produce request. */
export interface ProducerBatch {
  /**
   * Acknowledgments required before the request is complete. `0` none, `1` leader, `-1`/`all` ISR.
   * @see https://kafka.apache.org/43/configuration/producer-configs/#acks
   */
  acks?: number;
  timeout?: number;
  compression?: CompressionType;
  topicMessages?: readonly TopicMessages[];
}

/**
 * Per-partition Produce response. Offsets are `bigint`; message-set v0/v1 fields are not present.
 *
 * @see https://kafka.apache.org/43/design/protocol/
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

export interface PartitionerBatchArgs {
  topic: string;
  partitionMetadata: readonly PartitionMetadata[];
}

export interface Partitioner {
  (args: PartitionerArgs): number;
  /**
   * Called when the producer starts grouping a new batch for a topic.
   * Custom partitioners may use this to rotate batch-local state.
   */
  onNewBatch?: (args: PartitionerBatchArgs) => void;
}

export type CustomPartitioner = () => Partitioner;
