import type { PartitionMetadata } from '../cluster/index';
import type { CompressionType } from '../protocol/compression/index';
import type { RecordHeaders } from '../protocol/records/record';
import type { NodeLatencyReader } from './node-latency-tracker';

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
  /** Per-node Produce latency, for partitioners that bias routing toward faster leaders. */
  nodeLatency?: NodeLatencyReader;
}

export interface PartitionerBatchArgs {
  topic: string;
  partitionMetadata: readonly PartitionMetadata[];
  /** Per-node Produce latency, for partitioners that bias routing toward faster leaders. */
  nodeLatency?: NodeLatencyReader;
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

/**
 * Payload for {@link ProducerHooks.onSend}. Fires once per `send()`/`sendBatch()` call - not per
 * message and not per physical Produce request sent to a broker (a call may be split across
 * several broker requests, or coalesced with other calls under `lingerMs`). `topicMessages` is
 * the merged, post-validation view of what that one call is about to hand off.
 */
export interface ProducerSendHookEvent {
  topicMessages: readonly TopicMessages[];
  acks: number;
  timeout: number;
  compression?: CompressionType;
}

/**
 * Payload for {@link ProducerHooks.onAck}. Fires once per `send()`/`sendBatch()` call, after that
 * call settles: `metadata` is set on success (mirrors the call's resolved value), `error` is set
 * if the call rejected. Exactly one of the two is present.
 */
export interface ProducerAckHookEvent extends ProducerSendHookEvent {
  metadata?: RecordMetadata[];
  error?: unknown;
}

export type ProducerSendHook = (event: ProducerSendHookEvent) => void | Promise<void>;
export type ProducerAckHook = (event: ProducerAckHookEvent) => void | Promise<void>;

/**
 * User-supplied hooks for a producer's send path. These are plain ordered async callbacks, not
 * an interceptor SPI: each array runs in registration order, one hook is always awaited before
 * the next starts, and a hook that throws is caught and logged - it never fails or alters the
 * underlying `send()`/`sendBatch()` outcome.
 */
export interface ProducerHooks {
  /** Before the record(s) are dispatched to a broker (or enqueued, if `lingerMs > 0`). */
  onSend?: readonly ProducerSendHook[];
  /** After the call settles: `metadata` on success, `error` on failure. */
  onAck?: readonly ProducerAckHook[];
}
