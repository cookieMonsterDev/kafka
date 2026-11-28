/**
 * Which offset a ListOffsets lookup returns for a partition. Each value is the sentinel timestamp
 * the request sends in place of a real one; a lookup by real timestamp sends the timestamp itself.
 * A spec that needs a newer ListOffsets version than the broker supports throws
 * `KafkaServerDoesNotSupportApiKey` instead of being read as a timestamp.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage
 */
export const OFFSET_SPECS = Object.freeze({
  /** The end of the partition: the high watermark, or the last stable offset under `read_committed`. */
  LATEST: -1n,
  /** The log start offset: the first offset the partition still keeps. */
  EARLIEST: -2n,
  /** The offset of the message with the largest timestamp. ListOffsets v7+ (Kafka 3.0, KIP-734). */
  MAX_TIMESTAMP: -3n,
  /**
   * The first offset kept on the leader's local disk; earlier offsets are only in tiered storage.
   * The same as `EARLIEST` without tiered storage. ListOffsets v8+ (Kafka 3.5, KIP-405).
   */
  EARLIEST_LOCAL: -4n,
  /**
   * The last offset copied to tiered storage, or `-1n` when tiered storage is off or nothing is
   * copied yet. ListOffsets v9+ (Kafka 3.9, KIP-1005).
   */
  LATEST_TIERED: -5n,
  /**
   * The first offset not yet copied to tiered storage, or `-1n` when tiered storage is off or the
   * leader does not know yet what was copied. ListOffsets v11+ (Kafka 4.2, KIP-1023).
   */
  EARLIEST_PENDING_UPLOAD: -6n,
} as const);

export type OffsetSpec = (typeof OFFSET_SPECS)[keyof typeof OFFSET_SPECS];
