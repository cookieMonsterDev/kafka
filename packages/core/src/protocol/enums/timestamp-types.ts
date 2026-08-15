export const TIMESTAMP_TYPES = Object.freeze({
  // Timestamp type is unknown.
  NO_TIMESTAMP: -1,
  // Timestamp relates to message creation time as set by a Kafka client.
  CREATE_TIME: 0,
  // Timestamp relates to the time a message was appended to a Kafka log.
  LOG_APPEND_TIME: 1,
});

export type TimestampType = (typeof TIMESTAMP_TYPES)[keyof typeof TIMESTAMP_TYPES];
