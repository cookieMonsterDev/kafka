import { COMPRESSION_TYPES, type CompressionType } from '../compression/index';
import { Encoder } from '../encoder';
import { encodeMessage, type EncodeMessageOptions, type MessageVersion } from '../message/index';

/**
 * MessageSet => [Offset MessageSize Message]
 *   Offset => int64
 *   MessageSize => int32
 *   Message => Bytes
 *
 * Messages in a message set are not encoded as an array. They are written in sequence.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 * @see https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets
 */
export function encodeMessageSet({
  messageVersion = 0,
  compression = COMPRESSION_TYPES.None,
  entries,
}: {
  messageVersion?: MessageVersion;
  compression?: CompressionType;
  entries: readonly EncodeMessageOptions[];
}): Encoder {
  const isCompressed = compression !== COMPRESSION_TYPES.None;
  const encoder = new Encoder();

  entries.forEach((entry, i) => {
    const message = encodeMessage(messageVersion, entry);

    // When the producer is sending uncompressed messages, offsets can be anything.
    // When sending compressed messages, each inner message uses a relative offset
    // starting at 0 so the broker can skip recompression.
    encoder.writeInt64(isCompressed ? i : -1);
    encoder.writeInt32(message.size());
    encoder.writeEncoder(message, { release: true });
  });

  return encoder;
}
