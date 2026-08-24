import { KafkaPartialMessageError, KafkaUnsupportedMagicByteInMessageSet } from '../../errors';
import { lookupCodecByAttributes } from '../compression/index';
import { Decoder } from '../decoder';
import { TIMESTAMP_TYPES } from '../enums/timestamp-types';
import { decodeMessage, type DecodedMessage } from '../message/decoder';
import type { RecordBatchContext } from '../records/record';
import type { DecodedRecord } from '../records/record';

/**
 * MessageSet => [Offset MessageSize Message]
 *   Offset => int64
 *   MessageSize => int32
 *   Message => Bytes
 */

export interface DecodedMessageSetRecord extends DecodedRecord {
  size: number;
  crc: number;
}

function messageSetBatchContext(message: { offset: bigint; timestamp: bigint; magicByte: number }): RecordBatchContext {
  return {
    firstOffset: message.offset,
    firstTimestamp: message.timestamp,
    partitionLeaderEpoch: -1,
    inTransaction: false,
    isControlBatch: false,
    lastOffsetDelta: 0,
    producerId: -1n,
    producerEpoch: -1,
    firstSequence: -1,
    maxTimestamp: message.timestamp,
    timestampType: TIMESTAMP_TYPES.CREATE_TIME,
    magicByte: message.magicByte,
  };
}

function toDecodedRecord(message: DecodedMessage & { timestamp?: bigint }): DecodedMessageSetRecord {
  const timestamp = message.timestamp ?? 0n;
  const offset = message.offset;
  return {
    offset,
    size: message.size,
    crc: message.crc,
    magicByte: message.magicByte,
    attributes: message.attributes,
    timestamp,
    key: message.key,
    value: message.value,
    headers: {},
    isControlRecord: false,
    batchContext: messageSetBatchContext({ offset, timestamp, magicByte: message.magicByte }),
    byteSize: message.size,
  };
}

function decodeEntry(decoder: Decoder, checkCrcs: boolean): DecodedMessage {
  if (!decoder.canReadInt64()) {
    throw new KafkaPartialMessageError(
      `Tried to decode a partial message: There isn't enough bytes to read the offset`,
    );
  }

  const offset = decoder.readInt64();

  if (!decoder.canReadInt32()) {
    throw new KafkaPartialMessageError(
      `Tried to decode a partial message: There isn't enough bytes to read the message size`,
    );
  }

  const size = decoder.readInt32();
  return decodeMessage(offset, size, decoder, checkCrcs);
}

function decodeEntries(
  decoder: Decoder,
  compressedMessage: DecodedMessageSetRecord,
  checkCrcs: boolean,
): DecodedMessageSetRecord[] {
  const messages: DecodedMessageSetRecord[] = [];

  while (decoder.offset < decoder.buffer.length) {
    messages.push(toDecodedRecord(decodeEntry(decoder, checkCrcs)));
  }

  if (compressedMessage.magicByte > 0 && compressedMessage.offset >= 0n) {
    const lastMessage = messages.at(-1);
    if (lastMessage == null) return messages;

    const baseOffset = compressedMessage.offset - lastMessage.offset;

    for (const message of messages) {
      const offset = message.offset + baseOffset;
      message.offset = offset;
      message.batchContext = messageSetBatchContext({
        offset,
        timestamp: message.timestamp,
        magicByte: message.magicByte,
      });
    }
  }

  return messages;
}

/**
 * Decode a length-prefixed MessageSet. When `size` is provided the length has already been
 * read (Fetch v4+ magic-byte dispatch). Partial trailing messages and mixed-format upgrades
 * (magic 2 inside a MessageSet) stop the loop so the next fetch can finish the batch.
 */
export async function decodeMessageSet(
  primaryDecoder: Decoder,
  size?: number,
  checkCrcs = true,
): Promise<DecodedMessageSetRecord[]> {
  const messages: DecodedMessageSetRecord[] = [];
  const messageSetSize = size ?? primaryDecoder.readInt32();
  const messageSetDecoder = primaryDecoder.slice(messageSetSize);

  while (messageSetDecoder.offset < messageSetSize) {
    try {
      const message = toDecodedRecord(decodeEntry(messageSetDecoder, checkCrcs));
      const codec = lookupCodecByAttributes(message.attributes);

      if (codec) {
        if (message.value == null) {
          throw new Error('Invariant violated: compressed MessageSet entry has a null value');
        }
        const buffer = await codec.decompress(message.value);
        messages.push(...decodeEntries(new Decoder(buffer), message, checkCrcs));
      } else {
        messages.push(message);
      }
    } catch (e) {
      if (e instanceof KafkaPartialMessageError) {
        // minBytes was probably too low; the tail is an incomplete message.
        break;
      }

      if (e instanceof KafkaUnsupportedMagicByteInMessageSet) {
        // Received a MessageSet and a RecordBatch on the same response; the cluster is
        // probably upgrading the message format from 0.10 to 0.11. Stop processing this
        // message set so the full record batch arrives on the next request.
        break;
      }

      throw e;
    }
  }

  primaryDecoder.forward(messageSetSize);
  return messages;
}
