import { Decoder } from '../../../decoder';
import { field, flexibleObject, int16, int32, int64 } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import { endTxnResponseV4, type EndTxnResponseV4Body } from '../v4/response';

export interface EndTxnResponseV5Body extends EndTxnResponseV4Body {
  /** The producer id to use for the next transaction; a new one once the epoch overflowed. */
  producerId: bigint;
  /** The producer epoch the coordinator bumped to when it ended the transaction. */
  producerEpoch: number;
}

/**
 * EndTxn Response (Version: 5) => throttle_time_ms error_code producer_id producer_epoch TAG_BUFFER
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   producer_id => INT64
 *   producer_epoch => INT16
 *
 * Transaction version 2 (KIP-890 part 2) bumps the producer epoch on every transaction, so the
 * response carries the producer id and epoch for the next one (`-1` when the request failed).
 * Quota timing follows v1 (KIP-219).
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
const bodySchema = flexibleObject([
  field('throttleTime', int32),
  field('errorCode', int16),
  field('producerId', int64),
  field('producerEpoch', int16),
]);

export const endTxnResponseV5: ResponseDefinition<EndTxnResponseV5Body> = {
  decode: async (rawData) => {
    const decoded = bodySchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await endTxnResponseV4.parse(data);
    return data;
  },
};
