import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import { compactArray, compactString, field, flexibleObject, int16, int32, int64 } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';

export interface ListTransactionsState {
  transactionalId: string;
  producerId: bigint;
  transactionState: string;
}

export interface ListTransactionsResponseV0Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  errorCode: number;
  unknownStateFilters: string[];
  transactionStates: ListTransactionsState[];
}

/**
 * ListTransactions Response (Version: 0) => throttle_time_ms error_code [unknown_state_filters]
 *                                           [transaction_states] TAG_BUFFER
 *   unknown_state_filters => COMPACT_STRING
 *   transaction_states => transactional_id producer_id transaction_state TAG_BUFFER
 *     transactional_id => COMPACT_STRING
 *     producer_id => INT64
 *     transaction_state => COMPACT_STRING
 *
 * Flexible from v0. Quota timing follows KIP-219: the decoded throttle is exposed as
 * `clientSideThrottleTime`.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const transactionStateSchema = flexibleObject([
  field('transactionalId', compactString),
  field('producerId', int64),
  field('transactionState', compactString),
]);
export const responseSchema = flexibleObject([
  field('throttleTime', int32),
  field('errorCode', int16),
  field('unknownStateFilters', compactArray(compactString)),
  field('transactionStates', compactArray(transactionStateSchema)),
]);

export const listTransactionsResponseV0: ResponseDefinition<ListTransactionsResponseV0Body> = {
  decode: async (rawData) => {
    const decoded = responseSchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
};
