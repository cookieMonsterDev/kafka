import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import { compactArray, compactString, field, flexibleObject, int16, int32, int64 } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';

export interface DescribeTransactionsTopic {
  topic: string;
  partitions: number[];
}

export interface DescribeTransactionsState {
  errorCode: number;
  transactionalId: string;
  transactionState: string;
  transactionTimeoutMs: number;
  transactionStartTimeMs: bigint;
  producerId: bigint;
  producerEpoch: number;
  topics: DescribeTransactionsTopic[];
}

export interface DescribeTransactionsResponseV0Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  transactionStates: DescribeTransactionsState[];
}

/**
 * DescribeTransactions Response (Version: 0) => throttle_time_ms [transaction_states] TAG_BUFFER
 *   transaction_states => error_code transactional_id transaction_state transaction_timeout_ms
 *                         transaction_start_time_ms producer_id producer_epoch [topics] TAG_BUFFER
 *   topics => topic [partitions] TAG_BUFFER
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const topicSchema = flexibleObject([field('topic', compactString), field('partitions', compactArray(int32))]);
const transactionStateSchema = flexibleObject([
  field('errorCode', int16),
  field('transactionalId', compactString),
  field('transactionState', compactString),
  field('transactionTimeoutMs', int32),
  field('transactionStartTimeMs', int64),
  field('producerId', int64),
  field('producerEpoch', int16),
  field('topics', compactArray(topicSchema)),
]);
export const responseSchema = flexibleObject([
  field('throttleTime', int32),
  field('transactionStates', compactArray(transactionStateSchema)),
]);

export const describeTransactionsResponseV0: ResponseDefinition<DescribeTransactionsResponseV0Body> = {
  decode: async (rawData) => {
    const decoded = responseSchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    const failedTransaction = data.transactionStates.find(({ errorCode }) => failure(errorCode));
    if (failedTransaction) throw createErrorFromCode(failedTransaction.errorCode);
    return data;
  },
};
