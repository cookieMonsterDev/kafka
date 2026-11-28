import { defineRequestFamily, highestVersionBelow, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { endTxnRequestV0 } from './v0/request';
import { endTxnResponseV0 } from './v0/response';
import { endTxnRequestV1 } from './v1/request';
import { endTxnResponseV1 } from './v1/response';
import { endTxnRequestV2 } from './v2/request';
import { endTxnResponseV2 } from './v2/response';
import { endTxnRequestV3 } from './v3/request';
import { endTxnResponseV3 } from './v3/response';
import { endTxnRequestV4 } from './v4/request';
import { endTxnResponseV4 } from './v4/response';
import { endTxnRequestV5 } from './v5/request';
import { endTxnResponseV5 } from './v5/response';

/** First EndTxn version that ends a transaction of transaction version 2 (KIP-890 part 2). */
export const END_TXN_TRANSACTION_V2_MIN_VERSION = 5;

export interface EndTxnOptions {
  transactionalId: string;
  producerId: bigint;
  producerEpoch: number;
  transactionResult: boolean;
  /**
   * Whether the transaction runs under transaction version 2. Only then is v5 sent, which makes the
   * coordinator bump the producer epoch and return the new producer id and epoch. Defaults to false.
   */
  isTransactionV2Enabled?: boolean;
}

const VERSIONS = {
  0: (values) => ({ request: endTxnRequestV0(values), response: endTxnResponseV0 }),
  1: (values) => ({ request: endTxnRequestV1(values), response: endTxnResponseV1 }),
  2: (values) => ({ request: endTxnRequestV2(values), response: endTxnResponseV2 }),
  3: (values) => ({ request: endTxnRequestV3(values), response: endTxnResponseV3 }),
  4: (values) => ({ request: endTxnRequestV4(values), response: endTxnResponseV4 }),
  5: (values) => ({ request: endTxnRequestV5(values), response: endTxnResponseV5 }),
} satisfies VersionFactories<EndTxnOptions>;

const END_TXN_VERSIONS: readonly number[] = Object.keys(VERSIONS).map(Number);

/** An EndTxn response, for every version. */
export interface EndTxnResponse {
  errorCode: number;
  /** From v5: the producer id for the next transaction, `-1` when the request failed. */
  producerId?: bigint;
  /** From v5: the producer epoch the coordinator bumped to as it ended the transaction. */
  producerEpoch?: number;
}

/**
 * v4 (Kafka 3.8+) tells the broker the client handles TRANSACTION_ABORTABLE (KIP-890). The broker
 * reads v5 as transaction version 2, so v5 is sent only when the transaction runs under it, as in
 * the Java client.
 */
export const EndTxn = defineRequestFamily<typeof VERSIONS, EndTxnResponse>(KAFKA_APIS.EndTxn, VERSIONS, {
  preferVersion: (options, version, advertised) =>
    version < END_TXN_TRANSACTION_V2_MIN_VERSION || options.isTransactionV2Enabled === true
      ? version
      : highestVersionBelow(KAFKA_APIS.EndTxn, END_TXN_VERSIONS, END_TXN_TRANSACTION_V2_MIN_VERSION, advertised),
});
