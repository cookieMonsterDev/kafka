import { defineRequestFamily, type VersionFactories, type VersionResponse } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { initProducerIdRequestV0 } from './v0/request';
import { initProducerIdResponseV0 } from './v0/response';
import { initProducerIdRequestV1 } from './v1/request';
import { initProducerIdResponseV1 } from './v1/response';
import { initProducerIdRequestV2 } from './v2/request';
import { initProducerIdResponseV2 } from './v2/response';
import { initProducerIdRequestV3 } from './v3/request';
import { initProducerIdResponseV3 } from './v3/response';
import { initProducerIdRequestV4 } from './v4/request';
import { initProducerIdResponseV4 } from './v4/response';
import { initProducerIdRequestV5 } from './v5/request';
import { initProducerIdResponseV5 } from './v5/response';

export interface InitProducerIdOptions {
  transactionalId: string | null;
  transactionTimeout: number;
  producerId?: bigint;
  producerEpoch?: number;
}

function withProducerIdentity(values: InitProducerIdOptions) {
  return {
    transactionalId: values.transactionalId,
    transactionTimeout: values.transactionTimeout,
    producerId: values.producerId ?? -1n,
    producerEpoch: values.producerEpoch ?? -1,
  };
}

const VERSIONS = {
  0: (values) => ({ request: initProducerIdRequestV0(values), response: initProducerIdResponseV0 }),
  1: (values) => ({ request: initProducerIdRequestV1(values), response: initProducerIdResponseV1 }),
  2: (values) => ({
    request: initProducerIdRequestV2(withProducerIdentity(values)),
    response: initProducerIdResponseV2,
  }),
  3: (values) => ({
    request: initProducerIdRequestV3(withProducerIdentity(values)),
    response: initProducerIdResponseV3,
  }),
  4: (values) => ({
    request: initProducerIdRequestV4(withProducerIdentity(values)),
    response: initProducerIdResponseV4,
  }),
  5: (values) => ({
    request: initProducerIdRequestV5(withProducerIdentity(values)),
    response: initProducerIdResponseV5,
  }),
} satisfies VersionFactories<InitProducerIdOptions>;

/** An InitProducerId response, for every version. */
export type InitProducerIdResponse = VersionResponse<typeof VERSIONS>;

/**
 * Kafka 2.4 advertised InitProducerId maxVersion=2 where v2 was the first flexible version
 * (KIP-482) and had no producer_id / producer_epoch. Kafka 2.5 redefined v2 as non-flexible
 * with those KIP-360 fields and moved flexible encoding to v3. Sending the 2.5+ v2 body to a
 * 2.4 broker fails to parse and the broker closes the connection.
 *
 * Skip v2: Kafka 2.4 (max 2) negotiates v1; Kafka 2.5+ (max 3+) negotiates v3 or later. v5 (Kafka
 * 3.8+) tells the broker the client handles TRANSACTION_ABORTABLE (KIP-890).
 * The v2 factory stays so encoder tests can still exercise the 2.5+ layout.
 */
export const InitProducerId = Object.freeze({
  ...defineRequestFamily(KAFKA_APIS.InitProducerId, VERSIONS),
  versions: Object.freeze([0, 1, 3, 4, 5]),
});
