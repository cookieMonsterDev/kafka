import type { ProtocolFactory, RequestFamily } from '../index';
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

const VERSIONS: Readonly<Record<number, ProtocolFactory<InitProducerIdOptions>>> = {
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
};

/**
 * Kafka 2.4 advertised InitProducerId maxVersion=2 where v2 was the first flexible version
 * (KIP-482) and had no producer_id / producer_epoch. Kafka 2.5 redefined v2 as non-flexible
 * with those KIP-360 fields and moved flexible encoding to v3. Sending the 2.5+ v2 body to a
 * 2.4 broker fails to parse and the broker closes the connection.
 *
 * Skip v2: Kafka 2.4 (max 2) negotiates v1; Kafka 2.5+ (max 3+) negotiates v3 or v4.
 * The v2 factory stays so encoder tests can still exercise the 2.5+ layout.
 */
export const InitProducerId: RequestFamily<InitProducerIdOptions> = Object.freeze({
  versions: Object.freeze([0, 1, 3, 4]),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no InitProducerId protocol for version ${version}`);
    return factory;
  },
});
