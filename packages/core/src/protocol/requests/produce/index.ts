import type { ProtocolFactory, RequestFamily } from '../index';
import { produceRequestV3 } from './v3/request';
import { produceResponseV3 } from './v3/response';
import { produceRequestV4 } from './v4/request';
import { produceResponseV4 } from './v4/response';
import { produceRequestV5 } from './v5/request';
import { produceResponseV5 } from './v5/response';
import { produceRequestV6 } from './v6/request';
import { produceResponseV6 } from './v6/response';
import { produceRequestV7 } from './v7/request';
import { produceResponseV7 } from './v7/response';
import type { ProduceRequestOptions } from './shared';

const VERSIONS: Readonly<Record<number, ProtocolFactory<ProduceRequestOptions>>> = {
  3: (options) => ({ request: produceRequestV3(options), response: produceResponseV3 }),
  4: (options) => ({ request: produceRequestV4(options), response: produceResponseV4 }),
  5: (options) => ({ request: produceRequestV5(options), response: produceResponseV5 }),
  6: (options) => ({ request: produceRequestV6(options), response: produceResponseV6 }),
  7: (options) => ({ request: produceRequestV7(options), response: produceResponseV7 }),
};

/**
 * Versions 0-2 are not implemented. Kafka 4.0+ requires Produce v3+ (RecordBatch v2 / KIP-98).
 * Brokers may still advertise `minVersion: 0` as a compatibility shim (`KAFKA-18659`); those
 * older versions are not decodable here.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 * @see https://kafka.apache.org/43/implementation/messages/
 */
export const Produce: RequestFamily<ProduceRequestOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no Produce protocol for version ${version}`);
    return factory;
  },
});
