import type { ProtocolFactory, RequestFamily } from '../index.js';
import { produceRequestV3 } from './v3/request.js';
import { produceResponseV3 } from './v3/response.js';
import { produceRequestV4 } from './v4/request.js';
import { produceResponseV4 } from './v4/response.js';
import { produceRequestV5 } from './v5/request.js';
import { produceResponseV5 } from './v5/response.js';
import { produceRequestV6 } from './v6/request.js';
import { produceResponseV6 } from './v6/response.js';
import { produceRequestV7 } from './v7/request.js';
import { produceResponseV7 } from './v7/response.js';
import type { ProduceRequestOptions } from './shared.js';

const VERSIONS: Readonly<Record<number, ProtocolFactory<ProduceRequestOptions>>> = {
  3: (options) => ({ request: produceRequestV3(options), response: produceResponseV3 }),
  4: (options) => ({ request: produceRequestV4(options), response: produceResponseV4 }),
  5: (options) => ({ request: produceRequestV5(options), response: produceResponseV5 }),
  6: (options) => ({ request: produceRequestV6(options), response: produceResponseV6 }),
  7: (options) => ({ request: produceRequestV7(options), response: produceResponseV7 }),
};

/**
 * Versions 0-2 are not implemented: Kafka 4.0.0's real floor for this API is v3
 * (`ProduceRequest.json`'s `validVersions` is `"3-12"`), the version RecordBatch v2 (KIP-98)
 * became mandatory. The broker's live `ApiVersionsResponse` advertises `minVersion: 0` for this
 * one API as a deliberate compatibility shim for old librdkafka clients (`KAFKA-18659`) - it does
 * not mean v0-2 are actually decodable.
 */
export const Produce: RequestFamily<ProduceRequestOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no Produce protocol for version ${version}`);
    return factory;
  },
});
