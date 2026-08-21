import type { ProtocolFactory, RequestFamily } from '../index';
import { produceRequestV0 } from './v0/request';
import { produceResponseV0 } from './v0/response';
import { produceRequestV1 } from './v1/request';
import { produceResponseV1 } from './v1/response';
import { produceRequestV2 } from './v2/request';
import { produceResponseV2 } from './v2/response';
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
import { produceRequestV8 } from './v8/request';
import { produceResponseV8 } from './v8/response';
import { produceRequestV9 } from './v9/request';
import { produceResponseV9 } from './v9/response';
import { produceRequestV10 } from './v10/request';
import { produceResponseV10 } from './v10/response';
import { produceRequestV11 } from './v11/request';
import { produceResponseV11 } from './v11/response';
import { produceRequestV12 } from './v12/request';
import { produceResponseV12 } from './v12/response';
import { produceRequestV13 } from './v13/request';
import { produceResponseV13 } from './v13/response';
import type { ProduceRequestOptions } from './shared';

const VERSIONS: Readonly<Record<number, ProtocolFactory<ProduceRequestOptions>>> = {
  0: (options) => ({ request: produceRequestV0(options), response: produceResponseV0 }),
  1: (options) => ({ request: produceRequestV1(options), response: produceResponseV1 }),
  2: (options) => ({ request: produceRequestV2(options), response: produceResponseV2 }),
  3: (options) => ({ request: produceRequestV3(options), response: produceResponseV3 }),
  4: (options) => ({ request: produceRequestV4(options), response: produceResponseV4 }),
  5: (options) => ({ request: produceRequestV5(options), response: produceResponseV5 }),
  6: (options) => ({ request: produceRequestV6(options), response: produceResponseV6 }),
  7: (options) => ({ request: produceRequestV7(options), response: produceResponseV7 }),
  8: (options) => ({ request: produceRequestV8(options), response: produceResponseV8 }),
  9: (options) => ({ request: produceRequestV9(options), response: produceResponseV9 }),
  10: (options) => ({ request: produceRequestV10(options), response: produceResponseV10 }),
  11: (options) => ({ request: produceRequestV11(options), response: produceResponseV11 }),
  12: (options) => ({ request: produceRequestV12(options), response: produceResponseV12 }),
  13: (options) => ({ request: produceRequestV13(options), response: produceResponseV13(options) }),
};

/**
 * v0–v2 send MessageSet (magic 0/1, Kafka 0.10). v3+ send RecordBatch v2 (KIP-98).
 * v8 adds record-level errors (KIP-467). v9+ is flexible (KIP-482). v11–v12 add
 * TRANSACTION_ABORTABLE (KIP-890) with the same request body as v10. v13 replaces
 * topic names with topic IDs (KIP-516). Lookup picks the highest overlapping version,
 * so Kafka 4.0 brokers that still advertise `minVersion: 0` (`KAFKA-18659`) negotiate
 * v12, not v2.
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
