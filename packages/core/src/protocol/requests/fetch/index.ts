import type { ProtocolFactory, RequestFamily } from '../index';
import { fetchRequestV4 } from './v4/request';
import { fetchResponseV4 } from './v4/response';
import { fetchRequestV5 } from './v5/request';
import { fetchResponseV5 } from './v5/response';
import { fetchRequestV6 } from './v6/request';
import { fetchResponseV6 } from './v6/response';
import { fetchRequestV7 } from './v7/request';
import { fetchResponseV7 } from './v7/response';
import { fetchRequestV8 } from './v8/request';
import { fetchResponseV8 } from './v8/response';
import { fetchRequestV9 } from './v9/request';
import { fetchResponseV9 } from './v9/response';
import { fetchRequestV10 } from './v10/request';
import { fetchResponseV10 } from './v10/response';
import { fetchRequestV11 } from './v11/request';
import { fetchResponseV11 } from './v11/response';
import type { FetchRequestOptions } from './shared';

const VERSIONS: Readonly<Record<number, ProtocolFactory<FetchRequestOptions>>> = {
  4: (options) => ({ request: fetchRequestV4(options), response: fetchResponseV4 }),
  5: (options) => ({ request: fetchRequestV5(options), response: fetchResponseV5 }),
  6: (options) => ({ request: fetchRequestV6(options), response: fetchResponseV6 }),
  7: (options) => ({ request: fetchRequestV7(options), response: fetchResponseV7 }),
  8: (options) => ({ request: fetchRequestV8(options), response: fetchResponseV8 }),
  9: (options) => ({ request: fetchRequestV9(options), response: fetchResponseV9 }),
  10: (options) => ({ request: fetchRequestV10(options), response: fetchResponseV10 }),
  11: (options) => ({ request: fetchRequestV11(options), response: fetchResponseV11 }),
};

/**
 * Versions 0-3 are not implemented. Kafka 4.0+ requires Fetch v4+ (RecordBatch v2 / KIP-98).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 * @see https://kafka.apache.org/43/implementation/messages/
 */
export const Fetch: RequestFamily<FetchRequestOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no Fetch protocol for version ${version}`);
    return factory;
  },
});
