import type { ProtocolFactory, RequestFamily } from '../index.js';
import { fetchRequestV4 } from './v4/request.js';
import { fetchResponseV4 } from './v4/response.js';
import { fetchRequestV5 } from './v5/request.js';
import { fetchResponseV5 } from './v5/response.js';
import { fetchRequestV6 } from './v6/request.js';
import { fetchResponseV6 } from './v6/response.js';
import { fetchRequestV7 } from './v7/request.js';
import { fetchResponseV7 } from './v7/response.js';
import { fetchRequestV8 } from './v8/request.js';
import { fetchResponseV8 } from './v8/response.js';
import { fetchRequestV9 } from './v9/request.js';
import { fetchResponseV9 } from './v9/response.js';
import { fetchRequestV10 } from './v10/request.js';
import { fetchResponseV10 } from './v10/response.js';
import { fetchRequestV11 } from './v11/request.js';
import { fetchResponseV11 } from './v11/response.js';
import type { FetchRequestOptions } from './shared.js';

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
 * Versions 0-3 are not implemented: Kafka 4.0.0's real floor for this API is v4
 * (`FetchRequest.json`'s `validVersions` is `"4-17"`) - the version RecordBatch v2 (KIP-98) and
 * transactional fetching became mandatory. The broker's live `ApiVersionsResponse` advertises
 * `minVersion: 4` for this API directly (unlike `Produce`, there's no librdkafka compatibility
 * override here), so the advertised floor is the real one.
 */
export const Fetch: RequestFamily<FetchRequestOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no Fetch protocol for version ${version}`);
    return factory;
  },
});
