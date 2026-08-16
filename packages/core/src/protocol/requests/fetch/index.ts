import type { ProtocolFactory, ProtocolResult, RequestFamily } from '../index';
import { fetchRequestV0 } from './v0/request';
import { fetchResponseV0 } from './v0/response';
import { fetchRequestV1 } from './v1/request';
import { fetchResponseV1 } from './v1/response';
import { fetchRequestV2 } from './v2/request';
import { fetchResponseV2 } from './v2/response';
import { fetchRequestV3 } from './v3/request';
import { fetchResponseV3 } from './v3/response';
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
import { fetchRequestV12 } from './v12/request';
import { fetchResponseV12 } from './v12/response';
import type { FetchRequestOptions } from './shared';

/**
 * Fetch can block up to `maxWaitTime`, which may exceed the connection `requestTimeout`.
 * Add a small network delay so the socket wait covers the broker-side wait.
 */
const NETWORK_DELAY = 100;

function fetchRequestTimeout(maxWaitTime: number): number {
  return Number.isSafeInteger(maxWaitTime + NETWORK_DELAY) ? maxWaitTime + NETWORK_DELAY : maxWaitTime;
}

function fetchProtocol(
  request: ProtocolResult['request'],
  response: ProtocolResult['response'],
  maxWaitTime: number,
): ProtocolResult {
  return {
    request,
    response,
    requestTimeout: fetchRequestTimeout(maxWaitTime),
  };
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<FetchRequestOptions>>> = {
  0: (options) => fetchProtocol(fetchRequestV0(options), fetchResponseV0, options.maxWaitTime),
  1: (options) => fetchProtocol(fetchRequestV1(options), fetchResponseV1, options.maxWaitTime),
  2: (options) => fetchProtocol(fetchRequestV2(options), fetchResponseV2, options.maxWaitTime),
  3: (options) => fetchProtocol(fetchRequestV3(options), fetchResponseV3, options.maxWaitTime),
  4: (options) => fetchProtocol(fetchRequestV4(options), fetchResponseV4, options.maxWaitTime),
  5: (options) => fetchProtocol(fetchRequestV5(options), fetchResponseV5, options.maxWaitTime),
  6: (options) => fetchProtocol(fetchRequestV6(options), fetchResponseV6, options.maxWaitTime),
  7: (options) => fetchProtocol(fetchRequestV7(options), fetchResponseV7, options.maxWaitTime),
  8: (options) => fetchProtocol(fetchRequestV8(options), fetchResponseV8, options.maxWaitTime),
  9: (options) => fetchProtocol(fetchRequestV9(options), fetchResponseV9, options.maxWaitTime),
  10: (options) => fetchProtocol(fetchRequestV10(options), fetchResponseV10, options.maxWaitTime),
  11: (options) => fetchProtocol(fetchRequestV11(options), fetchResponseV11, options.maxWaitTime),
  12: (options) => fetchProtocol(fetchRequestV12(options), fetchResponseV12, options.maxWaitTime),
};

/**
 * v0–v3 decode MessageSet only. v4+ probes the magic byte and dispatches to MessageSet
 * (magic 0/1) or RecordBatch (magic 2), including mixed-format responses during a cluster
 * upgrade from 0.10 to 0.11.
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
