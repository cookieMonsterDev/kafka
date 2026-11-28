import type { ApiVersionView } from '../../shared/contracts/cluster';

/**
 * Whether Studio's client can use one Kafka API with a broker: `usable` at its negotiated version,
 * `not-implemented` by the client, `unsupported` by the broker, or `no-common-version` when both
 * have the API but share no version.
 */
export type ApiVersionStatus = 'usable' | 'not-implemented' | 'unsupported' | 'no-common-version';

type ApiVersionFacts = Pick<ApiVersionView, 'brokerVersions' | 'isImplemented' | 'version'>;

export function apiVersionStatus(api: ApiVersionFacts): ApiVersionStatus {
  if (api.brokerVersions === null) return 'unsupported';
  if (!api.isImplemented) return 'not-implemented';
  return api.version === null ? 'no-common-version' : 'usable';
}

/** How many of `apis` have each status. */
export function countApiVersionStatuses(apis: readonly ApiVersionFacts[]): Record<ApiVersionStatus, number> {
  const counts: Record<ApiVersionStatus, number> = {
    usable: 0,
    'not-implemented': 0,
    unsupported: 0,
    'no-common-version': 0,
  };
  for (const api of apis) counts[apiVersionStatus(api)] += 1;
  return counts;
}
