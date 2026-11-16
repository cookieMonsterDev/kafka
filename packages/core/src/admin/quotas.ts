import { KafkaNonRetriableError } from '../errors';
import type { AlterClientQuotasEntry } from '../protocol/requests/alter-client-quotas/index';
import type { AlterClientQuotasResponseV1Body } from '../protocol/requests/alter-client-quotas/v1/response';
import type { DescribeClientQuotasComponent } from '../protocol/requests/describe-client-quotas/index';
import type { DescribeClientQuotasResponseV1Body } from '../protocol/requests/describe-client-quotas/v1/response';
import { CONTROLLER } from '../cluster/run';
import { retrier as createRetrier } from '../retry/index';
import type { AdminContext } from './helpers';
import { formatUnknown } from './helpers';
import { AlterClientQuotas, DescribeClientQuotas } from '../protocol/requests/lazy-families';

export interface QuotasApi {
  describeClientQuotas: (options?: {
    components?: DescribeClientQuotasComponent[];
    strict?: boolean;
  }) => Promise<{ entries: DescribeClientQuotasResponseV1Body['entries'] }>;
  alterClientQuotas: (options: {
    entries: AlterClientQuotasEntry[];
    validateOnly?: boolean;
  }) => Promise<{ entries: AlterClientQuotasResponseV1Body['entries'] }>;
}

export function createQuotasApi({ cluster, retry }: AdminContext): QuotasApi {
  const retrier = createRetrier(retry);

  const describeClientQuotas = async (
    options: { components?: DescribeClientQuotasComponent[]; strict?: boolean } = {},
  ): Promise<{ entries: DescribeClientQuotasResponseV1Body['entries'] }> => {
    const components = options.components ?? [];
    if (!Array.isArray(components)) {
      throw new KafkaNonRetriableError(`Invalid quota components ${formatUnknown(components)}`);
    }

    return cluster.run(
      CONTROLLER,
      async (broker) => {
        const { entries } = await broker.send(DescribeClientQuotas, {
          components,
          strict: options.strict ?? false,
        });
        return { entries };
      },
      { retrier, refreshMetadata: true, description: 'Could not describe client quotas' },
    );
  };

  const alterClientQuotas = async ({
    entries,
    validateOnly,
  }: {
    entries: AlterClientQuotasEntry[];
    validateOnly?: boolean;
  }): Promise<{ entries: AlterClientQuotasResponseV1Body['entries'] }> => {
    if (!Array.isArray(entries) || entries.length === 0) {
      throw new KafkaNonRetriableError('Entries array cannot be empty');
    }

    return cluster.run(
      CONTROLLER,
      async (broker) => {
        const response = await broker.send(AlterClientQuotas, { entries, validateOnly: validateOnly ?? false });
        return { entries: response.entries };
      },
      { retrier, refreshMetadata: true, description: 'Could not alter client quotas' },
    );
  };

  return { describeClientQuotas, alterClientQuotas };
}
