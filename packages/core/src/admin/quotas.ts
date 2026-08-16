import { KafkaNonRetriableError } from '../errors';
import type { AlterClientQuotasEntry } from '../protocol/requests/alter-client-quotas/index';
import type { AlterClientQuotasResponseV1Body } from '../protocol/requests/alter-client-quotas/v1/response';
import type { DescribeClientQuotasComponent } from '../protocol/requests/describe-client-quotas/index';
import type { DescribeClientQuotasResponseV1Body } from '../protocol/requests/describe-client-quotas/v1/response';
import { retrier } from '../retry/index';
import type { AdminContext } from './helpers';
import { formatUnknown, protocolType } from './helpers';

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

export function createQuotasApi({ cluster, logger, retry }: AdminContext): QuotasApi {
  const describeClientQuotas = async (
    options: { components?: DescribeClientQuotasComponent[]; strict?: boolean } = {},
  ): Promise<{ entries: DescribeClientQuotasResponseV1Body['entries'] }> => {
    const components = options.components ?? [];
    if (!Array.isArray(components)) {
      throw new KafkaNonRetriableError(`Invalid quota components ${formatUnknown(components)}`);
    }

    return retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata();
        const broker = await cluster.findControllerBroker();
        const { entries } = await broker.describeClientQuotas({
          components,
          strict: options.strict ?? false,
        });
        return { entries };
      } catch (error) {
        if (protocolType(error) === 'NOT_CONTROLLER') {
          logger.warn('Could not describe client quotas', {
            error: error instanceof Error ? error.message : String(error),
            retryCount,
            retryTime,
          });
          await cluster.refreshMetadata();
          throw error;
        }
        bail(error as Error);
        throw error;
      }
    });
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

    return retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata();
        const broker = await cluster.findControllerBroker();
        const response = await broker.alterClientQuotas({ entries, validateOnly: validateOnly ?? false });
        return { entries: response.entries };
      } catch (error) {
        if (protocolType(error) === 'NOT_CONTROLLER') {
          logger.warn('Could not alter client quotas', {
            error: error instanceof Error ? error.message : String(error),
            retryCount,
            retryTime,
          });
          await cluster.refreshMetadata();
          throw error;
        }
        bail(error as Error);
        throw error;
      }
    });
  };

  return { describeClientQuotas, alterClientQuotas };
}
