import { KafkaNonRetriableError } from '../errors';
import { retrier } from '../retry/index';
import type { AdminContext } from './helpers';
import { formatUnknown, protocolType } from './helpers';
import type { UnregisterBrokerOptions } from './types';

export interface UnregisterBrokerApi {
  unregisterBroker: (options: UnregisterBrokerOptions) => Promise<void>;
}

function validateBrokerId(brokerId: unknown): number {
  if (!Number.isInteger(brokerId) || (brokerId as number) < 0) {
    throw new KafkaNonRetriableError(`Invalid brokerId ${formatUnknown(brokerId)}`);
  }
  return brokerId as number;
}

export function createUnregisterBrokerApi({ cluster, logger, retry }: AdminContext): UnregisterBrokerApi {
  const unregisterBroker = async ({ brokerId }: UnregisterBrokerOptions): Promise<void> => {
    const normalizedBrokerId = validateBrokerId(brokerId);

    await retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata();
        const broker = await cluster.findControllerBroker();
        await broker.unregisterBroker({ brokerId: normalizedBrokerId });
      } catch (error) {
        if (protocolType(error) === 'NOT_CONTROLLER') {
          logger.warn('Could not unregister broker', {
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

  return { unregisterBroker };
}
