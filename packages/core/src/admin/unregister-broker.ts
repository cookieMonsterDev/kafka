import { KafkaNonRetriableError } from '../errors';
import { CONTROLLER } from '../cluster/run';
import { retrier as createRetrier } from '../retry/index';
import type { AdminContext } from './helpers';
import { formatUnknown } from './helpers';
import type { UnregisterBrokerOptions } from './types';
import { UnregisterBroker } from '../protocol/requests/lazy-families';

export interface UnregisterBrokerApi {
  unregisterBroker: (options: UnregisterBrokerOptions) => Promise<void>;
}

function validateBrokerId(brokerId: unknown): number {
  if (!Number.isInteger(brokerId) || (brokerId as number) < 0) {
    throw new KafkaNonRetriableError(`Invalid brokerId ${formatUnknown(brokerId)}`);
  }
  return brokerId as number;
}

export function createUnregisterBrokerApi({ cluster, retry }: AdminContext): UnregisterBrokerApi {
  const retrier = createRetrier(retry);

  const unregisterBroker = async ({ brokerId }: UnregisterBrokerOptions): Promise<void> => {
    const normalizedBrokerId = validateBrokerId(brokerId);

    await cluster.run(
      CONTROLLER,
      async (broker) => {
        await broker.send(UnregisterBroker, { brokerId: normalizedBrokerId });
      },
      { retrier, refreshMetadata: true, description: 'Could not unregister broker' },
    );
  };

  return { unregisterBroker };
}
