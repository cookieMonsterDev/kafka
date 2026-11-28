import { KafkaNonRetriableError } from '../errors';
import { CONTROLLER } from '../cluster/run';
import { retrier as createRetrier } from '../retry/index';
import type { AdminContext } from './helpers';
import { formatUnknown } from './helpers';
import type { UnregisterControllerOptions } from './types';
import { UnregisterController } from '../protocol/requests/lazy-families';

export interface UnregisterControllerApi {
  unregisterController: (options: UnregisterControllerOptions) => Promise<void>;
}

function validateControllerId(controllerId: unknown): number {
  if (!Number.isInteger(controllerId) || (controllerId as number) < 0) {
    throw new KafkaNonRetriableError(`Invalid controllerId ${formatUnknown(controllerId)}`);
  }
  return controllerId as number;
}

export function createUnregisterControllerApi({ cluster, retry }: AdminContext): UnregisterControllerApi {
  const retrier = createRetrier(retry);

  const unregisterController = async ({ controllerId }: UnregisterControllerOptions): Promise<void> => {
    const normalizedControllerId = validateControllerId(controllerId);

    await cluster.run(
      CONTROLLER,
      async (broker) => {
        await broker.send(UnregisterController, { controllerId: normalizedControllerId });
      },
      { retrier, refreshMetadata: true, description: 'Could not unregister controller' },
    );
  };

  return { unregisterController };
}
