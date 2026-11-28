import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { confirmDestructive } from '../../interaction/confirm';
import { stringifyJsonSafe } from '../../output/json';

export const clusterUnregisterControllerCommand: CommandSpec = {
  path: ['cluster', 'unregister-controller'],
  summary: 'Unregister a controller that was removed from the KRaft quorum for good (Kafka 4.4+)',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    { name: 'controller-id', type: 'number', brief: 'id of the controller to unregister' },
    { name: 'yes', type: 'boolean', brief: 'confirm the unregistration without an interactive prompt' },
  ],
  examples: ['cluster unregister-controller --controller-id 3001 --brokers localhost:9092 --yes'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage, EXIT_CODES.abortedOrUnconfirmed],
  async run({ flags, runtime, output, config }) {
    const controllerId = flags['controller-id'] as number | undefined;
    if (controllerId === undefined) {
      throw new CliUsageError('cluster unregister-controller requires --controller-id');
    }

    await confirmDestructive({
      runtime,
      yes: flags.yes === true,
      message: `Unregister controller ${String(controllerId)}?`,
      confirmDestructive: config.cli.confirmDestructive,
    });

    const brokers = parseBrokersFlag(flags.brokers);
    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      await admin.unregisterController({ controllerId });

      output.write({
        human: () => `Controller ${String(controllerId)} unregistered.`,
        json: () => stringifyJsonSafe({ controllerId, ok: true }),
      });

      return EXIT_CODES.ok;
    } finally {
      await admin.disconnect();
    }
  },
};
