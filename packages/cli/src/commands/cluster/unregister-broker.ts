import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { confirmDestructive } from '../../interaction/confirm';
import { stringifyJsonSafe } from '../../output/json';

export const clusterUnregisterBrokerCommand: CommandSpec = {
  path: ['cluster', 'unregister-broker'],
  summary: 'Unregister a broker from the cluster (KRaft broker decommissioning)',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    { name: 'broker-id', type: 'number', brief: 'id of the broker to unregister' },
    { name: 'yes', type: 'boolean', brief: 'confirm the unregistration without an interactive prompt' },
  ],
  examples: ['cluster unregister-broker --broker-id 3 --brokers localhost:9092 --yes'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage, EXIT_CODES.abortedOrUnconfirmed],
  async run({ flags, runtime, output, config }) {
    const brokerId = flags['broker-id'] as number | undefined;
    if (brokerId === undefined) {
      throw new CliUsageError('cluster unregister-broker requires --broker-id');
    }

    await confirmDestructive({
      runtime,
      yes: flags.yes === true,
      message: `Unregister broker ${String(brokerId)}?`,
      confirmDestructive: config.cli.confirmDestructive,
    });

    const brokers = parseBrokersFlag(flags.brokers);
    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      await admin.unregisterBroker({ brokerId });

      output.write({
        human: () => `Broker ${String(brokerId)} unregistered.`,
        json: () => stringifyJsonSafe({ brokerId, ok: true }),
      });

      return EXIT_CODES.ok;
    } finally {
      await admin.disconnect();
    }
  },
};
