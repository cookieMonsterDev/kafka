import { parseBrokersFlag } from '../../admin/parse-brokers';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';

export const clusterInfoCommand: CommandSpec = {
  path: ['cluster', 'info'],
  summary: 'Describe the cluster: its brokers, controller, and cluster id',
  flags: [{ name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' }],
  examples: ['cluster info --brokers localhost:9092'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage],
  async run({ flags, runtime, output, config }) {
    const brokers = parseBrokersFlag(flags.brokers);
    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      const description = await admin.describeCluster();

      output.write({
        human: () =>
          [
            `Cluster ID: ${description.clusterId ?? '(none)'}`,
            `Controller: ${description.controller ?? '(none)'}`,
            '',
            renderTable(
              ['NODE_ID', 'HOST', 'PORT'],
              description.brokers.map((broker) => [String(broker.nodeId), broker.host, String(broker.port)]),
            ),
          ].join('\n'),
        json: () => stringifyJsonSafe(description),
      });

      return EXIT_CODES.ok;
    } finally {
      await admin.disconnect();
    }
  },
};
