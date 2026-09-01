import type { CommandSpec } from '../../args/define';
import { parseBrokersFlag } from '../../admin/parse-brokers';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';

export const pingCommand: CommandSpec = {
  path: ['ping'],
  summary: 'Check connectivity to the cluster',
  flags: [{ name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' }],
  examples: ['ping --brokers localhost:9092'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage],
  async run({ flags, runtime, output, config }) {
    const brokers = parseBrokersFlag(flags.brokers);
    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      const cluster = await admin.describeCluster();
      output.write({
        human: () => `ok — ${String(cluster.brokers.length)} broker(s), cluster ${cluster.clusterId ?? '(unknown)'}`,
        json: () => stringifyJsonSafe({ ok: true, cluster }),
      });
    } finally {
      await admin.disconnect();
    }
    return EXIT_CODES.ok;
  },
};
