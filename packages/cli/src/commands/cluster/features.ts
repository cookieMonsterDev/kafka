import { parseBrokersFlag } from '../../admin/parse-brokers';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';

export const clusterFeaturesCommand: CommandSpec = {
  path: ['cluster', 'features'],
  summary: 'Describe supported and finalized feature versions',
  flags: [{ name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' }],
  examples: ['cluster features --brokers localhost:9092'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage],
  async run({ flags, runtime, output, config }) {
    const brokers = parseBrokersFlag(flags.brokers);
    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      const features = await admin.describeFeatures();

      const supportedByName = new Map(features.supportedFeatures.map((feature) => [feature.name, feature]));
      const finalizedByName = new Map(features.finalizedFeatures.map((feature) => [feature.name, feature]));
      const names = [...new Set([...supportedByName.keys(), ...finalizedByName.keys()])].sort();

      const rows = names.map((name) => {
        const supported = supportedByName.get(name);
        const finalized = finalizedByName.get(name);
        return [
          name,
          supported ? `${String(supported.minVersion)}-${String(supported.maxVersion)}` : '(unsupported)',
          finalized ? String(finalized.maxVersionLevel) : '(not finalized)',
        ];
      });

      output.write({
        human: () =>
          [
            renderTable(['FEATURE', 'SUPPORTED_RANGE', 'FINALIZED'], rows),
            '',
            `Finalized features epoch: ${features.finalizedFeaturesEpoch.toString()}`,
            `ZK migration ready: ${features.zkMigrationReady ?? '(n/a)'}`,
          ].join('\n'),
        json: () => stringifyJsonSafe(features),
      });

      return EXIT_CODES.ok;
    } finally {
      await admin.disconnect();
    }
  },
};
