import type { ApiVersionDescription, BrokerApiVersionsDescription } from '@cookiemonsterdev/kafka-core';
import { parseBrokersFlag } from '../../admin/parse-brokers';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';

/** `<min> to <max>`, or one number when the broker supports a single version, as kafka-cluster.sh prints it. */
function formatRange({ minVersion, maxVersion }: { minVersion: number; maxVersion: number }): string {
  return minVersion === maxVersion ? String(minVersion) : `${String(minVersion)} to ${String(maxVersion)}`;
}

function formatApi(api: ApiVersionDescription): string {
  const name = `${api.apiName ?? 'UNKNOWN'}(${String(api.apiKey)})`;
  if (api.brokerVersions === undefined) return `${name}: UNSUPPORTED`;

  const range = formatRange(api.brokerVersions);
  if (!api.isImplemented) return `${name}: ${range} [not implemented]`;
  if (api.version === undefined) return `${name}: ${range} [unusable: no common version]`;
  return `${name}: ${range} [usable: ${String(api.version)}]`;
}

/** One block per broker, in the shape of `kafka-cluster.sh api-versions` (KIP-1220). */
function formatBroker(broker: BrokerApiVersionsDescription): string {
  const header = `${broker.host}:${String(broker.port)} (id: ${String(broker.nodeId)} rack: ${broker.rack ?? 'null'}) -> (`;
  const apis = broker.apis.map((api) => `\t${formatApi(api)}`).join(',\n');
  return `${header}\n${apis}\n)`;
}

export const clusterApiVersionsCommand: CommandSpec = {
  path: ['cluster', 'api-versions'],
  summary: 'List the API versions each broker supports and the version this client uses',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    { name: 'node', type: 'number', brief: 'describe only this broker id (default: every broker)' },
  ],
  examples: ['cluster api-versions --brokers localhost:9092', 'cluster api-versions --node 1 --json'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage],
  async run({ flags, runtime, output, config }) {
    const brokers = parseBrokersFlag(flags.brokers);
    const nodeId = flags.node as number | undefined;

    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      const result = await admin.describeApiVersions(nodeId === undefined ? {} : { brokerIds: [nodeId] });

      output.write({
        human: () => result.brokers.map(formatBroker).join('\n'),
        json: () => stringifyJsonSafe(result),
      });

      return EXIT_CODES.ok;
    } finally {
      await admin.disconnect();
    }
  },
};
