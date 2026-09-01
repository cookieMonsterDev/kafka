import type { PartitionProducerState } from '@cookiemonsterdev/kafka-core';
import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';
import { describeTopics } from './describe-topics';

function renderHuman(states: readonly PartitionProducerState[]): string {
  const rows: string[][] = [];
  for (const state of states) {
    for (const producer of state.activeProducers) {
      rows.push([
        String(state.partition),
        producer.producerId.toString(),
        String(producer.producerEpoch),
        String(producer.lastSequence),
        producer.lastTimestamp.toString(),
      ]);
    }
  }
  if (rows.length === 0) return '(no active producers)';
  return renderTable(['PARTITION', 'PRODUCER_ID', 'PRODUCER_EPOCH', 'LAST_SEQUENCE', 'LAST_TIMESTAMP'], rows);
}

export const topicProducersCommand: CommandSpec = {
  path: ['topic', 'producers'],
  summary: "Show a topic's active producer state, per partition",
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    {
      name: 'partition',
      type: 'number',
      multiple: true,
      brief: 'partition index to query (repeatable; defaults to every partition)',
    },
    { name: 'broker-id', type: 'string', brief: 'query a specific replica broker instead of the partition leaders' },
  ],
  positionals: [{ name: 'topic', brief: 'topic name' }],
  examples: [
    'topic producers orders --brokers localhost:9092',
    'topic producers orders --partition 0 --partition 1 --brokers localhost:9092',
  ],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage, EXIT_CODES.unsupportedByBroker],
  async run({ flags, positionals, runtime, output, config }) {
    const topic = positionals[0];
    if (topic === undefined) {
      throw new CliUsageError('topic producers requires a topic name');
    }

    const brokers = parseBrokersFlag(flags.brokers);
    const explicitPartitions = flags.partition as number[] | undefined;
    const brokerId = flags['broker-id'] as string | undefined;

    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      const partitions =
        explicitPartitions !== undefined && explicitPartitions.length > 0
          ? explicitPartitions
          : ((await describeTopics(admin, [topic]))[0]?.partitions.map((p) => p.partitionIndex) ?? []);

      if (partitions.length === 0) {
        output.write({ human: () => '(no partitions)', json: () => stringifyJsonSafe({ partitions: [] }) });
        return EXIT_CODES.ok;
      }

      const results = await admin.describeProducers({
        topicPartitions: [{ topic, partitions }],
        ...(brokerId !== undefined ? { brokerId } : {}),
      });

      output.write({
        human: () => renderHuman(results),
        json: () => stringifyJsonSafe({ partitions: results }),
      });
      return EXIT_CODES.ok;
    } finally {
      await admin.disconnect();
    }
  },
};
