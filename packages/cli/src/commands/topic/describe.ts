import type { CommandSpec } from '../../args/define';
import { CliUsageError } from '../../args/coerce';
import { parseBrokersFlag } from '../../admin/parse-brokers';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';
import { describeTopics, type DescribedTopic } from './describe-topics';

function renderHuman(topics: readonly DescribedTopic[]): string {
  const rows: string[][] = [];
  for (const topic of topics) {
    for (const partition of topic.partitions) {
      rows.push([
        topic.name ?? '(unknown)',
        String(partition.partitionIndex),
        String(partition.leader),
        partition.replicas.join(','),
        partition.isr.join(','),
      ]);
    }
  }
  if (rows.length === 0) return '(no partitions)';
  return renderTable(['TOPIC', 'PARTITION', 'LEADER', 'REPLICAS', 'ISR'], rows);
}

export const topicDescribeCommand: CommandSpec = {
  path: ['topic', 'describe'],
  summary: 'Describe one or more topics',
  flags: [{ name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' }],
  positionals: [{ name: 'topics', variadic: true, brief: 'topic names to describe' }],
  examples: ['topic describe orders --brokers localhost:9092'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage],
  async run({ flags, positionals, runtime, output, config }) {
    if (positionals.length === 0) {
      throw new CliUsageError('topic describe requires at least one topic name');
    }
    const brokers = parseBrokersFlag(flags.brokers);
    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    let topics: DescribedTopic[];
    try {
      topics = await describeTopics(admin, positionals);
    } finally {
      await admin.disconnect();
    }
    output.write({
      human: () => renderHuman(topics),
      json: () => stringifyJsonSafe({ topics }),
    });
    return EXIT_CODES.ok;
  },
};
