import type { CommandSpec } from '../../args/define';
import { parseBrokersFlag } from '../../admin/parse-brokers';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';

export const topicListCommand: CommandSpec = {
  path: ['topic', 'list'],
  summary: 'List every topic',
  flags: [{ name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' }],
  examples: ['topic list --brokers localhost:9092'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage],
  async run({ flags, runtime, output }) {
    const brokers = parseBrokersFlag(flags.brokers);
    const admin = await runtime.openAdmin({ brokers });
    let topics: string[];
    try {
      topics = await admin.listTopics();
    } finally {
      await admin.disconnect();
    }
    output.write({
      human: () =>
        topics.length === 0
          ? '(no topics)'
          : renderTable(
              ['NAME'],
              topics.map((topic) => [topic]),
            ),
      json: () => stringifyJsonSafe({ topics }),
    });
    return EXIT_CODES.ok;
  },
};
