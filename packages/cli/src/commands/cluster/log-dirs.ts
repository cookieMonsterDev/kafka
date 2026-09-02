import { parseBrokersFlag } from '../../admin/parse-brokers';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';

export const clusterLogDirsCommand: CommandSpec = {
  path: ['cluster', 'log-dirs'],
  summary: 'Describe log directories and their partition sizes, per broker',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    { name: 'broker', type: 'number', multiple: true, brief: 'limit to this broker id (repeatable; default: all)' },
    { name: 'topic', type: 'string', multiple: true, brief: 'limit to this topic (repeatable; default: all)' },
  ],
  examples: ['cluster log-dirs --brokers localhost:9092', 'cluster log-dirs --topic orders --broker 1 --broker 2'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage],
  async run({ flags, runtime, output, config }) {
    const brokers = parseBrokersFlag(flags.brokers);
    const brokerIds = flags.broker as number[] | undefined;
    // Filtered client-side by topic, matching `kafka-log-dirs.sh --topic-list`: the real tool
    // filters its own already-fetched response rather than sending a per-topic filter on the
    // wire (core's own `topics` option exists but is left unused here for exactly that reason).
    const topicFilter = flags.topic as string[] | undefined;
    const topicFilterSet = topicFilter === undefined ? null : new Set(topicFilter);

    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      const { brokers: allBrokerLogDirs } = await admin.describeLogDirs({ brokerIds });

      const matchesFilter = (topic: string): boolean => topicFilterSet === null || topicFilterSet.has(topic);
      const brokerLogDirs = allBrokerLogDirs.map((broker) => ({
        ...broker,
        logDirs: broker.logDirs.map((logDir) => ({
          ...logDir,
          topics: logDir.topics.filter((topic) => matchesFilter(topic.topic)),
        })),
      }));

      const rows: string[][] = [];
      for (const broker of brokerLogDirs) {
        for (const logDir of broker.logDirs) {
          for (const topic of logDir.topics) {
            for (const partition of topic.partitions) {
              rows.push([
                String(broker.brokerId),
                logDir.logDir,
                topic.topic,
                String(partition.partition),
                partition.size.toString(),
                partition.offsetLag.toString(),
                String(partition.isFuture),
              ]);
            }
          }
        }
      }

      output.write({
        human: () =>
          rows.length === 0
            ? '(no matching log dirs)'
            : renderTable(['BROKER', 'LOG_DIR', 'TOPIC', 'PARTITION', 'SIZE', 'OFFSET_LAG', 'IS_FUTURE'], rows),
        json: () => stringifyJsonSafe({ brokers: brokerLogDirs }),
      });

      return EXIT_CODES.ok;
    } finally {
      await admin.disconnect();
    }
  },
};
