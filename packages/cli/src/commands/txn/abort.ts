import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { confirmDestructive } from '../../interaction/confirm';

function parseBigIntFlag(raw: string, flagName: string): bigint {
  try {
    return BigInt(raw);
  } catch {
    throw new CliUsageError(`--${flagName} expects an integer, got "${raw}"`);
  }
}

export const txnAbortCommand: CommandSpec = {
  path: ['txn', 'abort'],
  summary: 'Write an abort marker for one in-flight transaction on a topic partition',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    { name: 'topic', type: 'string', brief: 'topic holding the in-flight transactional data' },
    { name: 'partition', type: 'number', brief: 'partition holding the in-flight transactional data' },
    { name: 'producer-id', type: 'string', brief: "the transaction's producer id" },
    { name: 'producer-epoch', type: 'number', brief: "the transaction's producer epoch" },
    {
      name: 'coordinator-epoch',
      type: 'number',
      brief: 'coordinator epoch, if known (resolved automatically otherwise)',
    },
    { name: 'transaction-version', type: 'number', brief: 'transaction protocol version, if known' },
    { name: 'yes', type: 'boolean', brief: 'confirm the abort without an interactive prompt' },
  ],
  examples: [
    'txn abort --topic orders --partition 0 --producer-id 1000 --producer-epoch 0 --brokers localhost:9092 --yes',
  ],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage, EXIT_CODES.abortedOrUnconfirmed],
  async run({ flags, runtime, output, config }) {
    const topic = flags.topic as string | undefined;
    if (topic === undefined) {
      throw new CliUsageError('txn abort requires --topic');
    }
    if (flags.partition === undefined) {
      throw new CliUsageError('txn abort requires --partition');
    }
    const partition = flags.partition as number;

    const producerIdFlag = flags['producer-id'] as string | undefined;
    if (producerIdFlag === undefined) {
      throw new CliUsageError('txn abort requires --producer-id');
    }
    const producerId = parseBigIntFlag(producerIdFlag, 'producer-id');

    if (flags['producer-epoch'] === undefined) {
      throw new CliUsageError('txn abort requires --producer-epoch');
    }
    const producerEpoch = flags['producer-epoch'] as number;

    const coordinatorEpoch = flags['coordinator-epoch'] as number | undefined;
    const transactionVersion = flags['transaction-version'] as number | undefined;

    await confirmDestructive({
      runtime,
      yes: flags.yes === true,
      message: `Abort the in-flight transaction on ${topic}:${String(partition)} (producer ${producerIdFlag})?`,
      confirmDestructive: config.cli.confirmDestructive,
    });

    const brokers = parseBrokersFlag(flags.brokers);
    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      await admin.abortTransaction({
        topic,
        partition,
        producerId,
        producerEpoch,
        coordinatorEpoch,
        transactionVersion,
      });
      output.write({ human: () => 'aborted', json: () => JSON.stringify({ ok: true }) });
      return EXIT_CODES.ok;
    } finally {
      await admin.disconnect();
    }
  },
};
