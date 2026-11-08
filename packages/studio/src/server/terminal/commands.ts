import { z } from 'zod';
import {
  EmbeddedCliAbortedError,
  EmbeddedCliUsageError,
  type EmbeddedCommandSpec,
} from '@cookiemonsterdev/kafka-cli/embedded';
import { produceRequestSchema } from '../../shared/contracts/produce';
import { messagesQuerySchema, type MessageRecord } from '../../shared/contracts/message';
import type { AdminPool } from '../kafka/admin-pool';
import type { MessageConsumerFactory } from '../kafka/messages';
import { readMessagesPage } from '../kafka/messages';
import { runTail } from '../kafka/tail';
import type { ProducerPool } from '../kafka/produce';
import { sendMessages } from '../kafka/produce';
import type { StudioEventBus } from '../kafka/events';

function decodedMessage(message: MessageRecord) {
  return {
    ...message,
    key: message.key === null ? null : Buffer.from(message.key, 'base64').toString('utf8'),
    value: message.value === null ? null : Buffer.from(message.value, 'base64').toString('utf8'),
  };
}

/** Studio message operations share its existing producer, bounded-reader and live-tail implementations. */
export function studioTerminalCommands(context: {
  readonly readOnly: boolean;
  readonly profile: string | null;
  readonly pool: AdminPool;
  readonly producers: ProducerPool;
  readonly consumerFactory: MessageConsumerFactory;
  readonly events: StudioEventBus;
}): readonly EmbeddedCommandSpec[] {
  return [
    {
      path: ['produce'],
      summary: 'Send messages using the Studio producer',
      positionals: [{ name: 'topic', brief: 'topic name' }],
      flags: [
        { name: 'value', type: 'string', brief: 'UTF-8 message value' },
        { name: 'tombstone', type: 'boolean', brief: 'send a null value instead of --value' },
        {
          name: 'header',
          type: 'string',
          multiple: true,
          keyValue: true,
          brief: 'message header key=value (repeatable)',
        },
        { name: 'timestamp', type: 'number', brief: 'Unix epoch milliseconds' },
        { name: 'key', type: 'string', brief: 'UTF-8 message key' },
        { name: 'partition', type: 'number', brief: 'target partition' },
        { name: 'count', type: 'number', brief: 'number of messages (1-1000, default 1)' },
      ],
      examples: ['produce orders --value \'{"id":1}\' --key order-1'],
      exitCodes: [0, 1, 2, 5],
      async run({ flags, positionals, output, runtime }) {
        if (context.readOnly) throw new EmbeddedCliAbortedError('Studio is in read-only mode; produce is unavailable.');
        const count = z
          .number()
          .int()
          .min(1)
          .max(1000)
          .safeParse(flags.count ?? 1);
        if (!count.success) throw new EmbeddedCliUsageError('--count must be an integer between 1 and 1000.');
        if (flags.tombstone === true && flags.value !== undefined)
          throw new EmbeddedCliUsageError('Use --tombstone or --value, not both.');
        const request = produceRequestSchema.safeParse({
          topic: positionals[0],
          messages: Array.from({ length: count.data }, () => ({
            value: flags.tombstone === true ? null : flags.value,
            headers: flags.header,
            timestamp: flags.timestamp,
            key: flags.key,
            partition: flags.partition,
          })),
        });
        if (!request.success || positionals.length !== 1)
          throw new EmbeddedCliUsageError(
            'Usage: produce <topic> --value <text> [--key <text>] [--partition <n>] [--count <n>]',
          );
        const producer = await context.producers.get(context.profile);
        runtime.signal.throwIfAborted();
        const result = await sendMessages(producer, { ...request.data, signal: runtime.signal });
        context.events.publish({
          kind: 'produce',
          topic: request.data.topic,
          partition: null,
          count: count.data,
          bytes: request.data.messages.reduce(
            (bytes, message) => bytes + Buffer.byteLength(message.value ?? '') + Buffer.byteLength(message.key ?? ''),
            0,
          ),
        });
        output.write({
          human: () =>
            `Sent ${String(count.data)} message(s) to ${request.data.topic}\n${JSON.stringify(result, null, 2)}`,
          json: () => JSON.stringify(result),
        });
        return 0;
      },
    },
    {
      path: ['consume'],
      summary: 'Read stored messages or follow new messages',
      handlesSignal: true,
      positionals: [{ name: 'topic', brief: 'topic name' }],
      flags: [
        { name: 'limit', type: 'number', brief: 'maximum stored messages (1-1000, default 100)' },
        { name: 'from', type: 'string', brief: 'earliest, latest or a decimal offset' },
        { name: 'partition', type: 'number', brief: 'read one partition' },
        { name: 'tail', type: 'boolean', brief: 'follow new messages until Ctrl+C' },
        { name: 'timestamp', type: 'number', brief: 'seek stored records by Unix epoch milliseconds' },
      ],
      examples: ['consume orders --limit 10', 'consume orders --tail'],
      exitCodes: [0, 1, 2],
      async run({ flags, positionals, output, runtime }) {
        const topic = positionals[0];
        const query = messagesQuerySchema.safeParse({
          limit: flags.limit,
          from: flags.from,
          timestamp: flags.timestamp,
          partition: flags.partition,
        });
        if (topic === undefined || positionals.length !== 1 || !query.success)
          throw new EmbeddedCliUsageError(
            'Usage: consume <topic> [--limit <1-1000>] [--from earliest|latest|<offset>] [--partition <n>] [--tail]',
          );
        const admin = await context.pool.get(context.profile);
        runtime.signal.throwIfAborted();
        const consumer = context.consumerFactory(context.profile).consumer();
        if (flags.tail === true) {
          if (flags.from !== undefined || flags.limit !== undefined || flags.timestamp !== undefined)
            throw new EmbeddedCliUsageError('--tail follows new messages; omit --from and --limit.');
          await runTail(
            admin,
            consumer,
            {
              send: (event, data) => {
                if (event === 'gap') {
                  const gap = data as { dropped: number };
                  output.write({
                    human: () => `Dropped ${String(gap.dropped)} buffered messages.`,
                    json: () => JSON.stringify({ type: 'gap', ...gap }),
                  });
                  return;
                }
                const message = data as MessageRecord;
                output.write({
                  human: () => JSON.stringify(decodedMessage(message)),
                  json: () => JSON.stringify(message),
                });
              },
            },
            { topic, partition: query.data.partition, maxBuffered: 1000 },
            runtime.signal,
          );
        } else {
          const abort = () => {
            void consumer.disconnect().catch(() => {});
          };
          runtime.signal.addEventListener('abort', abort, { once: true });
          try {
            runtime.signal.throwIfAborted();
            const result = await readMessagesPage(admin, consumer, { topic, query: query.data });
            output.write({
              human: () =>
                result.messages.map((message) => JSON.stringify(decodedMessage(message))).join('\n') || '(no messages)',
              json: () => JSON.stringify(result),
            });
          } finally {
            runtime.signal.removeEventListener('abort', abort);
            await consumer.disconnect();
          }
        }
        return 0;
      },
    },
  ];
}
