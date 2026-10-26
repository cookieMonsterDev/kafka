# Producer options

## Partitioners

| Option                                                                         | Routing                                                             |
| ------------------------------------------------------------------------------ | ------------------------------------------------------------------- |
| default (`Partitioners.DefaultPartitioner`)                                    | murmur2 on the key, same as the Java client                         |
| `createPartitioner: Partitioners.LegacyPartitioner`                            | pre-2.0 key routing                                                 |
| `createPartitioner: Partitioners.StickyPartitioner`                            | KIP-794 uniform sticky: unkeyed records share a partition per batch |
| `createPartitioner: () => Partitioners.StickyPartitioner({ adaptive: false })` | sticky without latency-adaptive rotation                            |

Explicit `partition` on a message always wins. `Partitioners.JavaCompatiblePartitioner` no longer
exists; use `DefaultPartitioner`.

## maxRequestSize

`maxRequestSize` (default `1_048_576`) caps the uncompressed bytes of one Produce request and is
checked before the record is buffered. A single record, or one call's records combined, over the
cap reject with `KafkaMessageTooLargeError` (`error.size`, `error.maxRequestSize`). The client
does not split one call across requests. Keep `maxRequestSize >= batchSize`.

## Hooks

`hooks` are ordered async callbacks, not an interceptor class. `onSend` fires once per
`send()`/`sendBatch()` before dispatch; `onAck` fires once per call after it settles, with
`metadata` or `error`. A hook that throws is logged and never changes the send result.

```ts
import { Kafka } from '@cookiemonsterdev/kafka-core';

const kafka = new Kafka({ brokers: ['localhost:9092'] });
const producer = kafka.producer({
  hooks: {
    onSend: [({ topicMessages }) => console.log('sending', topicMessages.length, 'topic(s)')],
    onAck: [
      ({ metadata, error }) => (error ? console.error('send failed', error) : console.log('acked', metadata?.length)),
    ],
  },
});
```

## Cancellation

`send()` and `sendBatch()` accept `signal` (an `AbortSignal`) to stop waiting. Aborting does not
recall a request already on the wire.

```ts
const controller = new AbortController();
await producer.send({ topic: 'events', messages: [{ value: 'hello' }], signal: controller.signal });
```
