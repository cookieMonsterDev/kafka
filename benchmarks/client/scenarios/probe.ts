import { createSignal, pooledRecord, type ScenarioContext } from './harness.ts';

/**
 * Support probe: connect a producer, produce one record, read it back with a new consumer from
 * the beginning and check the value. Throws on any mismatch; produces no metrics.
 */
export async function runProbe(context: ScenarioContext): Promise<null> {
  const { spec, observed } = context;
  const record = pooledRecord(context.records, 0);

  const producer = await context.clients.connectProducer(context.adapter, context.adapterOptions);
  await producer.send(spec.topic, [record]);
  observed.producedRecords = 1;

  const received = createSignal<Buffer | null>();
  const consumer = await context.clients.connectConsumer(context.adapter, context.adapterOptions);
  await consumer.start({
    topic: spec.topic,
    groupId: spec.groupId,
    mode: 'each-message',
    commitEvery: 0,
    onMessage(value) {
      observed.consumedRecords++;
      observed.consumedBytes += value === null ? 0 : value.length;
      received.resolve(value);
    },
    onBatch() {
      received.reject(new Error('probe asked for per-record delivery but got a batch'));
    },
  });

  const value = await received.promise;
  if (value === null || !value.equals(record.value)) {
    const actual = value === null ? 'null' : `${value.length} bytes`;
    throw new Error(`probe read back a different value: expected ${record.value.length} bytes, got ${actual}`);
  }
  return null;
}
