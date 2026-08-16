import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { InstrumentationEventEmitter } from '../../../src/instrumentation/emitter';
import { createProducer } from '../../../src/producer/index';
import { createCluster, createTopic, newLogger, secureRandom } from '../../helpers/index';

describe('producer.instrumentation', () => {
  let topicName: string;
  let producer: ReturnType<typeof createProducer> | undefined;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    await createTopic({ topic: topicName });
  });

  afterEach(async () => {
    await producer?.disconnect();
  });

  it('emits connect, request, and disconnect events', async () => {
    const instrumentationEmitter = new InstrumentationEventEmitter();
    producer = createProducer({
      cluster: createCluster({ instrumentationEmitter }),
      instrumentationEmitter,
      logger: newLogger(),
    });
    const events: string[] = [];
    producer.on(producer.events.CONNECT, () => {
      events.push('connect');
    });
    producer.on(producer.events.DISCONNECT, () => {
      events.push('disconnect');
    });
    producer.on(producer.events.REQUEST, () => {
      events.push('request');
    });

    await producer.connect();
    await producer.send({ acks: 1, topic: topicName, messages: [{ key: 'k', value: 'v' }] });
    await producer.disconnect();

    expect(events).toEqual(expect.arrayContaining(['connect', 'request', 'disconnect']));
  });
});
