import { Kafka } from '@cookiemonsterdev/kafka-core';

const kafka = new Kafka({ clientId: 'docs-producer', brokers: ['localhost:9092'] });

const producer = kafka.producer();

try {
  await producer.connect();
  await producer.send({ topic: 'docs-demo-events', messages: [{ key: 'user-1', value: 'hello' }] });
  console.log('Sent hello');
} finally {
  await producer.disconnect();
}
