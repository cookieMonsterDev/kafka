import { Kafka } from '@cookiemonsterdev/kafka-core';

const kafka = new Kafka({ clientId: 'docs-consumer', brokers: ['localhost:9092'] });

const consumer = kafka.consumer({ groupId: 'docs-demo-reader' });

let stopping = false;

async function shutdown() {
  if (stopping) return;
  stopping = true;
  try {
    await consumer.disconnect();
  } catch (error) {
    console.error(error);
    process.exitCode = 1;
  }
}

process.once('SIGINT', () => {
  void shutdown();
});

process.once('SIGTERM', () => {
  void shutdown();
});

try {
  await consumer.connect();
  await consumer.subscribe({ topics: ['docs-demo-events'], fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      console.log(`partition=${partition} offset=${message.offset.toString()} value=${message.value?.toString()}`);
    },
  });
} catch (error) {
  await shutdown();
  throw error;
}
