import { Kafka } from '@cookiemonsterdev/kafka-core';

const kafka = new Kafka({ clientId: 'docs-setup', brokers: ['localhost:9092'] });

const admin = kafka.admin();

try {
  await admin.connect();
  await admin.createTopics({ topics: [{ topic: 'docs-demo-events', numPartitions: 1, replicationFactor: 1 }] });
  console.log('Topic docs-demo-events is ready');
} finally {
  await admin.disconnect();
}
