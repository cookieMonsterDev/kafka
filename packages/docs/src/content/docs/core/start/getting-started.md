---
title: Your first message
description: Start a local broker, create a topic, and run a producer and consumer
order: 3
section: start
---

This walkthrough uses Node.js 24+ and a single local Kafka broker. You will create
one topic, send `hello`, and read it from a separate process. Docker is needed only
for the local broker; applications can use an existing cluster instead.

## Start a broker

Download the [demo Compose file](../../../../examples/first-message/docker-compose.yml)
into an empty directory as `docker-compose.yml`, then run:

```sh
docker compose up -d --wait
```

The broker listens at `localhost:9092`. Port 9092 must be free. This is a local,
plaintext, single-broker setup; see [Security](../../guides/security/) for remote clusters.

## Install the client

In that directory:

```sh
npm init -y
npm install @cookiemonsterdev/kafka-core
```

Create the three `.mts` files below. Node runs their erasable TypeScript directly.
Create one `Kafka` instance per process and share it between clients in that process.

## Create a topic

Save as `setup.mts`:

<!-- example: first-message/setup.mts -->

```ts
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
```

```sh
node setup.mts
```

Expected: `Topic docs-demo-events is ready`. The single broker requires replication factor 1.

## Start the consumer

Save as `consumer.mts` and run `node consumer.mts` in a second terminal:

<!-- example: first-message/consumer.mts -->

```ts
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
```

The process stays running and waits for records. Press Ctrl+C to disconnect cleanly.

## Send a message

Save as `producer.mts` and run `node producer.mts` in your first terminal:

<!-- example: first-message/producer.mts -->

```ts
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
```

The producer prints `Sent hello`. The consumer prints a line such as
`partition=0 offset=0 value=hello`. Offsets are `bigint`; convert to strings before JSON serialization.

## Know before deploying

- Idempotence defaults to `false`; choose delivery guarantees deliberately.
- Consumers default to `read_committed` isolation.
- Keyed messages use murmur2 partitioning; unkeyed routing differs from the Java sticky default.
- `fromBeginning` applies when a group has no committed offset; it does not reset an existing group.

See [Compatibility](../../reference/compatibility/) and [Production readiness](../../guides/production-readiness/).

## Clean up

Stop the consumer with Ctrl+C, then remove this demo cluster and its data:

```sh
docker compose down -v
```

On an existing cluster, remove only the demo topic with `kafka topic delete docs-demo-events`
after checking your active profile. Re-running with the same group resumes its committed offset;
use a new demo group to read the history again.

Next: [Producer](../../guides/producer/), [Consumer](../../guides/consumer/),
[Troubleshooting](../../guides/troubleshooting/), [Task recipes](../../guides/recipes/).
