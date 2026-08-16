import { randomBytes, randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe } from 'vitest';
import { createAdmin } from '../../src/admin/index';
import { Broker } from '../../src/broker/index';
import { createSaslAuthenticator } from '../../src/broker/sasl-authenticator/index';
import { Cluster } from '../../src/cluster/index';
import type { ClusterOptions } from '../../src/cluster/index';
import { BrokerPool } from '../../src/cluster/broker-pool';
import { connectionPoolBuilder, type ConnectionPoolBuilder } from '../../src/cluster/connection-pool-builder';
import type { Consumer } from '../../src/consumer/index';
import type { ConsumerEventName } from '../../src/consumer/instrumentation-events';
import { Kafka } from '../../src/client';
import { createLogger, LOG_LEVELS, type Logger } from '../../src/loggers/index';
import { consoleLogCreator } from '../../src/loggers/console';
import { ConnectionPool } from '../../src/network/connection-pool';
import type { ConnectionOptions } from '../../src/network/connection';
import { createDefaultSocketFactory } from '../../src/network/socket-factory';
import { FAST_RETRY_DEFAULTS } from '../../src/retry/test-defaults';
import type { KafkaConfig } from '../../src/types/index';
import type { CustomPartitioner } from '../../src/producer/types';
import { waitFor } from '../../src/utils/wait';

const helpersDir = path.dirname(fileURLToPath(import.meta.url));
const certSigned = path.resolve(helpersDir, '../assets/certs/cert-signed');

const socketFactory = createDefaultSocketFactory();

export function getHost(): string {
  return 'localhost';
}

/** Map compose-internal advertised listeners (kafkaN:29092) to host-published ports. */
const INTERNAL_LISTENER_PORTS: Record<string, Record<number, number>> = {
  kafka1: { 29092: 9092, 9092: 9092, 9093: 9093, 9094: 9094 },
  kafka2: { 29092: 9095, 9092: 9095, 9093: 9096, 9094: 9097 },
  kafka3: { 29092: 9098, 9092: 9098, 9093: 9099, 9094: 9100 },
};

export function advertisedAddress(host: string, port: number): { host: string; port: number } {
  const mapped = INTERNAL_LISTENER_PORTS[host]?.[port];
  if (mapped != null) {
    return { host: getHost(), port: mapped };
  }
  return { host, port };
}

export const TRANSIENT_METADATA_ERRORS = [
  'LEADER_NOT_AVAILABLE',
  'UNKNOWN_TOPIC_OR_PARTITION',
  'NOT_LEADER_FOR_PARTITION',
] as const;

export function secureRandom(length = 10): string {
  return `${randomBytes(length).toString('hex')}-${process.pid}-${randomUUID()}`;
}

export function plainTextBrokers(host = getHost()): string[] {
  return [`${host}:9092`, `${host}:9095`, `${host}:9098`];
}

export function sslBrokers(host = getHost()): string[] {
  return [`${host}:9093`, `${host}:9096`, `${host}:9099`];
}

export function saslBrokers(host = getHost()): string[] {
  return [`${host}:9094`, `${host}:9097`, `${host}:9100`];
}

export function newLogger(opts: { level?: (typeof LOG_LEVELS)[keyof typeof LOG_LEVELS] } = {}): Logger {
  return createLogger({
    level: opts.level ?? LOG_LEVELS.NOTHING,
    logCreator: consoleLogCreator,
  });
}

export function connectionOpts(opts: Partial<ConnectionOptions> = {}): ConnectionOptions {
  return {
    socketFactory,
    clientId: `test-${secureRandom()}`,
    connectionTimeout: 3000,
    requestTimeout: 30_000,
    logger: newLogger(),
    host: getHost(),
    port: 9092,
    createSaslAuthenticator,
    ...opts,
  };
}

export function sslConnectionOpts(opts: Partial<ConnectionOptions> = {}): ConnectionOptions {
  return connectionOpts({
    port: 9093,
    ssl: {
      servername: 'localhost',
      rejectUnauthorized: false,
      ca: [readFileSync(certSigned, 'utf8')],
    },
    ...opts,
  });
}

export function saslConnectionOpts(): ConnectionOptions {
  return sslConnectionOpts({
    port: 9094,
    sasl: { mechanism: 'plain', username: 'test', password: 'testtest' },
  });
}

export function saslWrongConnectionOpts(): ConnectionOptions {
  return sslConnectionOpts({
    port: 9094,
    sasl: { mechanism: 'plain', username: 'wrong', password: 'wrong' },
  });
}

export function saslSCRAM256ConnectionOpts(): ConnectionOptions {
  return sslConnectionOpts({
    port: 9094,
    sasl: { mechanism: 'scram-sha-256', username: 'testscram', password: 'testtestscram=256' },
  });
}

export function saslSCRAM256WrongConnectionOpts(): ConnectionOptions {
  return sslConnectionOpts({
    port: 9094,
    sasl: { mechanism: 'scram-sha-256', username: 'wrong', password: 'wrong' },
  });
}

export function saslSCRAM512ConnectionOpts(): ConnectionOptions {
  return sslConnectionOpts({
    port: 9094,
    sasl: { mechanism: 'scram-sha-512', username: 'testscram', password: 'testtestscram=512' },
  });
}

export function saslSCRAM512WrongConnectionOpts(): ConnectionOptions {
  return sslConnectionOpts({
    port: 9094,
    sasl: { mechanism: 'scram-sha-512', username: 'wrong', password: 'wrong' },
  });
}

/** `alg: none` JWT: two base64url JSON blobs and an empty signature. */
export function createUnsignedJwt(payload: Record<string, unknown>): string {
  const encode = (value: unknown): string => Buffer.from(JSON.stringify(value)).toString('base64url');
  return `${encode({ alg: 'none', typ: 'JWT' })}.${encode(payload)}.`;
}

export function saslOAuthBearerConnectionOpts(): ConnectionOptions {
  return sslConnectionOpts({
    port: 9094,
    sasl: {
      mechanism: 'oauthbearer',
      oauthBearerProvider: () => Promise.resolve({ value: createUnsignedJwt({ sub: 'test' }) }),
    },
  });
}

export interface SaslEntry {
  name: string;
  opts: () => ConnectionOptions;
  wrongOpts?: () => ConnectionOptions;
  expectedErr?: RegExp;
}

export const saslEntries: SaslEntry[] =
  process.env.OAUTHBEARER_ENABLED === '1'
    ? [{ name: 'OAUTHBEARER', opts: saslOAuthBearerConnectionOpts }]
    : [
        {
          name: 'PLAIN',
          opts: saslConnectionOpts,
          wrongOpts: saslWrongConnectionOpts,
          expectedErr: /SASL PLAIN authentication failed/,
        },
        {
          name: 'SCRAM 256',
          opts: saslSCRAM256ConnectionOpts,
          wrongOpts: saslSCRAM256WrongConnectionOpts,
          expectedErr: /SASL SCRAM SHA256 authentication failed/,
        },
        {
          name: 'SCRAM 512',
          opts: saslSCRAM512ConnectionOpts,
          wrongOpts: saslSCRAM512WrongConnectionOpts,
          expectedErr: /SASL SCRAM SHA512 authentication failed/,
        },
      ];

export function createConnectionPool(opts: Partial<ConnectionOptions> = {}): ConnectionPool {
  return new ConnectionPool(connectionOpts(opts));
}

export function createConnectionBuilder(
  opts: Partial<Parameters<typeof connectionPoolBuilder>[0]> = {},
  brokers = plainTextBrokers(),
): ConnectionPoolBuilder {
  return connectionPoolBuilder({
    socketFactory,
    logger: newLogger(),
    brokers,
    connectionTimeout: 1000,
    requestTimeout: 30_000,
    clientId: `test-${secureRandom()}`,
    ...opts,
  });
}

type CreateClusterOpts = Partial<Omit<ClusterOptions, 'instrumentationEmitter'>> &
  Partial<Omit<ConnectionOptions, 'instrumentationEmitter'>> & {
    instrumentationEmitter?:
      ClusterOptions['instrumentationEmitter'] | ConnectionOptions['instrumentationEmitter'] | null;
  };

export function createCluster(opts: CreateClusterOpts = {}, brokers = plainTextBrokers()): Cluster {
  return new Cluster({
    logger: opts.logger ?? newLogger(),
    socketFactory: opts.socketFactory ?? socketFactory,
    brokers,
    ssl: opts.ssl,
    sasl: opts.sasl,
    clientId: opts.clientId ?? `test-${secureRandom()}`,
    connectionTimeout: opts.connectionTimeout ?? 3000,
    authenticationTimeout: opts.authenticationTimeout,
    reauthenticationThreshold: opts.reauthenticationThreshold,
    requestTimeout: opts.requestTimeout ?? 30_000,
    enforceRequestTimeout: opts.enforceRequestTimeout ?? true,
    metadataMaxAge: opts.metadataMaxAge,
    retry: opts.retry ?? FAST_RETRY_DEFAULTS,
    allowAutoTopicCreation: opts.allowAutoTopicCreation,
    maxInFlightRequests: opts.maxInFlightRequests,
    isolationLevel: opts.isolationLevel,
    instrumentationEmitter: opts.instrumentationEmitter as ClusterOptions['instrumentationEmitter'],
    offsets: opts.offsets,
  });
}

export function createBrokerPool(opts: Partial<ConstructorParameters<typeof BrokerPool>[0]> = {}): BrokerPool {
  return new BrokerPool({
    connectionPoolBuilder: opts.connectionPoolBuilder ?? createConnectionBuilder(),
    logger: opts.logger ?? newLogger(),
    retry: opts.retry ?? FAST_RETRY_DEFAULTS,
    allowAutoTopicCreation: opts.allowAutoTopicCreation,
    authenticationTimeout: opts.authenticationTimeout,
    metadataMaxAge: opts.metadataMaxAge,
  });
}

export function createBroker(opts: Partial<ConnectionOptions> = {}): Broker {
  return new Broker({
    connectionPool: createConnectionPool(opts),
    logger: newLogger(),
  });
}

export const createModPartitioner: CustomPartitioner = () => {
  return ({ partitionMetadata, message }) => {
    if (message.partition != null) {
      return message.partition;
    }
    const numPartitions = partitionMetadata.length;
    const keyText = message.key == null ? '0' : message.key.toString();
    const key = Number.parseInt(keyText.replace(/[^\d]/g, ''), 10);
    return ((key || 0) % 3) % Math.max(numPartitions, 1);
  };
};

export function createKafka(config: Partial<KafkaConfig> = {}): Kafka {
  return new Kafka({
    clientId: `test-${secureRandom()}`,
    brokers: plainTextBrokers(),
    logLevel: LOG_LEVELS.NOTHING,
    retry: FAST_RETRY_DEFAULTS,
    ...config,
  });
}

export async function createTopic({
  topic,
  partitions = 1,
  replicas = 1,
  config = [],
}: {
  topic: string;
  partitions?: number;
  replicas?: number;
  config?: { name: string; value: string }[];
}): Promise<void> {
  const admin = createKafka().admin();
  try {
    await admin.connect();
    await admin.createTopics({
      waitForLeaders: true,
      topics: [{ topic, numPartitions: partitions, replicationFactor: replicas, configEntries: config }],
    });
  } finally {
    await admin.disconnect();
  }
}

export async function addPartitions({ topic, partitions }: { topic: string; partitions: number }): Promise<void> {
  const admin = createKafka().admin();
  const cluster = createCluster();
  try {
    await admin.connect();
    await admin.createPartitions({ topicPartitions: [{ topic, count: partitions }] });
    await cluster.connect();
    await cluster.addTargetTopic(topic);
    await waitFor(
      async () => {
        await cluster.refreshMetadata();
        return cluster.findTopicPartitionMetadata(topic).length === partitions;
      },
      { ignoreTimeout: true },
    );
  } finally {
    await cluster.disconnect();
    await admin.disconnect();
  }
}

export async function retryProtocol<T>(errorType: string | readonly string[], fn: () => Promise<T>): Promise<T> {
  const types = new Set(typeof errorType === 'string' ? [errorType] : errorType);
  return waitFor(
    async () => {
      try {
        return await fn();
      } catch (e) {
        if (!types.has((e as { type?: string }).type ?? '')) throw e;
        return false;
      }
    },
    { ignoreTimeout: true },
  );
}

export function waitForMessages<T>(
  buffer: T[],
  { number = 1, delay = 50 }: { number?: number; delay?: number } = {},
): Promise<T[]> {
  return waitFor(() => (buffer.length >= number ? buffer : false), { delay, ignoreTimeout: true });
}

export function waitForNextEvent(
  consumer: Consumer,
  eventName: ConsumerEventName,
  { maxWait = 10_000 }: { maxWait?: number } = {},
): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const timeoutId = setTimeout(() => {
      reject(new Error(`Timeout waiting for '${eventName}'`));
    }, maxWait);
    consumer.on(eventName, (event) => {
      clearTimeout(timeoutId);
      resolve(event);
    });
    consumer.on(consumer.events.CRASH, (event) => {
      clearTimeout(timeoutId);
      reject((event.payload as { error: Error }).error);
    });
  });
}

export function waitForConsumerToJoinGroup(
  consumer: Consumer,
  { maxWait = 10_000, label = '' }: { maxWait?: number; label?: string } = {},
): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const timeoutId = setTimeout(() => {
      void consumer.disconnect().then(() => {
        reject(new Error(`Timeout ${label}`.trim()));
      });
    }, maxWait);
    consumer.on(consumer.events.GROUP_JOIN, (event) => {
      clearTimeout(timeoutId);
      resolve(event);
    });
    consumer.on(consumer.events.CRASH, (event) => {
      clearTimeout(timeoutId);
      void consumer.disconnect().then(() => {
        reject((event.payload as { error: Error }).error);
      });
    });
  });
}

export const testWaitFor = async <T>(
  fn: (elapsed: number) => T | false | Promise<T | false>,
  opts: Parameters<typeof waitFor>[1] = {},
): Promise<Exclude<T, false>> => waitFor(fn, { ignoreTimeout: true, ...opts });

export { waitFor, waitFor as waitForPoll };

type DescribeFn = (name: string, fn: () => void) => void;

function runDescribe(run: DescribeFn, name: string, fn: () => void): void {
  run(name, fn);
}

export function describeIfOauthbearerEnabled(name: string, fn: () => void): void {
  const run: DescribeFn = process.env.OAUTHBEARER_ENABLED === '1' ? describe : describe.skip;
  runDescribe(run, name, fn);
}

export function describeIfOauthbearerDisabled(name: string, fn: () => void): void {
  const run: DescribeFn = process.env.OAUTHBEARER_ENABLED === '1' ? describe.skip : describe;
  runDescribe(run, name, fn);
}

export function generateMessages({ prefix, number = 100 }: { prefix?: string; number?: number } = {}): {
  key: string;
  value: string;
}[] {
  const prefixOrEmpty = prefix ? `-${prefix}` : '';
  return Array.from({ length: number }, (_, i) => {
    const value = secureRandom();
    return {
      key: `key${prefixOrEmpty}-${i}-${value}`,
      value: `value${prefixOrEmpty}-${i}-${value}`,
    };
  });
}

export function createAdminClient(opts: CreateClusterOpts = {}, brokers = plainTextBrokers()) {
  const cluster = createCluster(opts, brokers);
  return { cluster, admin: createAdmin({ cluster, logger: newLogger() }) };
}
