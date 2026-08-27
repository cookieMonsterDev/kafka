import { randomBytes, randomUUID } from 'node:crypto';
import type { ConnectionOptions as TlsConnectionOptions } from 'node:tls';
import { describe, expectTypeOf, it } from 'vitest';
import type * as IndexModule from './index';
import {
  CompressionTypes,
  Kafka,
  logLevel,
  type Admin,
  type AdminConfig,
  type Batch,
  type Consumer,
  type KafkaConfig,
  type KafkaMessage,
  type LogLevel,
  type Producer,
  type RecordMetadata,
  type SaslOptions,
} from './index';

describe('public types', () => {
  it('types KafkaConfig, including ssl: true and SASL mechanism unions', () => {
    expectTypeOf<KafkaConfig>().toHaveProperty('brokers');
    expectTypeOf<KafkaConfig['ssl']>().toEqualTypeOf<TlsConnectionOptions | boolean | undefined>();
    expectTypeOf<KafkaConfig['logLevel']>().toEqualTypeOf<LogLevel | undefined>();
    expectTypeOf(logLevel.INFO).toEqualTypeOf<4>();

    expectTypeOf({
      mechanism: 'plain' as const,
      username: 'u',
      password: 'p',
    }).toExtend<SaslOptions>();

    expectTypeOf({
      mechanism: 'scram-sha-256' as const,
      username: 'u',
      password: 'p',
    }).toExtend<SaslOptions>();

    expectTypeOf({
      mechanism: 'scram-sha-256' as const,
      tokenId: `token-${randomUUID()}`,
      tokenHmac: randomBytes(16),
    }).toExtend<SaslOptions>();

    expectTypeOf({
      mechanism: 'scram-sha-512' as const,
      tokenId: `token-${randomUUID()}`,
      tokenHmac: randomBytes(24).toString('base64'),
    }).toExtend<SaslOptions>();

    expectTypeOf({
      mechanism: 'gssapi' as const,
      serviceName: 'kafka',
      principal: 'user@EXAMPLE.COM',
      gssProvider: async () => ({ token: Buffer.alloc(0), complete: true }),
    }).toExtend<SaslOptions>();
  });

  it('types producer/consumer/admin factories and bigint offsets', () => {
    const kafka = new Kafka({ brokers: ['localhost:9092'] });

    expectTypeOf(kafka.producer()).toEqualTypeOf<Producer>();
    expectTypeOf(kafka.consumer({ groupId: 'g' })).toEqualTypeOf<Consumer>();
    expectTypeOf(kafka.admin()).toEqualTypeOf<Admin>();

    expectTypeOf<RecordMetadata['baseOffset']>().toEqualTypeOf<bigint>();
    expectTypeOf<KafkaMessage['offset']>().toEqualTypeOf<bigint>();
    expectTypeOf<KafkaMessage['timestamp']>().toEqualTypeOf<bigint>();
  });

  it('types AbortSignal on connect/send/run and asyncDispose', () => {
    const kafka = new Kafka({ brokers: ['localhost:9092'] });
    const producer = kafka.producer();
    const consumer = kafka.consumer({ groupId: 'g' });

    expectTypeOf(producer.connect).toBeCallableWith({ signal: AbortSignal.abort() });
    expectTypeOf<Parameters<Producer['send']>[0]>().toHaveProperty('signal');
    expectTypeOf<NonNullable<Parameters<Consumer['run']>[0]>>().toHaveProperty('signal');
    expectTypeOf(consumer.stream).returns.toEqualTypeOf<AsyncIterableIterator<Batch>>();
    expectTypeOf(producer[Symbol.asyncDispose]).returns.toEqualTypeOf<Promise<void>>();
    expectTypeOf(consumer[Symbol.asyncDispose]).returns.toEqualTypeOf<Promise<void>>();
  });

  it('types CompressionTypes as the frozen literal map', () => {
    expectTypeOf(CompressionTypes.GZIP).toEqualTypeOf<1>();
    expectTypeOf(CompressionTypes.ZSTD).toEqualTypeOf<4>();
  });

  it('types metrics and bootstrapControllers', () => {
    expectTypeOf<KafkaConfig>().toHaveProperty('metrics');
    expectTypeOf<AdminConfig>().toHaveProperty('bootstrapControllers');
  });

  it('types connection knobs and partitionsFor helpers', () => {
    expectTypeOf<KafkaConfig>().toHaveProperty('connectionsMaxIdleMs');
    expectTypeOf<KafkaConfig>().toHaveProperty('clientDnsLookup');
    expectTypeOf<KafkaConfig>().toHaveProperty('reconnectBackoffMs');
    expectTypeOf<Producer>().toHaveProperty('listTopics');
    expectTypeOf<Producer>().toHaveProperty('partitionsFor');
    expectTypeOf<Consumer>().toHaveProperty('listTopics');
    expectTypeOf<Consumer>().toHaveProperty('partitionsFor');
    expectTypeOf<KafkaConfig>().toHaveProperty('enableMetricsPush');
    expectTypeOf<Producer>().toHaveProperty('clientInstanceId');
    expectTypeOf<Consumer>().toHaveProperty('clientInstanceId');
    expectTypeOf<Admin>().toHaveProperty('clientInstanceId');
  });

  it('does not re-export the config surface — that is the ./config subpath only', () => {
    expectTypeOf<typeof IndexModule>().not.toHaveProperty('defineConfig');
    expectTypeOf<typeof IndexModule>().not.toHaveProperty('loadKafkaConfig');
    expectTypeOf<typeof IndexModule>().not.toHaveProperty('loadConfigFileSync');
    expectTypeOf<typeof IndexModule>().not.toHaveProperty('loadConfigFileAsync');
    expectTypeOf<typeof IndexModule>().not.toHaveProperty('discoverConfigFile');
    expectTypeOf<typeof IndexModule>().not.toHaveProperty('mergeConfigLayers');
    expectTypeOf<typeof IndexModule>().not.toHaveProperty('KafkaConfigError');
    expectTypeOf<typeof IndexModule>().not.toHaveProperty('KafkaConfigRequiresAsyncError');
  });
});
