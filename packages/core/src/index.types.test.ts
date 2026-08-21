import { randomBytes, randomUUID } from 'node:crypto';
import type { ConnectionOptions as TlsConnectionOptions } from 'node:tls';
import { describe, expectTypeOf, it } from 'vitest';
import {
  CompressionTypes,
  Kafka,
  logLevel,
  type Admin,
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
    }).toMatchTypeOf<SaslOptions>();

    expectTypeOf({
      mechanism: 'scram-sha-256' as const,
      username: 'u',
      password: 'p',
    }).toMatchTypeOf<SaslOptions>();

    expectTypeOf({
      mechanism: 'scram-sha-256' as const,
      tokenId: `token-${randomUUID()}`,
      tokenHmac: randomBytes(16),
    }).toMatchTypeOf<SaslOptions>();

    expectTypeOf({
      mechanism: 'scram-sha-512' as const,
      tokenId: `token-${randomUUID()}`,
      tokenHmac: randomBytes(24).toString('base64'),
    }).toMatchTypeOf<SaslOptions>();
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
});
