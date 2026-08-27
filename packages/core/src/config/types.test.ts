import { describe, expectTypeOf, it } from 'vitest';
import type { KafkaConfig } from '../types/index';
import { defineConfig } from './define-config';
import type { KafkaFileConfig } from './types';

describe('KafkaFileConfig', () => {
  it('makes client.brokers optional while KafkaConfig.brokers stays required', () => {
    expectTypeOf<NonNullable<KafkaFileConfig['client']>>().toHaveProperty('brokers');
    expectTypeOf<NonNullable<KafkaFileConfig['client']>['brokers']>().toEqualTypeOf<
      KafkaConfig['brokers'] | undefined
    >();
    expectTypeOf<KafkaConfig['brokers']>().not.toBeNullable();

    expectTypeOf({}).toExtend<KafkaFileConfig['client']>();
    expectTypeOf({ brokers: ['localhost:9092'] }).toExtend<KafkaFileConfig['client']>();
  });

  it('makes consumer.groupId and shareConsumer.groupId optional', () => {
    expectTypeOf({}).toExtend<KafkaFileConfig['consumer']>();
    expectTypeOf({}).toExtend<KafkaFileConfig['shareConsumer']>();
  });

  it('allows unknown top-level sections for forward compatibility', () => {
    expectTypeOf({ cli: { profiles: {} } }).toExtend<KafkaFileConfig>();
  });
});

/**
 * Proves the three ways a real `kafka.config.ts` author would reach for `KafkaFileConfig`
 * actually typecheck — not just that the type shape is right in the abstract (the `describe`
 * block above), but that the exact syntax the docs recommend compiles as written.
 */
describe('KafkaFileConfig, as a consumer would author it', () => {
  it('typechecks a literal via `satisfies KafkaFileConfig`, no defineConfig', () => {
    const config = {
      client: { brokers: ['localhost:9092'], clientId: 'my-app' },
      producer: { lingerMs: 5 },
    } satisfies KafkaFileConfig;

    expectTypeOf(config).toExtend<KafkaFileConfig>();
  });

  it('typechecks an explicitly-annotated `const config: KafkaFileConfig`', () => {
    const config: KafkaFileConfig = {
      client: { brokers: ['localhost:9092'] },
      consumer: { groupId: 'my-group' },
    };

    expectTypeOf(config).toExtend<KafkaFileConfig>();
  });

  it('composes `satisfies KafkaFileConfig` with `defineConfig`, the documented form', () => {
    const config = defineConfig({
      client: { brokers: ['localhost:9092'] },
      admin: { bootstrapControllers: ['localhost:9093'] },
    } satisfies KafkaFileConfig);

    // defineConfig<T extends KafkaFileConfigInput>(input: T): T — the result stays exactly as
    // specific as the literal passed in, not widened to the full KafkaFileConfig | Factory union.
    expectTypeOf(config).toExtend<KafkaFileConfig>();
  });
});
