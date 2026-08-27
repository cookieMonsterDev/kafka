import { describe, expectTypeOf, it } from 'vitest';
import type { KafkaConfig } from '../types/index';
import type { KafkaFileConfig } from './types';

describe('KafkaFileConfig', () => {
  it('makes client.brokers optional while KafkaConfig.brokers stays required', () => {
    expectTypeOf<NonNullable<KafkaFileConfig['client']>>().toHaveProperty('brokers');
    expectTypeOf<NonNullable<KafkaFileConfig['client']>['brokers']>().toEqualTypeOf<
      KafkaConfig['brokers'] | undefined
    >();
    expectTypeOf<KafkaConfig['brokers']>().not.toBeNullable();

    expectTypeOf({}).toMatchTypeOf<KafkaFileConfig['client']>();
    expectTypeOf({ brokers: ['localhost:9092'] }).toMatchTypeOf<KafkaFileConfig['client']>();
  });

  it('makes consumer.groupId and shareConsumer.groupId optional', () => {
    expectTypeOf({}).toMatchTypeOf<KafkaFileConfig['consumer']>();
    expectTypeOf({}).toMatchTypeOf<KafkaFileConfig['shareConsumer']>();
  });

  it('allows unknown top-level sections for forward compatibility', () => {
    expectTypeOf({ cli: { profiles: {} } }).toMatchTypeOf<KafkaFileConfig>();
  });
});
