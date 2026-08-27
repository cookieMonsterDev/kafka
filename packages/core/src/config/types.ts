import type { AdminConfig, ConsumerConfig, KafkaConfig, ProducerConfig, ShareConsumerConfig } from '../types/index';

/**
 * Shape of a `kafka.config.*` file. Namespaced so new sections (e.g. `cli`) can be added without
 * a breaking change. Every section is optional; omit `client.brokers` and it is resolved from
 * elsewhere (an explicit `new Kafka({ brokers })`, a later env-var layer, ...).
 */
export interface KafkaFileConfig {
  client?: Omit<KafkaConfig, 'brokers'> & { brokers?: KafkaConfig['brokers'] };
  producer?: ProducerConfig;
  consumer?: Omit<ConsumerConfig, 'groupId'> & { groupId?: string };
  shareConsumer?: Omit<ShareConsumerConfig, 'groupId'> & { groupId?: string };
  admin?: AdminConfig;
  /** Forward compatibility: `cli`, and whatever a future core or CLI version adds. */
  [key: string]: unknown;
}

/** Lazily-produced {@link KafkaFileConfig}, for values that must be computed (secrets from Vault/AWS SM, ...). */
export type KafkaFileConfigFactory = () => KafkaFileConfig | Promise<KafkaFileConfig>;

/** Accepted shape of a config file's default export. */
export type KafkaFileConfigInput = KafkaFileConfig | KafkaFileConfigFactory;
