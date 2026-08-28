import type { AdminConfig, ConsumerConfig, KafkaConfig, ProducerConfig, ShareConsumerConfig } from '../types/index';

/**
 * Shape of a `kafka.config.*` file's default export. Namespaced from day one so a future section
 * (a CLI `cli:` block, and whatever comes after it) can be added without a breaking change.
 * `[key: string]: unknown` means an older `@cookiemonsterdev/kafka-core` never rejects a config
 * file that also carries a section it doesn't know about yet.
 *
 * @see https://kafka.apache.org/43/getting-started/introduction/
 */
export interface KafkaFileConfig {
  /**
   * Options merged into `new Kafka()` / `Kafka.fromConfig()` / `Kafka.from()`. `config` itself is
   * excluded — it only means something as an argument to those calls, never as a value to resolve
   * from a file.
   */
  client?: Omit<KafkaConfig, 'brokers' | 'config'> & { brokers?: KafkaConfig['brokers'] };
  /** Options merged into every `Kafka.producer()` call, under whatever that call passes directly. */
  producer?: ProducerConfig;
  /** Options merged into every `Kafka.consumer()` call, under whatever that call passes directly. */
  consumer?: Omit<ConsumerConfig, 'groupId'> & { groupId?: string };
  /** Options merged into every `Kafka.shareConsumer()` call, under whatever that call passes directly. */
  shareConsumer?: Omit<ShareConsumerConfig, 'groupId'> & { groupId?: string };
  /** Options merged into every `Kafka.admin()` call, under whatever that call passes directly. */
  admin?: AdminConfig;
  [key: string]: unknown;
}
