import { DefaultPartitioner } from './default/index';
import { LegacyPartitioner } from './legacy/index';

export { DefaultPartitioner, LegacyPartitioner };

/**
 * Alias of {@link DefaultPartitioner} (Java-compatible murmur2).
 * @see https://kafka.apache.org/43/configuration/producer-configs/#partitioner.class
 */
export const JavaCompatiblePartitioner = DefaultPartitioner;
