import type { CustomPartitioner } from '../../types';
import { murmur2 } from './murmur2';
import { createPartitionerFactory } from './partitioner';

/**
 * Pre-2.0 murmur2 partitioner. Use this to keep existing key-to-partition routing.
 * @see https://kafka.apache.org/43/configuration/producer-configs/#partitioner.class
 */
export const LegacyPartitioner: CustomPartitioner = createPartitionerFactory(murmur2);
