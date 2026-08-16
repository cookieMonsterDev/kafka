import type { CustomPartitioner } from '../../types';
import { createPartitionerFactory } from '../legacy/partitioner';
import { murmur2 } from './murmur2';

/**
 * Java-compatible murmur2 partitioner (the default).
 * @see https://kafka.apache.org/43/configuration/producer-configs/#partitioner.class
 */
export const DefaultPartitioner: CustomPartitioner = createPartitionerFactory(murmur2);
