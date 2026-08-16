import type { CustomPartitioner } from '../../types.js';
import { murmur2 } from './murmur2.js';
import { createPartitionerFactory } from './partitioner.js';

export const LegacyPartitioner: CustomPartitioner = createPartitionerFactory(murmur2);
