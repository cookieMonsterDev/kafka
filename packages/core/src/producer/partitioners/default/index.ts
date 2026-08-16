import type { CustomPartitioner } from '../../types.js';
import { createPartitionerFactory } from '../legacy/partitioner.js';
import { murmur2 } from './murmur2.js';

export const DefaultPartitioner: CustomPartitioner = createPartitionerFactory(murmur2);
