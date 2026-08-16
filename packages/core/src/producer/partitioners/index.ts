import { DefaultPartitioner } from './default/index.js';
import { LegacyPartitioner } from './legacy/index.js';

export { DefaultPartitioner, LegacyPartitioner };

/**
 * @deprecated Use `DefaultPartitioner` instead. The Java-compatible partitioner was renamed
 * `DefaultPartitioner` and made the default in 2.0.0.
 */
export const JavaCompatiblePartitioner = DefaultPartitioner;
