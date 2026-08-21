import type { Cluster } from '../../cluster/index';
import type { PartitionAssigner } from '../types';
import { createStickyAssigner } from './sticky-assigner';

/**
 * Incremental sticky assignor (Java `cooperative-sticky`, KIP-429).
 *
 * `assign()` computes the desired sticky assignment, then leaves partitions that still belong
 * to another member unassigned this generation so they can move on the follow-up join. The
 * consumer group retains all other partitions and automatically performs that settling
 * generation when the assignment revokes partitions.
 *
 * @see https://kafka.apache.org/43/design/design/
 */
export const cooperativeSticky: PartitionAssigner = ({ cluster }: { cluster: Cluster }) =>
  createStickyAssigner({ cluster, name: 'cooperative-sticky', protocolType: 'cooperative', cooperative: true });
