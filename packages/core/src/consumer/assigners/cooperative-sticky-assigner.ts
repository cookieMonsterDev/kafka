import type { Cluster } from '../../cluster/index';
import type { PartitionAssigner } from '../types';
import { createStickyAssigner } from './sticky-assigner';

/**
 * Incremental sticky assignor (Java `cooperative-sticky`, KIP-429).
 *
 * `assign()` computes the desired sticky assignment, then leaves partitions that still belong
 * to another member unassigned this generation so they can move on the next join. The consumer
 * group currently uses eager join/sync (revoke all, then assign); that remains correct with this
 * assignor, it just does not get the stop-the-world-free rebalance.
 *
 * @see https://kafka.apache.org/43/design/design/
 */
export const cooperativeSticky: PartitionAssigner = ({ cluster }: { cluster: Cluster }): Assigner =>
  createStickyAssigner({ cluster, name: 'cooperative-sticky', protocolType: 'cooperative', cooperative: true });
