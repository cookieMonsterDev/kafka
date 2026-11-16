import type { TopicPartitions } from '../types';
import type { GroupMembership, MembershipHost } from './group-membership';

/** Assign mode: the application chose the partitions, so there is no group to join, heartbeat or leave. */
export class AssignModeMembership implements GroupMembership {
  readonly isGroupMember = false;
  readonly coordinator = null;
  readonly memberId = null;
  readonly generationId = null;
  readonly leaderId = null;
  readonly groupProtocol = null;
  readonly nextHeartbeatAt = Infinity;

  readonly #host: MembershipHost;
  readonly #assignment: readonly TopicPartitions[];

  constructor(host: MembershipHost, assignment: readonly TopicPartitions[]) {
    this.#host = host;
    this.#assignment = assignment.map(({ topic, partitions }) => ({ topic, partitions: [...partitions] }));
  }

  /** Installs the chosen partitions, refreshing metadata first so their partitions are known. */
  async join(): Promise<void> {
    await this.#host.cluster.refreshMetadata();
    await this.#host.changeAssignment({
      assignment: this.#assignment.map(({ topic, partitions }) => ({ topic, partitions: [...partitions] })),
      revoked: [],
      gained: [],
    });
    this.#host.logger.info('Assigned partitions, no group membership', { assignment: this.#assignment });
  }

  async heartbeat(): Promise<void> {}

  async leave(): Promise<void> {}

  resetRejectedMember(): void {}
}
