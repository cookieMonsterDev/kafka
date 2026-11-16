import { randomUUID } from 'node:crypto';
import { CONSUMER_GROUP_JOIN_EPOCH } from '../../protocol/requests/consumer-group-heartbeat/index';

/** ConsumerGroupHeartbeat (KIP-848) and ShareGroupHeartbeat (KIP-932) both join at member epoch 0. */
const JOIN_EPOCH = CONSUMER_GROUP_JOIN_EPOCH;

/** The fields of a ConsumerGroupHeartbeat or ShareGroupHeartbeat response that describe the member. */
export interface HeartbeatMemberResponse {
  memberId: string | null;
  memberEpoch: number;
  heartbeatIntervalMs: number;
}

/**
 * A member that joins and stays in its group through heartbeats: the consumer protocol (KIP-848)
 * and share groups (KIP-932). Tracks the member id and epoch, the broker's heartbeat interval,
 * and whether the next heartbeat has to carry the join fields (subscription, rack, and so on).
 */
export class HeartbeatMember {
  memberId: string | null = null;
  memberEpoch = JOIN_EPOCH;
  /** The interval the broker asked for on its last heartbeat response, or `null` before one arrives. */
  heartbeatIntervalMs: number | null = null;
  /** Whether the broker has accepted the member: a heartbeat returned an epoch above the join epoch. */
  isJoined = false;
  /** When the last heartbeat response arrived, in epoch milliseconds. */
  lastHeartbeatAt = 0;
  #joinFieldsDue = true;

  /** The member id to send; the client picks one before its first heartbeat (KIP-1082). */
  ensureMemberId(): string {
    this.memberId ??= randomUUID();
    return this.memberId;
  }

  get isJoining(): boolean {
    return this.memberEpoch === JOIN_EPOCH;
  }

  /** Whether the next heartbeat carries the join fields: at the join epoch, and on the first one after a reset. */
  get sendsJoinFields(): boolean {
    return this.#joinFieldsDue || this.isJoining;
  }

  /** Starts a join at the join epoch. The member id is kept, so a fenced member rejoins as itself. */
  startJoin(): void {
    this.reset();
    this.ensureMemberId();
  }

  /**
   * Goes back to the join epoch, so the next heartbeat rejoins. `forgetMemberId` drops the member
   * id too, for a coordinator that no longer knows it (`UNKNOWN_MEMBER_ID`).
   */
  reset({ forgetMemberId = false }: { forgetMemberId?: boolean } = {}): void {
    if (forgetMemberId) this.memberId = null;
    this.memberEpoch = JOIN_EPOCH;
    this.#joinFieldsDue = true;
    this.isJoined = false;
  }

  /** Records a heartbeat response. */
  applyResponse({ memberId, memberEpoch, heartbeatIntervalMs }: HeartbeatMemberResponse): void {
    if (memberId) this.memberId = memberId;
    this.memberEpoch = memberEpoch;
    this.heartbeatIntervalMs = heartbeatIntervalMs;
    this.lastHeartbeatAt = Date.now();
    this.#joinFieldsDue = false;
    if (memberEpoch > JOIN_EPOCH) this.isJoined = true;
  }

  /** When the next heartbeat is due: the broker's interval, or `fallbackIntervalMs` until it sends one. */
  nextHeartbeatAt(fallbackIntervalMs: number): number {
    return this.lastHeartbeatAt + (this.heartbeatIntervalMs ?? fallbackIntervalMs);
  }
}
