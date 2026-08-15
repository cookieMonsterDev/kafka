// https://kafka.apache.org/protocol.html#The_Messages_FindCoordinator
export const COORDINATOR_TYPES = {
  GROUP: 0,
  TRANSACTION: 1,
} as const

export type CoordinatorType = (typeof COORDINATOR_TYPES)[keyof typeof COORDINATOR_TYPES]
