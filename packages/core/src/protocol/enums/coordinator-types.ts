// https://kafka.apache.org/protocol.html#The_Messages_FindCoordinator
export const COORDINATOR_TYPES = Object.freeze({
  GROUP: 0,
  TRANSACTION: 1,
})

export type CoordinatorType = (typeof COORDINATOR_TYPES)[keyof typeof COORDINATOR_TYPES]
