/** Acknowledgement types for ShareFetch / ShareAcknowledge (KIP-932). */
export const SHARE_ACKNOWLEDGE_TYPE = Object.freeze({
  GAP: 0,
  ACCEPT: 1,
  RELEASE: 2,
  REJECT: 3,
  RENEW: 4,
} as const);

export type ShareAcknowledgeType = (typeof SHARE_ACKNOWLEDGE_TYPE)[keyof typeof SHARE_ACKNOWLEDGE_TYPE];
