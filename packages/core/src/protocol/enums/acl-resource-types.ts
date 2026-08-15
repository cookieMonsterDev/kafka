// https://github.com/apache/kafka/blob/a15387f34d142684859c2a57fcbef25edcdce25a/clients/src/main/java/org/apache/kafka/common/resource/ResourceType.java#L25-L31
export const ACL_RESOURCE_TYPES = {
  UNKNOWN: 0,
  ANY: 1,
  TOPIC: 2,
  GROUP: 3,
  CLUSTER: 4,
  TRANSACTIONAL_ID: 5,
  DELEGATION_TOKEN: 6,
} as const

export type AclResourceType = (typeof ACL_RESOURCE_TYPES)[keyof typeof ACL_RESOURCE_TYPES]
