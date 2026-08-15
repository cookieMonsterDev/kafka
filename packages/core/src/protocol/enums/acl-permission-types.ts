// https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/acl/AclPermissionType.java#L31
export const ACL_PERMISSION_TYPES = Object.freeze({
  UNKNOWN: 0,
  ANY: 1,
  DENY: 2,
  ALLOW: 3,
})

export type AclPermissionType = (typeof ACL_PERMISSION_TYPES)[keyof typeof ACL_PERMISSION_TYPES]
