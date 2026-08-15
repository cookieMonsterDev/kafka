// https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/resource/PatternType.java#L32
export const RESOURCE_PATTERN_TYPES = {
  UNKNOWN: 0,
  ANY: 1,
  MATCH: 2,
  LITERAL: 3,
  PREFIXED: 4,
} as const

export type ResourcePatternType = (typeof RESOURCE_PATTERN_TYPES)[keyof typeof RESOURCE_PATTERN_TYPES]
