// https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/requests/DescribeConfigsResponse.java
export const CONFIG_TYPE = Object.freeze({
  UNKNOWN: 0,
  BOOLEAN: 1,
  STRING: 2,
  INT: 3,
  SHORT: 4,
  LONG: 5,
  DOUBLE: 6,
  LIST: 7,
  CLASS: 8,
  PASSWORD: 9,
});

export type ConfigType = (typeof CONFIG_TYPE)[keyof typeof CONFIG_TYPE];
