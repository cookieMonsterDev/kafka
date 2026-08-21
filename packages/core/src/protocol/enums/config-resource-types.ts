// https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/config/ConfigResource.java
export const CONFIG_RESOURCE_TYPES = Object.freeze({
  UNKNOWN: 0,
  TOPIC: 2,
  BROKER: 4,
  BROKER_LOGGER: 8,
  CLIENT_METRICS: 16,
  GROUP: 32,
});

export type ConfigResourceType = (typeof CONFIG_RESOURCE_TYPES)[keyof typeof CONFIG_RESOURCE_TYPES];
