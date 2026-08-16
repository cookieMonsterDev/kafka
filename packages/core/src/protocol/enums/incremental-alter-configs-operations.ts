/**
 * `AlterConfigOp.OpType` for IncrementalAlterConfigs (API key 44).
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const INCREMENTAL_ALTER_CONFIGS_OPERATIONS = Object.freeze({
  SET: 0,
  DELETE: 1,
  APPEND: 2,
  SUBTRACT: 3,
});

export type IncrementalAlterConfigsOperation =
  (typeof INCREMENTAL_ALTER_CONFIGS_OPERATIONS)[keyof typeof INCREMENTAL_ALTER_CONFIGS_OPERATIONS];
