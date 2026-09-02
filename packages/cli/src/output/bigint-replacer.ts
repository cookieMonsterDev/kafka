/** A `JSON.stringify` replacer that renders a `bigint` as its decimal string instead of throwing. */
export function bigintAwareReplacer(_key: string, value: unknown): unknown {
  return typeof value === 'bigint' ? value.toString() : value;
}
