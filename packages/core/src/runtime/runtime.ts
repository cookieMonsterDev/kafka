/** True under Bun. `process.versions.node` is a compatibility value there, so read `versions.bun`. */
export function isBun(): boolean {
  return typeof process.versions.bun === 'string';
}

/**
 * Whether to use a Bun-native implementation (for example `Bun.zstdCompress`) instead of the `node:` one.
 * Node.js always gets the `node:` implementation. On Bun, `KAFKA_BUN_NATIVE=0` turns every native
 * path off so an application can fall back to Bun's Node.js compatibility layer without a code change.
 * Read on each call so a process (or a test) can change it before creating a client.
 */
export function isBunNativeEnabled(): boolean {
  return isBun() && process.env.KAFKA_BUN_NATIVE !== '0';
}
