const REQUIRED_NODE_MAJOR = 24;
const REQUIRED_BUN = [1, 4, 0] as const;

/** The subset of `process.versions` the startup check reads. */
export interface RuntimeVersions {
  readonly node: string;
  readonly bun?: string;
}

function parseVersion(version: string): number[] {
  const [release = ''] = version.split('-');
  return release.split('.').map((part) => Number.parseInt(part, 10) || 0);
}

function isAtLeast(version: string, minimum: readonly number[]): boolean {
  const parts = parseVersion(version);
  for (const [i, required] of minimum.entries()) {
    const actual = parts[i] ?? 0;
    if (actual !== required) return actual > required;
  }
  return true;
}

/**
 * Returns the message to print when the running runtime is too old, or `undefined` when it is
 * supported. Bun is checked against its own minimum because it reports a Node.js compatibility
 * version in `versions.node` that says nothing about which Bun release is running.
 */
export function unsupportedRuntimeMessage(versions: RuntimeVersions): string | undefined {
  if (versions.bun !== undefined) {
    if (isAtLeast(versions.bun, REQUIRED_BUN)) return undefined;
    return `kafka: requires Bun >=${REQUIRED_BUN.join('.')}, found ${versions.bun}`;
  }
  if (isAtLeast(versions.node, [REQUIRED_NODE_MAJOR])) return undefined;
  return `kafka: requires Node.js >=${String(REQUIRED_NODE_MAJOR)}.0.0, found ${versions.node}`;
}
