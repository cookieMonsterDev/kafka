export const RELEASE_PACKAGES = new Set(['core', 'cli', 'docs']);

export class UnknownReleasePackageError extends Error {
  constructor(pkg) {
    super(`Unknown release package: ${String(pkg)}`);
    this.name = 'UnknownReleasePackageError';
    this.pkg = pkg;
  }
}

export function resolveReleasePackage(pkg) {
  if (!RELEASE_PACKAGES.has(pkg)) {
    throw new UnknownReleasePackageError(pkg);
  }
  return pkg;
}
