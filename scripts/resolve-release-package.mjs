// Ordered by publish dependency: config -> core -> cli -> docs (D19). A later package always
// builds against the freshly-released version of the ones before it, so this order is the
// contract release-chain.mjs walks — not just a listing.
export const RELEASE_PACKAGES = [
  {
    name: 'config',
    publishesToNpm: true,
    npmName: '@cookiemonsterdev/kafka-config',
  },
  {
    name: 'core',
    publishesToNpm: true,
    npmName: '@cookiemonsterdev/kafka-core',
  },
  {
    name: 'cli',
    publishesToNpm: true,
    npmName: '@cookiemonsterdev/kafka-cli',
  },
  {
    name: 'docs',
    publishesToNpm: false,
    buildEnv: { GITHUB_PAGES: '1' },
    npmName: '@cookiemonsterdev/kafka-docs',
  },
];

export const RELEASE_PACKAGE_NAMES = new Set(RELEASE_PACKAGES.map((pkg) => pkg.name));

export class UnknownReleasePackageError extends Error {
  constructor(pkg) {
    super(`Unknown release package: ${String(pkg)}. Expected one of: ${[...RELEASE_PACKAGE_NAMES].join(', ')}`);
    this.name = 'UnknownReleasePackageError';
    this.pkg = pkg;
  }
}

export function resolveReleasePackage(pkg) {
  if (!RELEASE_PACKAGE_NAMES.has(pkg)) {
    throw new UnknownReleasePackageError(pkg);
  }
  return pkg;
}
