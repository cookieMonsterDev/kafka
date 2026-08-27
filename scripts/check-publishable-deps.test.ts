import { describe, expect, it } from 'vitest';
import { checkManifest, checkWorkspace, CORE_REQUIRED_DEPENDENCIES } from './check-publishable-deps.mjs';

const CORE_DEPENDENCIES = Object.fromEntries(CORE_REQUIRED_DEPENDENCIES.map((name) => [name, '^1.0.0']));

function coreManifest(overrides = {}) {
  return {
    name: '@cookiemonsterdev/kafka-core',
    version: '2.0.0',
    dependencies: CORE_DEPENDENCIES,
    ...overrides,
  };
}

function cliManifest(overrides = {}) {
  return {
    name: '@cookiemonsterdev/kafka-cli',
    version: '1.0.0',
    dependencies: { '@cookiemonsterdev/kafka-core': '^2.0.0' },
    ...overrides,
  };
}

describe('checkManifest', () => {
  it('passes a clean published manifest', () => {
    expect(checkManifest(coreManifest())).toEqual([]);
  });

  it('rejects a workspace: specifier in dependencies', () => {
    const problems = checkManifest(cliManifest({ dependencies: { '@cookiemonsterdev/kafka-core': 'workspace:^' } }));
    expect(problems).toEqual([expect.stringContaining('workspace:^')]);
  });

  it('rejects a workspace: specifier in peerDependencies', () => {
    const problems = checkManifest(
      cliManifest({ peerDependencies: { '@cookiemonsterdev/kafka-core': 'workspace:*' } }),
    );
    expect(problems).toEqual([expect.stringContaining('peerDependencies')]);
  });

  it('rejects a workspace: specifier in optionalDependencies', () => {
    const problems = checkManifest(
      cliManifest({ optionalDependencies: { '@cookiemonsterdev/kafka-core': 'workspace:^' } }),
    );
    expect(problems).toEqual([expect.stringContaining('optionalDependencies')]);
  });

  it('allows a workspace: specifier in a private package', () => {
    const problems = checkManifest({
      name: '@cookiemonsterdev/kafka-docs',
      version: '2.0.0',
      private: true,
      dependencies: { '@cookiemonsterdev/kafka-core': 'workspace:^' },
    });
    expect(problems).toEqual([]);
  });

  it('rejects a workspace-internal range the local version does not satisfy', () => {
    const manifest = {
      name: '@cookiemonsterdev/kafka-cli',
      version: '1.0.0',
      dependencies: { '@cookiemonsterdev/kafka-core': '^1.0.0' },
    };
    const problems = checkManifest(manifest, { '@cookiemonsterdev/kafka-core': '2.0.0' });
    expect(problems).toEqual([expect.stringContaining('does not admit the local version 2.0.0')]);
  });

  it('accepts a workspace-internal range the local version satisfies', () => {
    const manifest = {
      name: '@cookiemonsterdev/kafka-cli',
      version: '1.0.0',
      dependencies: { '@cookiemonsterdev/kafka-core': '^2.0.0' },
    };
    expect(checkManifest(manifest, { '@cookiemonsterdev/kafka-core': '2.3.0' })).toEqual([]);
  });

  it('rejects core dependencies drifting from the required set', () => {
    const problems = checkManifest(coreManifest({ dependencies: { ...CORE_DEPENDENCIES, extra: '^1.0.0' } }));
    expect(problems).toEqual([expect.stringContaining('dependencies must be exactly')]);
  });

  it('rejects core missing a required dependency', () => {
    const { 'lz4-lite': _omit, ...rest } = CORE_DEPENDENCIES;
    const problems = checkManifest(coreManifest({ dependencies: rest }));
    expect(problems).toEqual([expect.stringContaining('dependencies must be exactly')]);
  });
});

describe('checkWorkspace', () => {
  it('resolves workspace versions from every manifest before checking ranges', () => {
    const problems = checkWorkspace([coreManifest(), cliManifest()]);
    expect(problems).toEqual([]);
  });

  it('collects problems across every manifest', () => {
    const problems = checkWorkspace([
      coreManifest(),
      cliManifest({ dependencies: { '@cookiemonsterdev/kafka-core': 'workspace:^' } }),
      {
        ...cliManifest(),
        name: '@cookiemonsterdev/kafka-other',
        dependencies: { '@cookiemonsterdev/kafka-core': '^9.0.0' },
      },
    ]);
    expect(problems).toHaveLength(2);
  });
});
