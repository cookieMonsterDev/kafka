import type { CliFileConfig, CliProfile, CliTopicDefaults } from './types';

const KNOWN_KEYS: ReadonlySet<string> = new Set([
  'output',
  'confirmDestructive',
  'timeoutMs',
  'topicDefaults',
  'profiles',
]);

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function readTopicDefaults(raw: unknown, warn: (message: string) => void): CliTopicDefaults | undefined {
  if (raw === undefined) return undefined;
  if (!isPlainObject(raw)) {
    warn('"cli.topicDefaults" must be an object; ignoring it');
    return undefined;
  }
  const partitions = typeof raw.partitions === 'number' ? raw.partitions : undefined;
  const replicationFactor = typeof raw.replicationFactor === 'number' ? raw.replicationFactor : undefined;
  return { partitions, replicationFactor };
}

function readProfiles(raw: unknown, warn: (message: string) => void): Readonly<Record<string, CliProfile>> | undefined {
  if (raw === undefined) return undefined;
  if (!isPlainObject(raw)) {
    warn('"cli.profiles" must be an object; ignoring it');
    return undefined;
  }
  const profiles: Record<string, CliProfile> = {};
  for (const [name, value] of Object.entries(raw)) {
    if (!isPlainObject(value)) {
      warn(`"cli.profiles.${name}" must be an object; ignoring it`);
      continue;
    }
    profiles[name] = value;
  }
  return profiles;
}

/**
 * Reads the `cli:` section of a raw, already-loaded config file object. Never throws — an unknown
 * top-level key inside `cli:` (or a known key holding the wrong shape) is reported through `warn`
 * and otherwise ignored, so a config written for a newer CLI still loads under an older one.
 */
export function readCliSection(
  fileConfig: Record<string, unknown> | null,
  warn: (message: string) => void = () => {},
): CliFileConfig {
  const raw = fileConfig?.cli;
  if (raw === undefined) return {};
  if (!isPlainObject(raw)) {
    warn('the "cli" section of the config file must be an object; ignoring it');
    return {};
  }

  for (const key of Object.keys(raw)) {
    if (!KNOWN_KEYS.has(key)) {
      warn(`unknown "cli.${key}" in the config file; ignoring it`);
    }
  }

  return {
    output: raw.output === 'human' || raw.output === 'json' ? raw.output : undefined,
    confirmDestructive: typeof raw.confirmDestructive === 'boolean' ? raw.confirmDestructive : undefined,
    timeoutMs: typeof raw.timeoutMs === 'number' ? raw.timeoutMs : undefined,
    topicDefaults: readTopicDefaults(raw.topicDefaults, warn),
    profiles: readProfiles(raw.profiles, warn),
  };
}
