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

function readOutput(raw: unknown, warn: (message: string) => void): 'human' | 'json' | undefined {
  if (raw === undefined) return undefined;
  if (raw === 'human' || raw === 'json') return raw;
  warn('"cli.output" must be "human" or "json"; ignoring it');
  return undefined;
}

function readBoolean(raw: unknown, key: string, warn: (message: string) => void): boolean | undefined {
  if (raw === undefined) return undefined;
  if (typeof raw === 'boolean') return raw;
  warn(`"cli.${key}" must be a boolean; ignoring it`);
  return undefined;
}

function readNumber(raw: unknown, key: string, warn: (message: string) => void): number | undefined {
  if (raw === undefined) return undefined;
  if (typeof raw === 'number') return raw;
  warn(`"cli.${key}" must be a number; ignoring it`);
  return undefined;
}

function readTopicDefaults(raw: unknown, warn: (message: string) => void): CliTopicDefaults | undefined {
  if (raw === undefined) return undefined;
  if (!isPlainObject(raw)) {
    warn('"cli.topicDefaults" must be an object; ignoring it');
    return undefined;
  }
  return {
    partitions: readNumber(raw.partitions, 'topicDefaults.partitions', warn),
    replicationFactor: readNumber(raw.replicationFactor, 'topicDefaults.replicationFactor', warn),
  };
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
    output: readOutput(raw.output, warn),
    confirmDestructive: readBoolean(raw.confirmDestructive, 'confirmDestructive', warn),
    timeoutMs: readNumber(raw.timeoutMs, 'timeoutMs', warn),
    topicDefaults: readTopicDefaults(raw.topicDefaults, warn),
    profiles: readProfiles(raw.profiles, warn),
  };
}
