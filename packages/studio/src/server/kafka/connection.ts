import { existsSync } from 'node:fs';
import { resolve as resolvePath } from 'node:path';
import {
  discoverConfigFile,
  loadConfigFileAsync,
  loadConfigFileSync,
  mergeConfigLayers,
  type OnConfigDiagnostic,
} from '@cookiemonsterdev/kafka-config';
import { fromEnv, Kafka } from '@cookiemonsterdev/kafka-core';

/** A named, alternate connection layer — the same shape (and the same `cli.profiles` section) the CLI reads. */
export interface StudioProfile {
  readonly brokers?: readonly string[];
  readonly clientId?: string;
  readonly [key: string]: unknown;
}

export interface StudioConnectionConfig {
  /** Absolute path of the config file used, or `null` if none was found. */
  readonly path: string | null;
  readonly fileConfig: Record<string, unknown> | null;
  readonly env: Readonly<Record<string, string | undefined>>;
  readonly profiles: Readonly<Record<string, StudioProfile>>;
}

export class UnknownProfileError extends Error {
  override readonly name = 'UnknownProfileError';
  readonly profile: string;
  readonly available: readonly string[];

  constructor(profile: string, available: readonly string[]) {
    const list = available.length > 0 ? available.join(', ') : '(none configured)';
    super(`unknown profile "${profile}" — available profiles: ${list}`);
    this.profile = profile;
    this.available = available;
  }
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * Matched by `.name`, not `instanceof`: `KafkaConfigRequiresAsyncError` is thrown by
 * `@cookiemonsterdev/kafka-config`, a separate package — if this workspace ever ends up with two
 * installed copies of it, the classes are distinct objects even though the errors behave
 * identically. (Errors this module defines itself are matched with `instanceof` instead — that
 * risk doesn't apply to them.)
 */
function hasName(error: unknown, name: string): boolean {
  return typeof error === 'object' && error !== null && (error as { name?: unknown }).name === name;
}

function readProfiles(
  fileConfig: Record<string, unknown> | null,
  warn: (message: string) => void,
): Readonly<Record<string, StudioProfile>> {
  const cli = fileConfig?.cli;
  if (!isPlainObject(cli)) return {};

  const raw = cli.profiles;
  if (!isPlainObject(raw)) return {};

  const profiles: Record<string, StudioProfile> = {};
  for (const [name, value] of Object.entries(raw)) {
    if (isPlainObject(value)) {
      profiles[name] = value;
    } else {
      warn(`"cli.profiles.${name}" must be an object; ignoring it`);
    }
  }
  return profiles;
}

function resolveConfigPath(
  cwd: string,
  env: Readonly<Record<string, string | undefined>>,
  onDiagnostic: OnConfigDiagnostic,
): string | null {
  const explicit = env.KAFKA_CONFIG;
  if (explicit === undefined) return discoverConfigFile({ cwd, onDiagnostic });

  const resolved = resolvePath(cwd, explicit);
  if (!existsSync(resolved)) {
    throw new Error(`KAFKA_CONFIG "${explicit}" does not exist`);
  }
  return resolved;
}

export interface ResolveStudioConnectionOptions {
  readonly cwd: string;
  readonly env: Readonly<Record<string, string | undefined>>;
  readonly onDiagnostic?: OnConfigDiagnostic;
  /** A malformed `cli.profiles.<name>` entry is skipped, never thrown on — reported here instead. */
  readonly onWarn?: (message: string) => void;
}

/**
 * Loads the same `kafka.config.*` file `@cookiemonsterdev/kafka-cli` reads, so a project only has
 * to configure its cluster and named profiles once — the studio takes its multi-cluster model
 * straight from the file's `cli.profiles` section rather than inventing a second one.
 */
export async function resolveStudioConnectionConfig(
  options: ResolveStudioConnectionOptions,
): Promise<StudioConnectionConfig> {
  const { cwd, env } = options;
  const onDiagnostic = options.onDiagnostic ?? (() => {});
  const onWarn = options.onWarn ?? (() => {});
  const path = resolveConfigPath(cwd, env, onDiagnostic);

  let fileConfig: Record<string, unknown> | null = null;
  if (path !== null) {
    try {
      fileConfig = loadConfigFileSync<Record<string, unknown>>(path, { onDiagnostic });
    } catch (error) {
      if (hasName(error, 'KafkaConfigRequiresAsyncError')) {
        fileConfig = await loadConfigFileAsync<Record<string, unknown>>(path);
      } else {
        throw error;
      }
    }
  }

  return { path, fileConfig, env, profiles: readProfiles(fileConfig, onWarn) };
}

/** Merged one level deep rather than replaced atomically — matches core's own `Kafka` and the CLI's `buildConnectionOverrides`. */
const SHALLOW_MERGE_KEYS = ['retry'];

export function listProfileNames(config: StudioConnectionConfig): string[] {
  return Object.keys(config.profiles);
}

export function isKnownProfile(config: StudioConnectionConfig, profileName: string): boolean {
  return config.profiles[profileName] !== undefined;
}

/**
 * Builds a `Kafka` client for the given profile — `null` means the file's own top-level `client`
 * section (and environment variables) with no profile layered on top. Every call returns a fresh
 * client; {@link AdminPool} is what actually caches connections across requests.
 *
 * Precedence, highest first: environment variables, the profile, the config file's own `client`
 * section (merged inside `Kafka.from`), the constructor's own defaults — the same order the CLI
 * merges flags → env → profile → file → default, minus the flags layer studio doesn't have.
 */
export function createKafkaClient(config: StudioConnectionConfig, profileName: string | null): Kafka {
  const envOverrides = fromEnv(config.env) as Record<string, unknown>;
  let overrides = envOverrides;

  if (profileName !== null) {
    const profile = config.profiles[profileName];
    if (profile === undefined) throw new UnknownProfileError(profileName, listProfileNames(config));
    overrides = mergeConfigLayers(envOverrides, profile as Record<string, unknown>, {
      shallowMergeKeys: SHALLOW_MERGE_KEYS,
    });
  }

  return Kafka.from(config.fileConfig ?? {}, overrides);
}
