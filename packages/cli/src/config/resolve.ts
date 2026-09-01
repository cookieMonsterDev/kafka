import { existsSync } from 'node:fs';
import { resolve as resolvePath } from 'node:path';
import {
  discoverConfigFile,
  loadConfigFileAsync,
  loadConfigFileSync,
  loadEnvFiles,
  type ConfigDiagnostic,
  type KafkaConfigError as GenericKafkaConfigError,
  type KafkaConfigRequiresAsyncError as GenericKafkaConfigRequiresAsyncError,
  type OnConfigDiagnostic,
} from '@cookiemonsterdev/kafka-config';
import { CliConfigError } from '../errors/cli-config-error';
import { readCliSection } from './cli-section';
import type { CliFileConfig } from './types';

export interface ResolveCliConfigOptions {
  readonly cwd: string;
  readonly env: Readonly<Record<string, string | undefined>>;
  /** From `--config-file`. A missing explicit path is a hard error, never a silent fallback. */
  readonly configFlag?: string;
  /** From `--profile`. An unknown name is a hard error listing what's actually configured. */
  readonly profileFlag?: string;
  /** Every diagnostic the generic loader reports — `config.loaded`, `config.multiple-candidates`, `config.transform-fallback`. */
  readonly onDiagnostic?: OnConfigDiagnostic;
  /** A CLI-owned warning that doesn't fit the generic loader's closed diagnostic-code union (e.g. an unknown `cli:` key). */
  readonly onWarn?: (message: string) => void;
}

/** Resolved once per invocation, in `main()`, and handed to every command via `CommandContext.config`. */
export interface ResolvedCliConfig {
  /** Absolute path of the config file used, or `null` if none was found (or discovery was never attempted). */
  readonly path: string | null;
  /**
   * The file's raw parsed content. Only structurally checked here — each of the five known
   * sections (`client`, `producer`, `consumer`, `shareConsumer`, `admin`), if present, must be a
   * plain object — never validated field-by-field against core's `KafkaFileConfig` shape, since
   * `Kafka.from()` (unlike `new Kafka()`/`Kafka.fromConfig()`) does not re-validate it either; a
   * connecting command hands this straight to `Kafka.from()`.
   */
  readonly fileConfig: Record<string, unknown> | null;
  /** This package's own `cli:` section, already warned-on-and-defaulted. */
  readonly cli: CliFileConfig;
  /** The active profile name (`--profile` / `KAFKA_PROFILE`), or `null` if none was requested. */
  readonly profile: string | null;
  /** Whether loading this file required the TypeScript-transform rescue (a non-erasable construct like an `enum` or an extensionless import, recovered through `require()`'s transform hooks). */
  readonly transformFallbackUsed: boolean;
}

function hasName<T extends { name: string }>(error: unknown, name: T['name']): error is T {
  return typeof error === 'object' && error !== null && (error as { name?: unknown }).name === name;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

const KNOWN_SECTIONS = ['client', 'producer', 'consumer', 'shareConsumer', 'admin'] as const;

/**
 * `Kafka.from()` (used by every connecting command's `openAdmin()`) does not itself validate a raw
 * `KafkaFileConfig`'s section shapes — only `loadKafkaConfig`/`loadKafkaConfigAsync` (used by
 * `new Kafka()`/`Kafka.fromConfig()`) do that. Left unchecked, a config file with, say,
 * `client: ['not', 'an', 'object']` would flow straight through the merge and surface as a
 * misleading `MissingBrokers` error instead of naming the actual problem — this catches that
 * before it gets that far.
 */
function assertKnownSectionsArePlainObjects(fileConfig: Record<string, unknown>, path: string): void {
  for (const section of KNOWN_SECTIONS) {
    const value = fileConfig[section];
    if (value !== undefined && !isPlainObject(value)) {
      const type = Array.isArray(value) ? 'an array' : value === null ? 'null' : typeof value;
      throw new CliConfigError(`"${section}" in "${path}" must be an object, not ${type}`);
    }
  }
}

function findConfigPath(options: ResolveCliConfigOptions, onDiagnostic: OnConfigDiagnostic): string | null {
  const explicit = options.configFlag ?? options.env.KAFKA_CONFIG;
  if (explicit !== undefined) {
    const resolved = resolvePath(options.cwd, explicit);
    if (!existsSync(resolved)) {
      throw new CliConfigError(`--config-file "${explicit}" does not exist`);
    }
    return resolved;
  }
  return discoverConfigFile({ cwd: options.cwd, onDiagnostic });
}

function resolveActiveProfile(options: ResolveCliConfigOptions, cli: CliFileConfig): string | null {
  const requested = options.profileFlag ?? options.env.KAFKA_PROFILE;
  if (requested === undefined) return null;

  const available = Object.keys(cli.profiles ?? {});
  if (!available.includes(requested)) {
    const list = available.length > 0 ? available.join(', ') : '(none configured)';
    throw new CliConfigError(`unknown profile "${requested}" — available profiles: ${list}`);
  }
  return requested;
}

/**
 * Loads a `kafka.config.*` file once for this invocation: load `.env` files (so `process.env`
 * reads inside the config module see them) → discover (or resolve `--config-file`) → load, with
 * the TypeScript-transform rescue tried synchronously first and the sync-only
 * {@link GenericKafkaConfigRequiresAsyncError} case retried through the async loader → read the
 * `cli:` section → resolve the active `--profile`.
 *
 * Deliberately does **not** import `@cookiemonsterdev/kafka-core` — every command pays for this
 * (it runs before any command's own logic), so it stays on the 26 KB, zero-dependency generic
 * loader. A connecting command hands the raw `fileConfig` here straight to `Kafka.from()`, lazily
 * inside core — only the structural, section-is-an-object check below runs before that; anything
 * more specific (an invalid `sasl.mechanism`, say) still only surfaces once core merges it.
 */
export async function resolveCliConfig(options: ResolveCliConfigOptions): Promise<ResolvedCliConfig> {
  loadEnvFiles({ cwd: options.cwd });

  let transformFallbackUsed = false;
  const onDiagnostic: OnConfigDiagnostic = (diagnostic: ConfigDiagnostic) => {
    if (diagnostic.code === 'config.transform-fallback') transformFallbackUsed = true;
    options.onDiagnostic?.(diagnostic);
  };
  const onWarn = options.onWarn ?? (() => {});

  const path = findConfigPath(options, onDiagnostic);
  let fileConfig: Record<string, unknown> | null = null;

  if (path !== null) {
    try {
      fileConfig = loadConfigFileSync<Record<string, unknown>>(path, { onDiagnostic });
    } catch (error) {
      if (hasName<GenericKafkaConfigRequiresAsyncError>(error, 'KafkaConfigRequiresAsyncError')) {
        fileConfig = await loadConfigFileAsync<Record<string, unknown>>(path);
      } else if (hasName<GenericKafkaConfigError>(error, 'KafkaConfigError')) {
        throw new CliConfigError(error.message);
      } else {
        throw error;
      }
    }
    onDiagnostic({ code: 'config.loaded', level: 'info', message: `Loaded kafka config from "${path}"`, path });
    assertKnownSectionsArePlainObjects(fileConfig, path);
  }

  const cli = readCliSection(fileConfig, onWarn);
  const profile = resolveActiveProfile(options, cli);

  return { path, fileConfig, cli, profile, transformFallbackUsed };
}
