import { existsSync } from 'node:fs';
import { resolve as resolvePath } from 'node:path';
import {
  discoverConfigFile,
  loadConfigFileAsync,
  loadConfigFileSync,
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
  /** From `--config-file`. A missing explicit path is a hard error, never a silent fallback (D7). */
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
  /** The file's raw parsed content — not validated against core's `KafkaFileConfig` shape, since most commands never need to. Connecting commands hand this to `Kafka.from()`, which validates it itself. */
  readonly fileConfig: Record<string, unknown> | null;
  /** This package's own `cli:` section, already warned-on-and-defaulted. */
  readonly cli: CliFileConfig;
  /** The active profile name (`--profile` / `KAFKA_PROFILE`), or `null` if none was requested. */
  readonly profile: string | null;
  /** Whether loading this file required the D8 TypeScript-transform rescue. */
  readonly transformFallbackUsed: boolean;
}

function hasName<T extends { name: string }>(error: unknown, name: T['name']): error is T {
  return typeof error === 'object' && error !== null && (error as { name?: unknown }).name === name;
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
 * Loads a `kafka.config.*` file once for this invocation: discover (or resolve `--config`) →
 * load, with the D8 transform-hook rescue tried synchronously first and the sync-only
 * {@link GenericKafkaConfigRequiresAsyncError} case retried through the async loader → read the
 * `cli:` section → resolve the active `--profile`.
 *
 * Deliberately does **not** import `@cookiemonsterdev/kafka-core` — every command pays for this
 * (it runs before any command's own logic), so it stays on the 26 KB, zero-dependency generic
 * loader. A connecting command hands the raw `fileConfig` here to `Kafka.from()`, which validates
 * and merges it against the real `KafkaConfig` shape lazily, inside core.
 */
export async function resolveCliConfig(options: ResolveCliConfigOptions): Promise<ResolvedCliConfig> {
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
  }

  const cli = readCliSection(fileConfig, onWarn);
  const profile = resolveActiveProfile(options, cli);

  return { path, fileConfig, cli, profile, transformFallbackUsed };
}
