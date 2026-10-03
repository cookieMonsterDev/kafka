import { existsSync } from 'node:fs';
import { extname, resolve as resolvePath } from 'node:path';
import {
  defaultOnConfigDiagnostic,
  discoverConfigFile,
  mergeConfigLayers,
  type OnConfigDiagnostic,
} from '@cookiemonsterdev/kafka-config';
import { KafkaConfigError } from '../errors';
import type { KafkaConfig } from '../types/index';
import { loadKafkaConfig, loadKafkaConfigAsync } from './load';
import type { KafkaFileConfig } from './types';

/**
 * Keys merged one level deep instead of replaced atomically — matches what `client.ts`'s
 * producer/consumer/admin already do for their own `retry` option.
 */
export const SHALLOW_MERGE_KEYS = ['retry'];

/** `KafkaConfig` has no index signature; this local alias is what lets it flow through the generic, string-indexed `mergeConfigLayers`. */
export type MergeableKafkaConfig = KafkaConfig & Record<string, unknown>;

/** `KafkaConfig` after resolution — `brokers` is guaranteed present (resolution throws otherwise). */
export interface ResolvedKafkaConfig extends Omit<KafkaConfig, 'brokers' | 'config'> {
  brokers: NonNullable<KafkaConfig['brokers']>;
}

export interface ResolveKafkaConfigResult {
  config: ResolvedKafkaConfig;
  /** Absolute path of the config file that was used, or `null` if none was loaded. */
  path: string | null;
  /** The file's full parsed content, or `null` if no file was loaded. */
  fileConfig: KafkaFileConfig | null;
}

export interface ResolveKafkaConfigOptions {
  /** Directory discovery starts from. Default `process.cwd()`. */
  cwd?: string;
  onDiagnostic?: OnConfigDiagnostic;
}

/**
 * `undefined` (or `null`) `brokers` after merging every layer means neither the call nor any
 * config file resolved a value — the promised runtime error, naming what was searched.
 */
function assertBrokersResolved(
  merged: Partial<KafkaConfig>,
  path: string | null,
  searchedDescription: string,
): asserts merged is ResolvedKafkaConfig {
  if (merged.brokers !== undefined && merged.brokers !== null) return;
  throw new KafkaConfigError(
    'MissingBrokers',
    `No "brokers" were provided, and none were found ${searchedDescription}. Fix: pass "brokers" directly, ` +
      'or add a kafka.config.ts file exporting "client.brokers".',
    { path: path ?? undefined },
  );
}

/**
 * Decides whether — and where — to look for a `kafka.config.*` file, per {@link KafkaConfig.config}:
 * a string is an explicit path (a missing one is a hard error, never a silent fallback); `false`
 * never discovers; `true` always discovers; omitted discovers only when `brokers` is absent, so a
 * call that already passes `brokers` never touches the filesystem.
 */
function findConfigPath(explicit: KafkaConfig, cwd: string, onDiagnostic: OnConfigDiagnostic): string | null {
  const configOption = explicit.config;

  if (configOption === false) {
    return null;
  }

  if (typeof configOption === 'string') {
    const resolved = resolvePath(cwd, configOption);
    if (!existsSync(resolved)) {
      throw new KafkaConfigError('ConfigFileNotFound', `kafka config file "${resolved}" does not exist.`, {
        path: resolved,
      });
    }
    return resolved;
  }

  if (configOption !== true && explicit.brokers !== undefined) {
    return null;
  }

  return discoverConfigFile({ cwd, onDiagnostic });
}

/**
 * `config` is a resolution instruction, never a resolved value — it must not survive into the
 * merged result (which is typed as excluding it, via {@link ResolvedKafkaConfig}) even though
 * `explicit` itself carries it.
 */
function withoutConfigKey(explicit: KafkaConfig): MergeableKafkaConfig {
  const rest: MergeableKafkaConfig = { ...explicit };
  delete rest.config;
  return rest;
}

function mergeWithExplicit(explicit: KafkaConfig, fileConfig: KafkaFileConfig | null): Partial<KafkaConfig> {
  return mergeConfigLayers<MergeableKafkaConfig>(withoutConfigKey(explicit), fileConfig?.client, {
    shallowMergeKeys: SHALLOW_MERGE_KEYS,
  });
}

const syncLoadDeprecationWarned = new Set<string>();

/**
 * `new Kafka()` keeps loading a TS/JS config file synchronously so nothing breaks, but that path
 * depends on `require()` (and, on Node, its transform-hook rescue) and is deprecated in favour of
 * `Kafka.fromConfig()`. JSON stays synchronous and silent: `JSON.parse` needs neither. Warns once
 * per path per process, so N clients built from one config file don't repeat it.
 */
function warnSyncLoadDeprecated(path: string, onDiagnostic: OnConfigDiagnostic): void {
  if (extname(path) === '.json' || syncLoadDeprecationWarned.has(path)) return;
  syncLoadDeprecationWarned.add(path);
  const fix = 'use "await Kafka.fromConfig()" instead, or switch to a kafka.config.json file';
  onDiagnostic({
    code: 'config.sync-load-deprecated',
    level: 'warn',
    message:
      `kafka config file "${path}" was loaded synchronously by new Kafka(), which is deprecated for ` +
      `TypeScript/JavaScript config files. Fix: ${fix}.`,
    path,
    fix,
  });
}

/**
 * Resolves `new Kafka()`'s options: discover, load, merge. Resolution order, highest first:
 * explicit argument → config file → constructor default — a key defined nowhere is omitted from
 * the result, so the constructor's own destructuring default fires naturally.
 *
 * A `.json` config file loads synchronously and silently. A TS/JS one still loads through the
 * deprecated synchronous loader, with a one-time `config.sync-load-deprecated` warning per path —
 * prefer {@link resolveKafkaConfigAsync} (`Kafka.fromConfig()`).
 */
export function resolveKafkaConfig(
  explicit: KafkaConfig,
  options: ResolveKafkaConfigOptions = {},
): ResolveKafkaConfigResult {
  const cwd = options.cwd ?? process.cwd();
  const onDiagnostic = options.onDiagnostic ?? defaultOnConfigDiagnostic;

  const path = findConfigPath(explicit, cwd, onDiagnostic);
  const fileConfig = path == null ? null : loadKafkaConfig(path, { onDiagnostic });
  if (path != null) {
    onDiagnostic({ code: 'config.loaded', level: 'info', message: `Loaded kafka config from "${path}"`, path });
    warnSyncLoadDeprecated(path, onDiagnostic);
  }

  const merged = mergeWithExplicit(explicit, fileConfig);
  assertBrokersResolved(merged, path, `(searched upward from "${cwd}")`);

  return { config: merged, path, fileConfig };
}

/** Async sibling of {@link resolveKafkaConfig}, used by `Kafka.fromConfig()`. Shares discovery and the merge function, so the two paths cannot drift. */
export async function resolveKafkaConfigAsync(
  explicit: KafkaConfig,
  options: ResolveKafkaConfigOptions = {},
): Promise<ResolveKafkaConfigResult> {
  const cwd = options.cwd ?? process.cwd();
  const onDiagnostic = options.onDiagnostic ?? defaultOnConfigDiagnostic;

  const path = findConfigPath(explicit, cwd, onDiagnostic);
  const fileConfig = path == null ? null : await loadKafkaConfigAsync(path);
  if (path != null) {
    onDiagnostic({ code: 'config.loaded', level: 'info', message: `Loaded kafka config from "${path}"`, path });
  }

  const merged = mergeWithExplicit(explicit, fileConfig);
  assertBrokersResolved(merged, path, `(searched upward from "${cwd}")`);

  return { config: merged, path, fileConfig };
}

/**
 * Merges an already-loaded {@link KafkaFileConfig} with call-site overrides — no discovery, no
 * filesystem read. Backs `Kafka.from()`, for a caller (the CLI) that loaded the file once itself.
 */
export function resolveKafkaConfigFrom(fileConfig: KafkaFileConfig, overrides: KafkaConfig = {}): ResolvedKafkaConfig {
  const merged = mergeWithExplicit(overrides, fileConfig);
  assertBrokersResolved(merged, null, 'in the given config');
  return merged;
}
