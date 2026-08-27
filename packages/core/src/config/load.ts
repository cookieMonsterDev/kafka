import { existsSync } from 'node:fs';
import { extname, resolve } from 'node:path';
import { CANDIDATE_EXTENSIONS, discoverConfigFile } from './discover';
import { defaultOnConfigDiagnostic, type OnConfigDiagnostic } from './diagnostics';
import { KafkaConfigError } from './errors';
import { loadConfigFileSync } from './load-sync';
import type { KafkaFileConfig } from './types';

export interface LoadKafkaConfigOptions {
  /** Directory to search from. Default `process.cwd()`. */
  cwd?: string;
  /**
   * Explicit config file path, resolved against `cwd`. Takes precedence over `KAFKA_CONFIG` and
   * discovery. A missing explicit path is a hard error (`'ConfigFileNotFound'`), never a silent
   * fallback to discovery.
   */
  path?: string;
  /** Walk upward toward the filesystem root when no explicit path is given. Default `true`. */
  searchParents?: boolean;
  /** See {@link import('./load-sync').LoadConfigFileSyncOptions.allowTransformFallback}. Default `true`. */
  allowTransformFallback?: boolean;
  onDiagnostic?: OnConfigDiagnostic;
  /** Source for the `KAFKA_CONFIG` override. Default `process.env`. */
  env?: Record<string, string | undefined>;
}

export type LoadKafkaConfigResult =
  | { ok: true; resolvedPath: string | null; config: KafkaFileConfig }
  | { ok: false; resolvedPath: string | null; error: KafkaConfigError };

function isSupportedExtension(ext: string): ext is (typeof CANDIDATE_EXTENSIONS)[number] {
  return (CANDIDATE_EXTENSIONS as readonly string[]).includes(ext);
}

function resolveExplicitPath(cwd: string, path: string): string {
  const resolved = resolve(cwd, path);
  if (!existsSync(resolved)) {
    throw new KafkaConfigError('ConfigFileNotFound', `kafka config file "${resolved}" does not exist`, {
      path: resolved,
    });
  }
  return resolved;
}

/**
 * Discovers (unless an explicit path or `KAFKA_CONFIG` is given) and loads a `kafka.config.*`
 * file. Never throws — every failure comes back as `{ ok: false, error }`, tagged by
 * {@link KafkaConfigError.tag} so callers can branch without parsing `.message`. The `config` on a
 * success result is always a frozen plain object (never a getter-backed or exotic one), even when
 * nothing was found (`{}`).
 */
export function loadKafkaConfig(options: LoadKafkaConfigOptions = {}): LoadKafkaConfigResult {
  const cwd = options.cwd ?? process.cwd();
  const onDiagnostic = options.onDiagnostic ?? defaultOnConfigDiagnostic;
  const env = options.env ?? process.env;

  try {
    const explicitPath = options.path ?? env.KAFKA_CONFIG;

    const resolvedPath =
      explicitPath != null && explicitPath !== ''
        ? resolveExplicitPath(cwd, explicitPath)
        : discoverConfigFile({ cwd, searchParents: options.searchParents, onDiagnostic });

    if (resolvedPath == null) {
      onDiagnostic({ code: 'config.loaded', level: 'info', message: 'No kafka config file found' });
      return { ok: true, resolvedPath: null, config: Object.freeze({}) };
    }

    const ext = extname(resolvedPath);
    if (!isSupportedExtension(ext)) {
      throw new KafkaConfigError(
        'UnsupportedExtension',
        `kafka config file "${resolvedPath}" has an unsupported extension "${ext}". Supported: ${CANDIDATE_EXTENSIONS.join(', ')}`,
        { path: resolvedPath },
      );
    }

    const config = loadConfigFileSync(resolvedPath, {
      allowTransformFallback: options.allowTransformFallback,
      onDiagnostic,
    });

    onDiagnostic({
      code: 'config.loaded',
      level: 'info',
      message: `Loaded kafka config from "${resolvedPath}"`,
      path: resolvedPath,
    });

    return { ok: true, resolvedPath, config: Object.freeze(config) };
  } catch (error) {
    if (error instanceof KafkaConfigError) {
      return { ok: false, resolvedPath: error.path ?? null, error };
    }

    const message = error instanceof Error ? error.message : String(error);
    return {
      ok: false,
      resolvedPath: null,
      error: new KafkaConfigError('ConfigLoadError', `Failed to load kafka config: ${message}`, { cause: error }),
    };
  }
}
