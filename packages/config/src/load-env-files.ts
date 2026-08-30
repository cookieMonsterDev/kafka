import { resolve as resolvePath } from 'node:path';

export interface LoadEnvFilesOptions {
  /** Directory each file is resolved against. Default `process.cwd()`. */
  cwd?: string;
  /** Files to load, in order. Default `['.env']`. */
  files?: readonly string[];
}

export interface LoadEnvFilesResult {
  /** Absolute paths of the files that were loaded, in the order they were loaded. */
  loaded: string[];
  /** Absolute paths of the files that do not exist. Never a throw — a missing `.env` is routine. */
  missing: string[];
}

/**
 * Loads zero or more `.env`-style files into `process.env` via `process.loadEnvFile()`. A file
 * already loaded, or a variable already present in `process.env`, is never overridden — Node's own
 * env-file loading already has that behaviour, so this wrapper adds nothing on top of it beyond
 * multi-file sequencing and a non-throwing report of what was missing.
 */
export function loadEnvFiles(options: LoadEnvFilesOptions = {}): LoadEnvFilesResult {
  const cwd = options.cwd ?? process.cwd();
  const files = options.files ?? ['.env'];

  const loaded: string[] = [];
  const missing: string[] = [];

  for (const file of files) {
    const resolved = resolvePath(cwd, file);
    try {
      process.loadEnvFile(resolved);
      loaded.push(resolved);
    } catch (error) {
      if ((error as NodeJS.ErrnoException)?.code !== 'ENOENT') throw error;
      missing.push(resolved);
    }
  }

  return { loaded, missing };
}
