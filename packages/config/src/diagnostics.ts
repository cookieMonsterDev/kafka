/**
 * A non-fatal event raised while discovering or loading a `kafka.config.*` file. `'warn'`
 * diagnostics reach stderr by default (see {@link defaultOnConfigDiagnostic}); `'info'` ones are
 * silent unless a caller supplies its own {@link OnConfigDiagnostic}.
 */
export interface ConfigDiagnostic {
  code: 'config.loaded' | 'config.multiple-candidates' | 'config.transform-fallback';
  level: 'info' | 'warn';
  message: string;
  path?: string;
  [key: string]: unknown;
}

export type OnConfigDiagnostic = (diagnostic: ConfigDiagnostic) => void;

/** Writes `'warn'` diagnostics to stderr, prefixed `[kafka-config]`. `'info'` diagnostics are silent. */
export const defaultOnConfigDiagnostic: OnConfigDiagnostic = (diagnostic) => {
  if (diagnostic.level !== 'warn') return;
  process.stderr.write(`[kafka-config] ${diagnostic.message}\n`);
};
