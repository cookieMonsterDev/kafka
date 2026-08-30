import { readFileSync } from 'node:fs';
import { LOG_LEVELS, type LogLevel } from '../loggers/index';
import type { KafkaConfig, SaslOptions } from '../types/index';

/**
 * A variable in the allow-list was present but could not be turned into the `KafkaConfig` field it
 * maps to. Never thrown — the field is simply omitted, so a typo in one variable does not stop
 * every other one from resolving.
 */
export interface FromEnvDiagnostic {
  code: 'config.env-invalid';
  level: 'warn';
  message: string;
  /** The environment variable name (with prefix) that could not be used. */
  key: string;
}

export type OnFromEnvDiagnostic = (diagnostic: FromEnvDiagnostic) => void;

/** Writes every diagnostic to stderr, prefixed `[kafka]` — matches `@cookiemonsterdev/kafka-config`'s own default. */
export const defaultOnFromEnvDiagnostic: OnFromEnvDiagnostic = (diagnostic) => {
  process.stderr.write(`[kafka] ${diagnostic.message}\n`);
};

export interface FromEnvOptions {
  /** Variable name prefix. Default `'KAFKA_'`. */
  prefix?: string;
  onDiagnostic?: OnFromEnvDiagnostic;
}

const SASL_MECHANISMS_WITH_PASSWORD = new Set(['plain', 'scram-sha-256', 'scram-sha-512']);

function warn(onDiagnostic: OnFromEnvDiagnostic, key: string, message: string): void {
  onDiagnostic({ code: 'config.env-invalid', level: 'warn', key, message: `${message} (from "${key}")` });
}

function parseBrokers(raw: string): readonly string[] {
  return raw
    .split(',')
    .map((broker) => broker.trim())
    .filter((broker) => broker.length > 0);
}

function parseBoolean(raw: string, key: string, onDiagnostic: OnFromEnvDiagnostic): boolean | undefined {
  const normalized = raw.trim().toLowerCase();
  if (normalized === 'true' || normalized === '1') return true;
  if (normalized === 'false' || normalized === '0') return false;
  warn(onDiagnostic, key, `"${raw}" is not a boolean (expected "true"/"false"/"1"/"0")`);
  return undefined;
}

function parseInteger(raw: string, key: string, onDiagnostic: OnFromEnvDiagnostic): number | undefined {
  const value = Number(raw.trim());
  if (Number.isFinite(value)) return value;
  warn(onDiagnostic, key, `"${raw}" is not a number`);
  return undefined;
}

function parseLogLevel(raw: string, key: string, onDiagnostic: OnFromEnvDiagnostic): LogLevel | undefined {
  const level = (LOG_LEVELS as Record<string, LogLevel | undefined>)[raw.trim().toUpperCase()];
  if (level !== undefined) return level;
  warn(onDiagnostic, key, `"${raw}" is not a log level (expected one of ${Object.keys(LOG_LEVELS).join(', ')})`);
  return undefined;
}

function readSslFile(raw: string, key: string, onDiagnostic: OnFromEnvDiagnostic): string | undefined {
  try {
    return readFileSync(raw.trim(), 'utf8');
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    warn(onDiagnostic, key, `could not read "${raw}": ${message}`);
    return undefined;
  }
}

function readSasl(env: NodeJS.ProcessEnv, prefix: string, onDiagnostic: OnFromEnvDiagnostic): SaslOptions | undefined {
  const mechanismKey = `${prefix}SASL_MECHANISM`;
  const usernameKey = `${prefix}SASL_USERNAME`;
  const passwordKey = `${prefix}SASL_PASSWORD`;

  const mechanism = env[mechanismKey]?.trim().toLowerCase();
  const username = env[usernameKey];
  const password = env[passwordKey];

  if (mechanism === undefined && username === undefined && password === undefined) return undefined;

  if (mechanism === undefined) {
    warn(
      onDiagnostic,
      usernameKey,
      `"${mechanismKey}" must also be set to build SASL credentials from the environment`,
    );
    return undefined;
  }

  if (!SASL_MECHANISMS_WITH_PASSWORD.has(mechanism)) {
    warn(
      onDiagnostic,
      mechanismKey,
      `"${mechanism}" cannot be built from environment variables (only ${[...SASL_MECHANISMS_WITH_PASSWORD].join(', ')} can)`,
    );
    return undefined;
  }

  if (username === undefined || password === undefined) {
    warn(onDiagnostic, mechanismKey, `both "${usernameKey}" and "${passwordKey}" are required`);
    return undefined;
  }

  return { mechanism, username, password } as SaslOptions;
}

function readSsl(
  env: NodeJS.ProcessEnv,
  prefix: string,
  onDiagnostic: OnFromEnvDiagnostic,
): KafkaConfig['ssl'] | undefined {
  const sslKey = `${prefix}SSL`;
  const caFileKey = `${prefix}SSL_CA_FILE`;
  const certFileKey = `${prefix}SSL_CERT_FILE`;
  const keyFileKey = `${prefix}SSL_KEY_FILE`;
  const rejectUnauthorizedKey = `${prefix}SSL_REJECT_UNAUTHORIZED`;

  const sslRaw = env[sslKey];
  const caFileRaw = env[caFileKey];
  const certFileRaw = env[certFileKey];
  const keyFileRaw = env[keyFileKey];
  const rejectUnauthorizedRaw = env[rejectUnauthorizedKey];

  if (
    sslRaw === undefined &&
    caFileRaw === undefined &&
    certFileRaw === undefined &&
    keyFileRaw === undefined &&
    rejectUnauthorizedRaw === undefined
  ) {
    return undefined;
  }

  const hasFileOptions =
    caFileRaw !== undefined ||
    certFileRaw !== undefined ||
    keyFileRaw !== undefined ||
    rejectUnauthorizedRaw !== undefined;
  if (!hasFileOptions) {
    return sslRaw === undefined ? undefined : parseBoolean(sslRaw, sslKey, onDiagnostic);
  }

  const ca = caFileRaw === undefined ? undefined : readSslFile(caFileRaw, caFileKey, onDiagnostic);
  const cert = certFileRaw === undefined ? undefined : readSslFile(certFileRaw, certFileKey, onDiagnostic);
  const key = keyFileRaw === undefined ? undefined : readSslFile(keyFileRaw, keyFileKey, onDiagnostic);
  const rejectUnauthorized =
    rejectUnauthorizedRaw === undefined
      ? undefined
      : parseBoolean(rejectUnauthorizedRaw, rejectUnauthorizedKey, onDiagnostic);

  return { ca, cert, key, rejectUnauthorized };
}

/**
 * Reads a fixed, documented allow-list of `KAFKA_*` variables into a `Partial<KafkaConfig>` — never
 * automatically; the library itself never reads `process.env` (the pre-existing `KAFKA_LOG_LEVEL`
 * read directly by the logger module is a documented wart, not a precedent this extends). Call it
 * explicitly, typically from a `kafka.config.ts` file: `client: fromEnv(process.env)`.
 *
 * Covers only the variables that map onto a `KafkaConfig` field: `${prefix}BROKERS`,
 * `${prefix}CLIENT_ID`, `${prefix}SASL_MECHANISM`/`_USERNAME`/`_PASSWORD`, `${prefix}SSL`/`_CA_FILE`/
 * `_CERT_FILE`/`_KEY_FILE`/`_REJECT_UNAUTHORIZED`, `${prefix}CONNECTION_TIMEOUT`,
 * `${prefix}REQUEST_TIMEOUT`, and `${prefix}LOG_LEVEL`. `KAFKA_PROFILE`, `KAFKA_CONFIG`, and
 * `KAFKA_OUTPUT` are CLI concepts with no `KafkaConfig` field and are not read here.
 *
 * A variable that cannot be parsed is reported through `onDiagnostic` (default: a warning on
 * stderr) and simply omitted — never `NaN`, never a thrown error.
 */
export function fromEnv(env: NodeJS.ProcessEnv, options: FromEnvOptions = {}): Partial<KafkaConfig> {
  const prefix = options.prefix ?? 'KAFKA_';
  const onDiagnostic = options.onDiagnostic ?? defaultOnFromEnvDiagnostic;

  const config: Partial<KafkaConfig> = {};

  const brokersKey = `${prefix}BROKERS`;
  const brokersRaw = env[brokersKey];
  if (brokersRaw !== undefined) config.brokers = parseBrokers(brokersRaw);

  const clientIdRaw = env[`${prefix}CLIENT_ID`];
  if (clientIdRaw !== undefined) config.clientId = clientIdRaw;

  const sasl = readSasl(env, prefix, onDiagnostic);
  if (sasl !== undefined) config.sasl = sasl;

  const ssl = readSsl(env, prefix, onDiagnostic);
  if (ssl !== undefined) config.ssl = ssl;

  const connectionTimeoutKey = `${prefix}CONNECTION_TIMEOUT`;
  const connectionTimeoutRaw = env[connectionTimeoutKey];
  if (connectionTimeoutRaw !== undefined) {
    const value = parseInteger(connectionTimeoutRaw, connectionTimeoutKey, onDiagnostic);
    if (value !== undefined) config.connectionTimeout = value;
  }

  const requestTimeoutKey = `${prefix}REQUEST_TIMEOUT`;
  const requestTimeoutRaw = env[requestTimeoutKey];
  if (requestTimeoutRaw !== undefined) {
    const value = parseInteger(requestTimeoutRaw, requestTimeoutKey, onDiagnostic);
    if (value !== undefined) config.requestTimeout = value;
  }

  const logLevelKey = `${prefix}LOG_LEVEL`;
  const logLevelRaw = env[logLevelKey];
  if (logLevelRaw !== undefined) {
    const value = parseLogLevel(logLevelRaw, logLevelKey, onDiagnostic);
    if (value !== undefined) config.logLevel = value;
  }

  return config;
}
