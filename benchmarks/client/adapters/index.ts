import { existsSync } from 'node:fs';
import type { ClientId } from '../spec.ts';
import type { ClientAdapter } from './types.ts';

const adapterFiles: Readonly<Record<ClientId, string>> = {
  'kafka-core': './kafka-core.ts',
  platformatic: './platformatic.ts',
  kafkajs: './kafkajs.ts',
  confluent: './confluent.ts',
};

// Signs that a package or its native addon is not installed or cannot load on this runtime, as
// opposed to a bug in the adapter.
const unsupportedPatterns: readonly RegExp[] = [
  /ERR_MODULE_NOT_FOUND/,
  /MODULE_NOT_FOUND/,
  /Cannot find (module|package)/i,
  /ERR_DLOPEN_FAILED/,
  /dlopen/i,
  /NODE_MODULE_VERSION/,
  /Could not locate the bindings file/i,
  /No native build was found/i,
  /invalid ELF header/i,
  /undefined symbol/i,
];

/** Whether an error means the client cannot run on this runtime, rather than that it failed. */
export function isUnsupportedError(error: unknown): boolean {
  const text = error instanceof Error ? `${error.name} ${error.message} ${String(errorCode(error))}` : String(error);
  return unsupportedPatterns.some((pattern) => pattern.test(text));
}

function errorCode(error: Error): unknown {
  return 'code' in error ? error.code : '';
}

/** Raised when an adapter module cannot be imported; `unsupported` follows `isUnsupportedError`. */
export class AdapterLoadError extends Error {
  readonly unsupported: boolean;

  constructor(clientId: ClientId, cause: unknown) {
    const reason = cause instanceof Error ? cause.message : String(cause);
    super(`cannot load the ${clientId} adapter: ${reason}`, { cause });
    this.name = 'AdapterLoadError';
    this.unsupported = isUnsupportedError(cause);
  }
}

function isClientAdapter(value: unknown, clientId: ClientId): value is ClientAdapter {
  if (typeof value !== 'object' || value === null) {
    return false;
  }
  const candidate = value as Partial<ClientAdapter>;
  return (
    candidate.id === clientId &&
    typeof candidate.packageName === 'string' &&
    typeof candidate.version === 'function' &&
    typeof candidate.createProducer === 'function' &&
    typeof candidate.createConsumer === 'function' &&
    typeof candidate.capabilityGaps === 'function'
  );
}

/**
 * Imports one adapter on demand, so a client whose package fails to load affects only its own
 * run. An adapter module exports its `ClientAdapter` as the default export.
 */
export async function loadAdapter(clientId: ClientId): Promise<ClientAdapter> {
  const adapterUrl = new URL(adapterFiles[clientId], import.meta.url);
  // Checked first so a missing adapter file reads as a harness bug, not as an unsupported client.
  if (!existsSync(adapterUrl)) {
    throw new Error(`adapter file ${adapterUrl.pathname} does not exist`);
  }
  let module: { default?: unknown };
  try {
    module = (await import(adapterUrl.href)) as { default?: unknown };
  } catch (error) {
    throw new AdapterLoadError(clientId, error);
  }
  if (!isClientAdapter(module.default, clientId)) {
    throw new AdapterLoadError(clientId, new Error(`the default export is not a ClientAdapter with id '${clientId}'`));
  }
  return module.default;
}
