import type { IncomingMessage } from 'node:http';
import { UnknownProfileError } from './kafka/connection';

/** `JSON.stringify` that renders a `bigint` (every Kafka offset, in every route) as its decimal string instead of throwing. */
export function stringifyJson(value: unknown): string {
  return JSON.stringify(value, (_key: string, val: unknown) => (typeof val === 'bigint' ? val.toString() : val));
}

const DEFAULT_MAX_BODY_BYTES = 1_000_000;

export class RequestBodyTooLargeError extends Error {
  override readonly name = 'RequestBodyTooLargeError';

  constructor(limit: number) {
    super(`request body exceeds ${String(limit)} bytes`);
  }
}

export class InvalidJsonBodyError extends Error {
  override readonly name = 'InvalidJsonBodyError';

  constructor(cause: unknown) {
    super(`request body is not valid JSON: ${cause instanceof Error ? cause.message : String(cause)}`);
  }
}

/**
 * Reads and parses a request body as JSON, capped at `limit` bytes so a client can't exhaust
 * memory with an unbounded body. An empty body parses as `undefined`, not an error — a route that
 * requires a body still has to check for that itself.
 */
export function readJsonBody(req: IncomingMessage, limit = DEFAULT_MAX_BODY_BYTES): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let bytes = 0;

    req.on('data', (chunk: Buffer) => {
      bytes += chunk.length;
      if (bytes > limit) {
        req.destroy();
        reject(new RequestBodyTooLargeError(limit));
        return;
      }
      chunks.push(chunk);
    });

    req.on('end', () => {
      const raw = Buffer.concat(chunks).toString('utf8').trim();
      if (raw.length === 0) {
        resolve(undefined);
        return;
      }
      try {
        resolve(JSON.parse(raw));
      } catch (error) {
        reject(new InvalidJsonBodyError(error));
      }
    });

    req.on('error', reject);
  });
}

export interface MappedApiError {
  readonly status: number;
  readonly code: string;
  readonly message: string;
  readonly details?: unknown;
}

/**
 * Matched by `.name`, not `instanceof`: both are thrown by `@cookiemonsterdev/kafka-config`, a
 * separate package — if this workspace ever ends up with two installed copies of it, the classes
 * are distinct objects even though the errors behave identically. Errors this package defines
 * itself (below) are matched with `instanceof` instead, since that risk doesn't apply to them.
 */
function hasName(error: unknown, name: string): boolean {
  return typeof error === 'object' && error !== null && (error as { name?: unknown }).name === name;
}

function messageOf(error: unknown, fallback: string): string {
  return error instanceof Error ? error.message : fallback;
}

/**
 * Maps an error thrown while handling a request to the status and error envelope it should
 * produce. Only the small set of errors a route already knows how to name more specifically is
 * recognized here — everything else still becomes a `500 internal_error`, matching the previous,
 * un-mapped behavior.
 */
export function mapErrorToApiError(error: unknown): MappedApiError {
  if (error instanceof RequestBodyTooLargeError || error instanceof InvalidJsonBodyError) {
    return { status: 400, code: 'bad_request', message: messageOf(error, 'invalid request body') };
  }

  if (error instanceof UnknownProfileError) {
    return {
      status: 404,
      code: 'unknown_profile',
      message: error.message,
      details: { available: error.available },
    };
  }

  if (hasName(error, 'KafkaConfigError') || hasName(error, 'KafkaConfigRequiresAsyncError')) {
    return { status: 500, code: 'config_error', message: messageOf(error, 'invalid kafka config') };
  }

  return { status: 500, code: 'internal_error', message: messageOf(error, 'unexpected error') };
}
