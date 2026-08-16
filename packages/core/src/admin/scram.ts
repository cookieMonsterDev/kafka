import crypto from 'node:crypto';
import { KafkaNonRetriableError } from '../errors';
import { SCRAM_MECHANISMS } from '../protocol/enums/scram-mechanisms';
import type { AlterUserScramCredentialsResponseV0Body } from '../protocol/requests/alter-user-scram-credentials/v0/response';
import type { DescribeUserScramCredentialsResponseV0Body } from '../protocol/requests/describe-user-scram-credentials/v0/response';
import { retrier } from '../retry/index';
import type { AdminContext } from './helpers';
import { formatUnknown, protocolType } from './helpers';

const DEFAULT_ITERATIONS = 4096;
const DEFAULT_SALT_BYTES = 16;

export interface ScramCredentialDeletionInput {
  name: string;
  mechanism: number;
}

export type ScramCredentialUpsertionInput =
  | {
      name: string;
      mechanism: number;
      iterations?: number;
      password: string;
      salt?: Buffer;
    }
  | {
      name: string;
      mechanism: number;
      iterations: number;
      salt: Buffer;
      saltedPassword: Buffer;
    };

export interface ScramApi {
  describeUserScramCredentials: (options?: {
    users?: string[] | null;
  }) => Promise<{ results: DescribeUserScramCredentialsResponseV0Body['results'] }>;
  alterUserScramCredentials: (options: {
    deletions?: ScramCredentialDeletionInput[];
    upsertions?: ScramCredentialUpsertionInput[];
  }) => Promise<{ results: AlterUserScramCredentialsResponseV0Body['results'] }>;
}

function digestFor(mechanism: number): { length: number; type: 'sha256' | 'sha512' } {
  if (mechanism === SCRAM_MECHANISMS.SCRAM_SHA_256) return { length: 32, type: 'sha256' };
  if (mechanism === SCRAM_MECHANISMS.SCRAM_SHA_512) return { length: 64, type: 'sha512' };
  throw new KafkaNonRetriableError(`Invalid SCRAM mechanism ${mechanism}`);
}

function hi(
  password: string,
  salt: Buffer,
  iterations: number,
  digest: { length: number; type: string },
): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    crypto.pbkdf2(password, salt, iterations, digest.length, digest.type, (err, derivedKey) =>
      err ? reject(err) : resolve(derivedKey),
    );
  });
}

async function toWireUpsertion(upsertion: ScramCredentialUpsertionInput) {
  if ('saltedPassword' in upsertion) {
    return {
      name: upsertion.name,
      mechanism: upsertion.mechanism,
      iterations: upsertion.iterations,
      salt: upsertion.salt,
      saltedPassword: upsertion.saltedPassword,
    };
  }

  const iterations = upsertion.iterations ?? DEFAULT_ITERATIONS;
  if (iterations < DEFAULT_ITERATIONS) {
    throw new KafkaNonRetriableError(`SCRAM iterations must be at least ${DEFAULT_ITERATIONS}`);
  }
  const salt = upsertion.salt ?? crypto.randomBytes(DEFAULT_SALT_BYTES);
  const saltedPassword = await hi(upsertion.password, salt, iterations, digestFor(upsertion.mechanism));
  return {
    name: upsertion.name,
    mechanism: upsertion.mechanism,
    iterations,
    salt,
    saltedPassword,
  };
}

export function createScramApi({ cluster, logger, retry }: AdminContext): ScramApi {
  const describeUserScramCredentials = async (
    options: { users?: string[] | null } = {},
  ): Promise<{ results: DescribeUserScramCredentialsResponseV0Body['results'] }> => {
    return retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata();
        const broker = await cluster.findControllerBroker();
        const { results } = await broker.describeUserScramCredentials({ users: options.users });
        return { results };
      } catch (error) {
        if (protocolType(error) === 'NOT_CONTROLLER') {
          logger.warn('Could not describe SCRAM credentials', {
            error: error instanceof Error ? error.message : String(error),
            retryCount,
            retryTime,
          });
          await cluster.refreshMetadata();
          throw error;
        }
        bail(error as Error);
        throw error;
      }
    });
  };

  const alterUserScramCredentials = async ({
    deletions = [],
    upsertions = [],
  }: {
    deletions?: ScramCredentialDeletionInput[];
    upsertions?: ScramCredentialUpsertionInput[];
  }): Promise<{ results: AlterUserScramCredentialsResponseV0Body['results'] }> => {
    if (!Array.isArray(deletions) || !Array.isArray(upsertions)) {
      throw new KafkaNonRetriableError(`Invalid SCRAM alterations ${formatUnknown({ deletions, upsertions })}`);
    }
    if (deletions.length === 0 && upsertions.length === 0) {
      throw new KafkaNonRetriableError('Must provide at least one SCRAM deletion or upsertion');
    }

    for (const deletion of deletions) {
      digestFor(deletion.mechanism);
      if (!deletion.name) throw new KafkaNonRetriableError('SCRAM deletion requires a user name');
    }
    for (const upsertion of upsertions) {
      digestFor(upsertion.mechanism);
      if (!upsertion.name) throw new KafkaNonRetriableError('SCRAM upsertion requires a user name');
    }

    const wireUpsertions = await Promise.all(upsertions.map(toWireUpsertion));

    return retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata();
        const broker = await cluster.findControllerBroker();
        const { results } = await broker.alterUserScramCredentials({ deletions, upsertions: wireUpsertions });
        return { results };
      } catch (error) {
        if (protocolType(error) === 'NOT_CONTROLLER') {
          logger.warn('Could not alter SCRAM credentials', {
            error: error instanceof Error ? error.message : String(error),
            retryCount,
            retryTime,
          });
          await cluster.refreshMetadata();
          throw error;
        }
        bail(error as Error);
        throw error;
      }
    });
  };

  return { describeUserScramCredentials, alterUserScramCredentials };
}
