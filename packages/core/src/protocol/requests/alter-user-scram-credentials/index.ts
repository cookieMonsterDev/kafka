import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import {
  alterUserScramCredentialsRequestV0,
  type ScramCredentialDeletion,
  type ScramCredentialUpsertion,
} from './v0/request';
import { alterUserScramCredentialsResponseV0 } from './v0/response';

export type { ScramCredentialDeletion, ScramCredentialUpsertion };

export interface AlterUserScramCredentialsOptions {
  deletions?: ScramCredentialDeletion[];
  upsertions?: ScramCredentialUpsertion[];
}

const VERSIONS = {
  0: (options) => ({
    request: alterUserScramCredentialsRequestV0({
      deletions: options.deletions ?? [],
      upsertions: options.upsertions ?? [],
    }),
    response: alterUserScramCredentialsResponseV0,
  }),
} satisfies VersionFactories<AlterUserScramCredentialsOptions>;

export const AlterUserScramCredentials = defineRequestFamily(KAFKA_APIS.AlterUserScramCredentials, VERSIONS);
