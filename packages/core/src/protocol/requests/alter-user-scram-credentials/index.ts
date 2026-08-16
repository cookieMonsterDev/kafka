import type { ProtocolFactory, RequestFamily } from '../index';
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

const VERSIONS: Readonly<Record<number, ProtocolFactory<AlterUserScramCredentialsOptions>>> = {
  0: (options) => ({
    request: alterUserScramCredentialsRequestV0({
      deletions: options.deletions ?? [],
      upsertions: options.upsertions ?? [],
    }),
    response: alterUserScramCredentialsResponseV0,
  }),
};

export const AlterUserScramCredentials: RequestFamily<AlterUserScramCredentialsOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) {
      throw new Error(`Invariant violated: no AlterUserScramCredentials protocol for version ${version}`);
    }
    return factory;
  },
});
