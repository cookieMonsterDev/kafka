import type { ProtocolFactory, RequestFamily } from '../index';
import { describeUserScramCredentialsRequestV0 } from './v0/request';
import { describeUserScramCredentialsResponseV0 } from './v0/response';

export interface DescribeUserScramCredentialsOptions {
  users?: string[] | null;
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<DescribeUserScramCredentialsOptions>>> = {
  0: (options) => ({
    request: describeUserScramCredentialsRequestV0({
      users: options.users == null || options.users.length === 0 ? null : options.users.map((name) => ({ name })),
    }),
    response: describeUserScramCredentialsResponseV0,
  }),
};

export const DescribeUserScramCredentials: RequestFamily<DescribeUserScramCredentialsOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) {
      throw new Error(`Invariant violated: no DescribeUserScramCredentials protocol for version ${version}`);
    }
    return factory;
  },
});
