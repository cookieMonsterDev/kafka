import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { describeUserScramCredentialsRequestV0 } from './v0/request';
import { describeUserScramCredentialsResponseV0 } from './v0/response';

export interface DescribeUserScramCredentialsOptions {
  users?: string[] | null;
}

const VERSIONS = {
  0: (options) => ({
    request: describeUserScramCredentialsRequestV0({
      users: options.users == null || options.users.length === 0 ? null : options.users.map((name) => ({ name })),
    }),
    response: describeUserScramCredentialsResponseV0,
  }),
} satisfies VersionFactories<DescribeUserScramCredentialsOptions>;

export const DescribeUserScramCredentials = defineRequestFamily(KAFKA_APIS.DescribeUserScramCredentials, VERSIONS);
