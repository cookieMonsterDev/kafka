import { Encoder } from '../encoder.js';

const NUL = String.fromCharCode(0);

export interface AwsIamSaslConfig {
  authorizationIdentity: string;
  accessKeyId: string;
  secretAccessKey: string;
  sessionToken?: string;
}

export function awsIamRequest({
  authorizationIdentity,
  accessKeyId,
  secretAccessKey,
  sessionToken = '',
}: AwsIamSaslConfig): { encode(): Promise<Buffer> } {
  return {
    encode: async () =>
      new Encoder().writeBytes([authorizationIdentity, accessKeyId, secretAccessKey, sessionToken].join(NUL)).buffer,
  };
}

export const awsIamResponse = {
  decode: async (): Promise<boolean> => true,
  parse: async (): Promise<boolean> => true,
};
