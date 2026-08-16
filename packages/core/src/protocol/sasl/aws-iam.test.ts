import { describe, expect, it } from 'vitest';
import { Decoder } from '../decoder';
import { awsIamRequest, awsIamResponse } from './aws-iam';

const NUL = String.fromCharCode(0);

function readBytesOrThrow(decoder: Decoder): Buffer {
  const bytes = decoder.readBytes();
  if (bytes === null) throw new Error('expected a non-null bytes field');
  return bytes;
}

describe('protocol/sasl/aws-iam', () => {
  it('encodes authzid, accessKeyId, secretAccessKey and sessionToken NUL-joined', async () => {
    const buffer = await awsIamRequest({
      authorizationIdentity: 'identity',
      accessKeyId: 'AKIA...',
      secretAccessKey: 'secret',
      sessionToken: 'token',
    }).encode();

    const decoder = new Decoder(buffer);
    expect(readBytesOrThrow(decoder).toString()).toBe(`identity${NUL}AKIA...${NUL}secret${NUL}token`);
  });

  it('defaults sessionToken to an empty string', async () => {
    const buffer = await awsIamRequest({
      authorizationIdentity: 'identity',
      accessKeyId: 'AKIA...',
      secretAccessKey: 'secret',
    }).encode();

    const decoder = new Decoder(buffer);
    expect(readBytesOrThrow(decoder).toString()).toBe(`identity${NUL}AKIA...${NUL}secret${NUL}`);
  });

  it('response decode/parse both resolve true without inspecting the bytes', async () => {
    await expect(awsIamResponse.decode()).resolves.toBe(true);
    await expect(awsIamResponse.parse()).resolves.toBe(true);
  });
});
