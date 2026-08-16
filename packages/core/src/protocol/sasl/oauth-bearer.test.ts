import { describe, expect, it } from 'vitest';
import { Decoder } from '../decoder';
import { Encoder } from '../encoder';
import { oauthBearerRequest, oauthBearerResponse } from './oauth-bearer';

const SOH = String.fromCharCode(1);

function readBytesOrThrow(decoder: Decoder): Buffer {
  const bytes = decoder.readBytes();
  if (bytes === null) throw new Error('expected a non-null bytes field');
  return bytes;
}

describe('protocol/sasl/oauth-bearer', () => {
  it('encodes the GS2 header, bearer token, and terminating SOH pair', async () => {
    const request = await oauthBearerRequest({}, { value: 'my-token' });
    const buffer = await request.encode();
    const decoder = new Decoder(buffer);
    expect(readBytesOrThrow(decoder).toString()).toBe(`n,,${SOH}auth=Bearer my-token${SOH}${SOH}`);
  });

  it('includes the authorization identity when present', async () => {
    const request = await oauthBearerRequest({ authorizationIdentity: 'admin' }, { value: 'my-token' });
    const buffer = await request.encode();
    const decoder = new Decoder(buffer);
    expect(readBytesOrThrow(decoder).toString()).toBe(`n,a=admin,${SOH}auth=Bearer my-token${SOH}${SOH}`);
  });

  it('formats extensions as key=value pairs separated by SOH', async () => {
    const request = await oauthBearerRequest({}, { value: 'my-token', extensions: { a: '1', b: '2' } });
    const buffer = await request.encode();
    const decoder = new Decoder(buffer);
    expect(readBytesOrThrow(decoder).toString()).toBe(`n,,${SOH}auth=Bearer my-token${SOH}a=1${SOH}b=2${SOH}${SOH}`);
  });

  it('treats an empty server payload as success', async () => {
    await expect(oauthBearerResponse.parse(await oauthBearerResponse.decode(Buffer.alloc(0)))).resolves.toEqual({
      status: 'ok',
    });
  });

  it('rejects an RFC 7628 invalid_token payload', async () => {
    const payload = Buffer.from('{"status":"invalid_token"}');
    await expect(oauthBearerResponse.parse(await oauthBearerResponse.decode(payload))).rejects.toThrow('invalid_token');

    const wrapped = new Encoder().writeBytes(payload).buffer;
    await expect(oauthBearerResponse.parse(await oauthBearerResponse.decode(wrapped))).rejects.toThrow('invalid_token');
  });
});
