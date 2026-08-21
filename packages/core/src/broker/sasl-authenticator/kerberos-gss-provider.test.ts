import { afterEach, describe, expect, it, vi } from 'vitest';
import { KafkaSASLAuthenticationError } from '../../errors';
import { applyKerberosEnv, createKerberosGssProvider, loadKerberosModule } from './kerberos-gss-provider';
import type { KerberosClientLike, KerberosModuleLike } from './kerberos-gss-provider';

describe('broker/sasl-authenticator/kerberos-gss-provider', () => {
  afterEach(() => {
    delete process.env.KRB5_CLIENT_KTNAME;
    delete process.env.KRB5_KTNAME;
    delete process.env.KRB5_CONFIG;
  });

  it('loadKerberosModule throws when the optional package is missing', async () => {
    await expect(loadKerberosModule()).rejects.toThrow(KafkaSASLAuthenticationError);
    await expect(loadKerberosModule()).rejects.toThrow('optional `kerberos` package');
  });

  it('applyKerberosEnv sets and restores keytab and krb5 paths', () => {
    process.env.KRB5_CONFIG = '/old/krb5.conf';
    const restore = applyKerberosEnv('/tmp/user.keytab', '/tmp/krb5.conf');
    expect(process.env.KRB5_CLIENT_KTNAME).toBe('/tmp/user.keytab');
    expect(process.env.KRB5_KTNAME).toBe('/tmp/user.keytab');
    expect(process.env.KRB5_CONFIG).toBe('/tmp/krb5.conf');
    restore();
    expect(process.env.KRB5_CLIENT_KTNAME).toBeUndefined();
    expect(process.env.KRB5_KTNAME).toBeUndefined();
    expect(process.env.KRB5_CONFIG).toBe('/old/krb5.conf');
  });

  it('steps until the GSS context is complete then RFC 4752 wraps', async () => {
    let contextComplete = false;
    const step = vi.fn(async (challenge: string) => {
      if (challenge === '') return Buffer.from('client-1').toString('base64');
      contextComplete = true;
      return Buffer.from('client-2').toString('base64');
    });
    const unwrap = vi
      .fn<(challenge: string) => Promise<string | null>>()
      .mockResolvedValue(Buffer.from([1, 0, 0, 255]).toString('base64'));
    const wrap = vi
      .fn<(challenge: string, options?: { user?: string }) => Promise<string | null>>()
      .mockResolvedValue(Buffer.from('wrapped').toString('base64'));

    const client: KerberosClientLike = {
      get contextComplete() {
        return contextComplete;
      },
      username: 'user@EXAMPLE.COM',
      step,
      unwrap,
      wrap,
    };

    const initializeClient = vi.fn().mockResolvedValue(client);
    const kerberos: KerberosModuleLike = {
      initializeClient,
      GSS_MECH_OID_KRB5: 9,
      GSS_C_MUTUAL_FLAG: 2,
      GSS_C_SEQUENCE_FLAG: 8,
      GSS_C_INTEG_FLAG: 32,
    };

    const provider = createKerberosGssProvider(
      { principal: 'user@EXAMPLE.COM', serviceName: 'kafka', authorizationIdentity: 'alice' },
      async () => kerberos,
    );

    const first = await provider({
      serverToken: null,
      host: 'broker.example.com',
      port: 9092,
      serviceName: 'kafka',
      principal: 'user@EXAMPLE.COM',
    });
    expect(first).toEqual({ token: Buffer.from('client-1'), complete: false });
    expect(step).toHaveBeenCalledWith('');

    const second = await provider({
      serverToken: Buffer.from('server-1'),
      host: 'broker.example.com',
      port: 9092,
      serviceName: 'kafka',
      principal: 'user@EXAMPLE.COM',
    });
    expect(second).toEqual({ token: Buffer.from('client-2'), complete: false });
    expect(step).toHaveBeenCalledWith(Buffer.from('server-1').toString('base64'));

    const third = await provider({
      serverToken: Buffer.from('server-wrap'),
      host: 'broker.example.com',
      port: 9092,
      serviceName: 'kafka',
      principal: 'user@EXAMPLE.COM',
    });
    expect(third).toEqual({ token: Buffer.from('wrapped'), complete: true });
    expect(unwrap).toHaveBeenCalledWith(Buffer.from('server-wrap').toString('base64'));
    expect(wrap).toHaveBeenCalledWith(Buffer.from([1, 0, 0, 255]).toString('base64'), { user: 'alice' });
    expect(initializeClient).toHaveBeenCalledWith('kafka@broker.example.com', {
      principal: 'user@EXAMPLE.COM',
      flags: 2 | 8 | 32,
      mechOID: 9,
    });
  });

  it('throws when the wrap round has no server token', async () => {
    const client: KerberosClientLike = {
      contextComplete: true,
      username: 'user@EXAMPLE.COM',
      step: vi.fn().mockResolvedValue(''),
      unwrap: vi.fn(),
      wrap: vi.fn(),
    };

    const provider = createKerberosGssProvider({}, async () => ({
      initializeClient: async () => client,
    }));

    await provider({ serverToken: null, host: 'broker', port: 9092, serviceName: 'kafka' });
    await expect(
      provider({ serverToken: Buffer.alloc(0), host: 'broker', port: 9092, serviceName: 'kafka' }),
    ).rejects.toThrow('did not send an RFC 4752 wrap token');
  });
});
