import { describe, expect, it, vi } from 'vitest';
import { createLogger, LOG_LEVELS } from '../../loggers/index.js';
import { awsIamAuthenticatorProvider } from './aws-iam.js';
import type { AwsIamSaslConfig } from '../../protocol/sasl/aws-iam.js';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

describe('broker/sasl-authenticator/aws-iam', () => {
  it('throws for a missing authorizationIdentity', async () => {
    const awsIam = awsIamAuthenticatorProvider({
      authorizationIdentity: '',
      accessKeyId: 'AKIA',
      secretAccessKey: 'secret',
    })({ host: '', port: 0, logger: silentLogger, saslAuthenticate: vi.fn() });

    await expect(awsIam.authenticate()).rejects.toThrow('Missing authorizationIdentity');
  });

  it('throws for a missing accessKeyId', async () => {
    const awsIam = awsIamAuthenticatorProvider({
      authorizationIdentity: 'identity',
      accessKeyId: '',
      secretAccessKey: 'secret',
    })({ host: '', port: 0, logger: silentLogger, saslAuthenticate: vi.fn() });

    await expect(awsIam.authenticate()).rejects.toThrow('Missing accessKeyId');
  });

  it('throws for a missing secretAccessKey', async () => {
    const awsIam = awsIamAuthenticatorProvider({
      authorizationIdentity: 'identity',
      accessKeyId: 'AKIA',
      secretAccessKey: '',
    })({ host: '', port: 0, logger: silentLogger, saslAuthenticate: vi.fn() });

    await expect(awsIam.authenticate()).rejects.toThrow('Missing secretAccessKey');
  });

  it('defaults sessionToken to an empty string and authenticates', async () => {
    const saslAuthenticate = vi.fn().mockResolvedValue(true);
    const config: AwsIamSaslConfig = { authorizationIdentity: 'identity', accessKeyId: 'AKIA', secretAccessKey: 'secret' };
    const awsIam = awsIamAuthenticatorProvider(config)({
      host: 'broker',
      port: 9092,
      logger: silentLogger,
      saslAuthenticate,
    });

    await awsIam.authenticate();
    expect(saslAuthenticate).toHaveBeenCalledOnce();
    expect(config.sessionToken).toBe('');
  });
});
