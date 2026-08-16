import { describe, expect, it } from 'vitest';
import { KafkaNonRetriableError } from '../errors';
import { parseBrokerAddress } from './parse-broker-address';

describe('cluster/parseBrokerAddress', () => {
  it('parses hostname:port', () => {
    expect(parseBrokerAddress('broker-1:9092')).toEqual({ host: 'broker-1', port: 9092 });
  });

  it('parses bracketed IPv6 addresses', () => {
    expect(parseBrokerAddress('[::1]:9092')).toEqual({ host: '::1', port: 9092 });
    expect(parseBrokerAddress('[2001:db8::1]:9093')).toEqual({ host: '2001:db8::1', port: 9093 });
  });

  it('rejects a missing port', () => {
    expect(() => parseBrokerAddress('localhost')).toThrow(KafkaNonRetriableError);
    expect(() => parseBrokerAddress('localhost')).toThrow('missing a port');
  });

  it('rejects a non-numeric or out-of-range port instead of passing NaN to net.connect', () => {
    expect(() => parseBrokerAddress('localhost:abc')).toThrow('invalid port');
    expect(() => parseBrokerAddress('localhost:99999')).toThrow('invalid port');
    expect(() => parseBrokerAddress('[::1]:')).toThrow('missing a port');
  });

  it('rejects unbracketed IPv6 so host/port are not split on the last colon', () => {
    expect(() => parseBrokerAddress('::1:9092')).toThrow('[host]:port');
  });
});
