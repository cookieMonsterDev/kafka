import dns from 'node:dns/promises';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { canonicalHostname, connectHappyEyeballs, resolveBrokerAddresses, sortHappyEyeballs } from './dns-lookup';

vi.mock('node:dns/promises', () => ({
  default: {
    resolveCname: vi.fn(),
    lookup: vi.fn(),
    reverse: vi.fn(),
  },
}));

describe('network/dns-lookup', () => {
  beforeEach(() => {
    vi.mocked(dns.resolveCname).mockReset();
    vi.mocked(dns.lookup).mockReset();
    vi.mocked(dns.reverse).mockReset();
  });

  describe('sortHappyEyeballs', () => {
    it('interleaves IPv6 then IPv4', () => {
      expect(
        sortHappyEyeballs([
          { address: '10.0.0.1', family: 4 },
          { address: '2001:db8::1', family: 6 },
          { address: '10.0.0.2', family: 4 },
        ]),
      ).toEqual([
        { address: '2001:db8::1', family: 6 },
        { address: '10.0.0.1', family: 4 },
        { address: '10.0.0.2', family: 4 },
      ]);
    });
  });

  describe('resolveBrokerAddresses', () => {
    it('returns a bare IPv4 without DNS', async () => {
      await expect(resolveBrokerAddresses('127.0.0.1')).resolves.toEqual({
        hostname: '127.0.0.1',
        addresses: [{ address: '127.0.0.1', family: 4 }],
      });
      expect(dns.lookup).not.toHaveBeenCalled();
    });

    it('returns a bare IPv6 without DNS', async () => {
      await expect(resolveBrokerAddresses('::1')).resolves.toEqual({
        hostname: '::1',
        addresses: [{ address: '::1', family: 6 }],
      });
    });

    it('resolves every A/AAAA for useAllDnsIps', async () => {
      vi.mocked(dns.lookup).mockResolvedValue([
        { address: '2001:db8::1', family: 6 },
        { address: '10.0.0.1', family: 4 },
      ] as never);

      await expect(resolveBrokerAddresses('broker.example', 'useAllDnsIps')).resolves.toEqual({
        hostname: 'broker.example',
        addresses: [
          { address: '2001:db8::1', family: 6 },
          { address: '10.0.0.1', family: 4 },
        ],
      });
      expect(dns.lookup).toHaveBeenCalledWith('broker.example', { all: true, verbatim: true });
    });

    it('follows CNAME then looks up the canonical name', async () => {
      vi.mocked(dns.resolveCname).mockResolvedValue(['kafka.internal.example']);
      vi.mocked(dns.lookup).mockResolvedValue([{ address: '10.0.0.9', family: 4 }] as never);

      await expect(resolveBrokerAddresses('bootstrap.example', 'canonicalBootstrap')).resolves.toEqual({
        hostname: 'kafka.internal.example',
        addresses: [{ address: '10.0.0.9', family: 4 }],
      });
      expect(dns.lookup).toHaveBeenCalledWith('kafka.internal.example', { all: true, verbatim: true });
    });
  });

  describe('canonicalHostname', () => {
    it('returns IPs unchanged', async () => {
      await expect(canonicalHostname('10.0.0.1')).resolves.toBe('10.0.0.1');
    });

    it('falls back to PTR when there is no CNAME', async () => {
      vi.mocked(dns.resolveCname).mockRejectedValue(Object.assign(new Error('ENODATA'), { code: 'ENODATA' }));
      vi.mocked(dns.lookup).mockResolvedValue({ address: '10.0.0.1', family: 4 });
      vi.mocked(dns.reverse).mockResolvedValue(['broker.example.com']);

      await expect(canonicalHostname('bootstrap')).resolves.toBe('broker.example.com');
    });
  });

  describe('connectHappyEyeballs', () => {
    it('connects the only address immediately', async () => {
      const connect = vi.fn().mockResolvedValue('ok');
      await expect(connectHappyEyeballs([{ address: '10.0.0.1', family: 4 }], connect)).resolves.toBe('ok');
      expect(connect).toHaveBeenCalledOnce();
    });

    it('returns the first success and does not wait for slower attempts', async () => {
      vi.useFakeTimers();
      const connect = vi.fn((address: { address: string }) => {
        if (address.address === '2001:db8::1') {
          return new Promise(() => undefined);
        }
        return Promise.resolve('v4');
      });

      const result = connectHappyEyeballs(
        [
          { address: '2001:db8::1', family: 6 },
          { address: '10.0.0.1', family: 4 },
        ],
        connect,
        250,
      );

      await vi.advanceTimersByTimeAsync(250);
      await expect(result).resolves.toBe('v4');
      vi.useRealTimers();
    });

    it('rejects when every address fails', async () => {
      const connect = vi.fn().mockRejectedValue(new Error('refused'));
      await expect(
        connectHappyEyeballs(
          [
            { address: '2001:db8::1', family: 6 },
            { address: '10.0.0.1', family: 4 },
          ],
          connect,
          0,
        ),
      ).rejects.toThrow('refused');
    });
  });
});
