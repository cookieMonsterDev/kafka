import dns from 'node:dns/promises';
import net from 'node:net';

/**
 * How bootstrap hostnames are resolved before a TCP/TLS connect.
 *
 * - `useAllDnsIps` (default): resolve every A/AAAA and try them (happy-eyeballs when both
 *   families are present). Matches broker `client.dns.lookup=use_all_dns_ips`.
 * - `canonicalBootstrap`: follow CNAME (and PTR if needed) so GSSAPI sees the FQDN
 *   (`sasl.kerberos.service.name` / SPN). Matches `resolve_canonical_bootstrap_servers_only`.
 *
 * @see https://kafka.apache.org/43/configuration/producer-configs/#client.dns.lookup
 */
export type ClientDnsLookup = 'useAllDnsIps' | 'canonicalBootstrap';

export interface ResolvedBrokerAddress {
  address: string;
  family: 4 | 6;
}

export interface ResolvedBrokerHost {
  /** Hostname used for TLS SNI and GSSAPI; the original name, or the canonical FQDN. */
  hostname: string;
  addresses: ResolvedBrokerAddress[];
}

/** RFC 8305 connection-attempt delay between addresses. */
export const HAPPY_EYEBALLS_DELAY_MS = 250;

export async function canonicalHostname(host: string): Promise<string> {
  if (net.isIP(host)) return host;

  try {
    const cnames = await dns.resolveCname(host);
    const last = cnames[cnames.length - 1];
    if (last) return last;
  } catch {
    // ENODATA / ENOTFOUND: not a CNAME — fall through to PTR, then the original name.
  }

  try {
    const { address } = await dns.lookup(host);
    const names = await dns.reverse(address);
    const ptr = names[0];
    if (ptr) return ptr;
  } catch {
    // No PTR; keep the configured hostname.
  }

  return host;
}

/**
 * Resolves `host` to connectable addresses. Bare IPs are returned as-is (no DNS).
 */
export async function resolveBrokerAddresses(
  host: string,
  lookup: ClientDnsLookup = 'useAllDnsIps',
): Promise<ResolvedBrokerHost> {
  if (net.isIP(host)) {
    const family: 4 | 6 = net.isIPv6(host) ? 6 : 4;
    return { hostname: host, addresses: [{ address: host, family }] };
  }

  const hostname = lookup === 'canonicalBootstrap' ? await canonicalHostname(host) : host;
  const records = await dns.lookup(hostname, { all: true, verbatim: true });
  const addresses: ResolvedBrokerAddress[] = records.map((record) => ({
    address: record.address,
    family: record.family === 6 ? 6 : 4,
  }));

  return { hostname, addresses };
}

/**
 * Interleave IPv6 then IPv4 so dual-stack hosts try both families (RFC 8305).
 */
export function sortHappyEyeballs(addresses: readonly ResolvedBrokerAddress[]): ResolvedBrokerAddress[] {
  const v6 = addresses.filter((entry) => entry.family === 6);
  const v4 = addresses.filter((entry) => entry.family === 4);
  const out: ResolvedBrokerAddress[] = [];
  const max = Math.max(v6.length, v4.length);
  for (let i = 0; i < max; i++) {
    const a = v6[i];
    const b = v4[i];
    if (a) out.push(a);
    if (b) out.push(b);
  }
  return out;
}

/**
 * Start connecting to each address, staggered by `delayMs`. The first success wins;
 * remaining attempts are ignored (callers must destroy unused sockets).
 */
export async function connectHappyEyeballs<T>(
  addresses: readonly ResolvedBrokerAddress[],
  connect: (address: ResolvedBrokerAddress) => Promise<T>,
  delayMs = HAPPY_EYEBALLS_DELAY_MS,
): Promise<T> {
  if (addresses.length === 0) {
    return Promise.reject(new Error('No addresses to connect to'));
  }

  const ordered = sortHappyEyeballs(addresses);
  if (ordered.length === 1) {
    return connect(ordered[0]!);
  }

  return new Promise((resolve, reject) => {
    let settled = false;
    let pending = 0;
    let started = 0;
    let lastError: unknown;
    const timers: NodeJS.Timeout[] = [];

    const finish = (error: unknown): void => {
      if (settled) return;
      if (pending !== 0 || started !== ordered.length) return;
      settled = true;
      for (const timer of timers) clearTimeout(timer);
      reject(error instanceof Error ? error : new Error(String(error)));
    };

    const start = (index: number): void => {
      const address = ordered[index];
      if (!address || settled) return;
      started++;
      pending++;
      void connect(address).then(
        (result) => {
          if (settled) return;
          settled = true;
          for (const timer of timers) clearTimeout(timer);
          resolve(result);
        },
        (error: unknown) => {
          pending--;
          lastError = error;
          finish(lastError);
        },
      );
    };

    start(0);
    for (let i = 1; i < ordered.length; i++) {
      timers.push(setTimeout(() => start(i), delayMs * i));
    }
  });
}
