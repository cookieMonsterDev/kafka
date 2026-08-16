import { KafkaNonRetriableError } from '../errors';

export interface BrokerAddress {
  host: string;
  port: number;
}

function parsePort(portPart: string, broker: string): number {
  if (!/^\d+$/.test(portPart)) {
    throw new KafkaNonRetriableError(`Failed to connect: broker "${broker}" has an invalid port "${portPart}"`);
  }

  const port = Number(portPart);
  if (!Number.isInteger(port) || port < 0 || port >= 65536) {
    throw new KafkaNonRetriableError(`Failed to connect: broker "${broker}" has an invalid port "${portPart}"`);
  }

  return port;
}

/**
 * Parse a `host:port` bootstrap string, including bracketed IPv6 (`[::1]:9092`).
 */
export function parseBrokerAddress(broker: string): BrokerAddress {
  if (broker.startsWith('[')) {
    const end = broker.indexOf(']');
    if (end === -1) {
      throw new KafkaNonRetriableError(`Failed to connect: invalid IPv6 broker address "${broker}"`);
    }

    const host = broker.slice(1, end);
    const rest = broker.slice(end + 1);
    if (!host || !rest.startsWith(':') || rest.length === 1) {
      throw new KafkaNonRetriableError(`Failed to connect: broker "${broker}" is missing a port`);
    }

    return { host, port: parsePort(rest.slice(1), broker) };
  }

  const lastColon = broker.lastIndexOf(':');
  if (lastColon <= 0 || lastColon === broker.length - 1) {
    throw new KafkaNonRetriableError(`Failed to connect: broker "${broker}" is missing a port`);
  }

  const host = broker.slice(0, lastColon);
  if (host.includes(':')) {
    throw new KafkaNonRetriableError(`Failed to connect: IPv6 broker "${broker}" must be written as [host]:port`);
  }

  return { host, port: parsePort(broker.slice(lastColon + 1), broker) };
}
