import { clsx, type ClassValue } from 'clsx';
import { twMerge } from 'tailwind-merge';

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

const BYTE_UNITS = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];

/** A decimal-string byte count (as the server serializes `bigint` size/offset fields) to a human-readable size, or `'—'` when there's nothing to show. */
export function formatBytes(value: string | null): string {
  if (value === null) return '—';
  let bytes: bigint;
  try {
    bytes = BigInt(value);
  } catch {
    return '—';
  }
  if (bytes < 0n) return '—';
  if (bytes === 0n) return '0 B';

  let unitIndex = 0;
  let amount = Number(bytes);
  while (amount >= 1024 && unitIndex < BYTE_UNITS.length - 1) {
    amount /= 1024;
    unitIndex += 1;
  }
  const precision = unitIndex === 0 ? 0 : 1;
  return `${amount.toFixed(precision)} ${BYTE_UNITS[unitIndex]}`;
}
