/** Negative Kafka sentinels are deliberately left uninterpreted: their meaning depends on the key. */
export function formatConfigDuration(name: string, value: string): string | null {
  if (!name.endsWith('.ms') || !/^\d+$/.test(value)) return null;
  let remaining = BigInt(value);
  if (remaining === 0n) return '0 ms';
  const parts: string[] = [];
  for (const [unit, milliseconds] of [
    ['d', 86_400_000n],
    ['h', 3_600_000n],
    ['m', 60_000n],
    ['s', 1_000n],
    ['ms', 1n],
  ] as const) {
    const count = remaining / milliseconds;
    if (count > 0n) parts.push(`${String(count)} ${unit}`);
    remaining %= milliseconds;
  }
  return parts.join(' ');
}
