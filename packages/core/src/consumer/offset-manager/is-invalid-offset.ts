export function isInvalidOffset(offset: unknown): boolean {
  if (offset === 0 || offset === 0n || offset === '0') return false;
  if (offset == null || offset === '') return true;

  try {
    return BigInt(offset as string | number | bigint) < 0n;
  } catch {
    return true;
  }
}
