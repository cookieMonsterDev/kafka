import type { LogDirView } from '../../shared/contracts/cluster';

/** The share of a log dir's volume in use, as a whole percentage, or `null` when the broker reported no sizes. */
export function logDirUsedPercent(logDir: Pick<LogDirView, 'totalBytes' | 'usedBytes'>): number | null {
  if (logDir.totalBytes === null || logDir.usedBytes === null) return null;
  const total = Number(logDir.totalBytes);
  if (total <= 0) return null;
  const used = Number(logDir.usedBytes);
  return Math.min(100, Math.max(0, Math.round((used / total) * 100)));
}
