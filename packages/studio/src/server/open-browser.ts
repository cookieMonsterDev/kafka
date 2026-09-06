import { spawn } from 'node:child_process';

export interface OpenBrowserOptions {
  readonly env: Readonly<Record<string, string | undefined>>;
  readonly platform: NodeJS.Platform;
  /** Injectable for tests; defaults to `node:child_process`'s real `spawn`. */
  readonly spawn?: typeof spawn;
}

function spawnAndForget(command: string, args: readonly string[], spawnFn: typeof spawn): Promise<void> {
  return new Promise((resolve, reject) => {
    const child = spawnFn(command, args as string[], { stdio: 'ignore', detached: true });
    child.once('error', reject);
    // A successful spawn is enough — we don't wait for the browser process itself to exit.
    child.once('spawn', () => {
      child.unref();
      resolve();
    });
  });
}

function commandFor(
  url: string,
  override: string | undefined,
  platform: NodeJS.Platform,
): { command: string; args: string[] } {
  if (override) return { command: override, args: [url] };
  if (platform === 'darwin') return { command: 'open', args: [url] };
  if (platform === 'win32') return { command: 'cmd', args: ['/c', 'start', '""', url] };
  return { command: 'xdg-open', args: [url] };
}

/**
 * Opens `url` in a browser, honouring `BROWSER=none` (or an explicit `override` of `"none"`) as
 * the sentinel to skip opening entirely. Never throws or rejects — a failure here (no display, no
 * `xdg-open` installed, whatever) is never a reason to fail the whole command, matching Prisma
 * Studio's own never-fatal rule. Returns whether it actually attempted to open something.
 */
export async function openBrowser(
  url: string,
  override: string | undefined,
  options: OpenBrowserOptions,
): Promise<boolean> {
  const choice = override ?? options.env.BROWSER;
  if (choice === 'none') return false;

  const { command, args } = commandFor(url, choice, options.platform);
  try {
    await spawnAndForget(command, args, options.spawn ?? spawn);
    return true;
  } catch {
    return false;
  }
}

export interface BannerOptions {
  readonly url: string;
  readonly readOnly: boolean;
}

/** The startup banner — always prints the address actually bound, never a hardcoded `localhost`. */
export function formatBanner(options: BannerOptions): string {
  const lines = [`kafka-studio listening on ${options.url}`];
  if (options.readOnly) lines.push('read-only mode: mutating requests will be rejected');
  return lines.join('\n');
}
