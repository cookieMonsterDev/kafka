/** Sends one named frame, as `event: <name>\ndata: <json>` would on a real stream. */
export type StreamEmit = (event: string, data: unknown) => void;

/**
 * Starts the stream for `url`. Returns its cleanup, or `null` when no stream exists at that path.
 * `end` is the server closing the stream.
 */
export type StreamOpener = (url: URL, emit: StreamEmit, end: () => void) => (() => void) | null;

/**
 * An in-page stand-in for `EventSource`, covering the surface the studio uses: named events through
 * `addEventListener`, `onerror`, `readyState` and `close()`. A stream the opener ends, or one that
 * doesn't exist, closes for good and fires `error` — the demo never reconnects.
 */
export function createFakeEventSource(open: StreamOpener, baseUrl: string): typeof EventSource {
  class FakeEventSource extends EventTarget {
    static readonly CONNECTING = 0;
    static readonly OPEN = 1;
    static readonly CLOSED = 2;
    readonly CONNECTING = 0;
    readonly OPEN = 1;
    readonly CLOSED = 2;

    readonly url: string;
    readonly withCredentials = false;
    readyState: number = FakeEventSource.CONNECTING;
    onopen: ((event: Event) => void) | null = null;
    onmessage: ((event: MessageEvent) => void) | null = null;
    onerror: ((event: Event) => void) | null = null;
    private cleanup: (() => void) | null = null;

    constructor(url: string | URL) {
      super();
      const resolved = new URL(url, baseUrl);
      this.url = resolved.toString();
      // Real streams connect asynchronously; listeners added right after construction must see `open`.
      setTimeout(() => {
        if (this.readyState === FakeEventSource.CLOSED) return;
        // Frames sent while the stream starts (a banner, an initial snapshot, an immediate error)
        // are held until `open` has fired, the order a real stream delivers them in.
        let connecting = true;
        let endedWhileConnecting = false;
        const pending: (readonly [string, unknown])[] = [];
        const cleanup = open(
          resolved,
          (event, data) => {
            if (connecting) pending.push([event, data]);
            else this.emit(event, data);
          },
          () => {
            if (connecting) endedWhileConnecting = true;
            else this.end();
          },
        );
        connecting = false;
        if (cleanup === null) {
          this.end();
          return;
        }
        this.cleanup = cleanup;
        this.readyState = FakeEventSource.OPEN;
        const opened = new Event('open');
        this.onopen?.(opened);
        this.dispatchEvent(opened);
        for (const [event, data] of pending) this.emit(event, data);
        if (endedWhileConnecting) this.end();
      }, 0);
    }

    close(): void {
      if (this.readyState === FakeEventSource.CLOSED) return;
      this.readyState = FakeEventSource.CLOSED;
      this.cleanup?.();
      this.cleanup = null;
    }

    private emit(event: string, data: unknown): void {
      if (this.readyState !== FakeEventSource.OPEN) return;
      const message = new MessageEvent(event, { data: JSON.stringify(data) });
      if (event === 'message') this.onmessage?.(message);
      this.dispatchEvent(message);
    }

    private end(): void {
      if (this.readyState === FakeEventSource.CLOSED) return;
      this.close();
      const failed = new Event('error');
      this.onerror?.(failed);
      this.dispatchEvent(failed);
    }
  }

  // Structurally the subset of `EventSource` the studio calls; the cast only drops members it never touches.
  return FakeEventSource as unknown as typeof EventSource;
}
