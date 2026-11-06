import { useEffect, useRef, useState } from 'react';
import { Terminal } from '@xterm/xterm';
import { FitAddon } from '@xterm/addon-fit';
import '@xterm/xterm/css/xterm.css';
import { apiSend } from '../../lib/api';
import { withAuthQuery } from '../../lib/auth';
import type { TerminalMode } from './terminal-context';
import { ErrorState } from '../ui/error-state';

function themeColor(name: string): string {
  const canvas = document.createElement('canvas');
  canvas.width = canvas.height = 1;
  const context = canvas.getContext('2d');
  if (context === null) return '#ffffff';
  context.fillStyle = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  context.fillRect(0, 0, 1, 1);
  const pixel = context.getImageData(0, 0, 1, 1).data;
  return `rgb(${String(pixel[0])},${String(pixel[1])},${String(pixel[2])})`;
}

export default function TerminalSurface({
  onExit,
  mode,
}: {
  readonly onExit: () => void;
  readonly mode: TerminalMode;
}) {
  const currentTerminal = useRef<Terminal | null>(null);
  useEffect(() => {
    if (mode !== 'hidden') currentTerminal.current?.focus();
  }, [mode]);
  const host = useRef<HTMLDivElement>(null);
  const [error, setError] = useState<unknown>();
  const [attempt, setAttempt] = useState(0);
  useEffect(() => {
    const element = host.current;
    if (element === null) return;
    let disposed = false;
    let id: string | undefined;
    let stream: EventSource | undefined;
    let terminal: Terminal | undefined;
    let observer: ResizeObserver | undefined;
    let queue = Promise.resolve();
    const send = (suffix: string, body: unknown) => {
      queue = queue
        .then(async () => {
          if (!disposed && id !== undefined) await apiSend('POST', `/api/terminal/sessions/${id}/${suffix}`, body);
        })
        .catch((failure: unknown) => {
          if (!disposed) setError(failure);
        });
    };
    void (async () => {
      const session = await apiSend<{ id: string }>('POST', '/api/terminal/sessions');
      id = session.id;
      if (disposed) {
        await apiSend('DELETE', `/api/terminal/sessions/${id}`);
        return;
      }
      terminal = new Terminal({
        fontFamily: 'Menlo, Monaco, Consolas, "Liberation Mono", monospace',
        fontSize: 13,
        lineHeight: 1.25,
        cursorStyle: 'block',
        cursorBlink: !window.matchMedia('(prefers-reduced-motion: reduce)').matches,
        screenReaderMode: true,
        minimumContrastRatio: 4.5,
        scrollback: 5000,
        theme: {
          background: themeColor('--background'),
          foreground: themeColor('--foreground'),
          cursor: themeColor('--primary'),
          green: themeColor('--primary'),
          red: themeColor('--destructive'),
          selectionBackground: themeColor('--accent'),
        },
      });
      currentTerminal.current = terminal;
      const fit = new FitAddon();
      terminal.loadAddon(fit);
      terminal.open(element);
      terminal.textarea?.setAttribute('aria-label', 'Kafka terminal input');
      terminal.textarea?.setAttribute('aria-describedby', 'terminal-keyboard-help');
      terminal.attachCustomKeyEventHandler((event) => {
        // Browser focus navigation stays available; terminal Tab remains native completion.
        if (event.key === 'Tab' && event.shiftKey) return false;
        return true;
      });
      observer = new ResizeObserver(() => {
        if (element.clientWidth === 0 || element.clientHeight === 0 || terminal === undefined) return;
        fit.fit();
        send('resize', { columns: Math.max(20, Math.min(500, terminal.cols)) });
      });
      observer.observe(element);
      terminal.onData((data) => {
        for (let offset = 0; offset < data.length; offset += 8192)
          send('input', { data: data.slice(offset, offset + 8192) });
      });
      stream = new EventSource(withAuthQuery(`/api/terminal/sessions/${id}/events`));
      stream.addEventListener('output', (event: MessageEvent<string>) => {
        const message = JSON.parse(event.data) as { data: string };
        terminal?.write(message.data);
      });
      stream.addEventListener('exit', onExit);
      stream.onerror = () => {
        if (stream?.readyState === EventSource.CLOSED && !disposed)
          setError(new Error('Terminal session ended. Reopen the terminal.'));
      };
      terminal.focus();
    })().catch((failure: unknown) => {
      if (!disposed) setError(failure);
    });
    return () => {
      disposed = true;
      observer?.disconnect();
      stream?.close();
      terminal?.dispose();
      currentTerminal.current = null;
      if (id !== undefined) void apiSend('DELETE', `/api/terminal/sessions/${id}`).catch(() => {});
    };
  }, [attempt, onExit]);
  return (
    <>
      {error !== undefined && (
        <ErrorState
          className="min-h-0 flex-1 justify-center"
          title="Terminal unavailable"
          error={error}
          onRetry={() => {
            setError(undefined);
            setAttempt((value) => value + 1);
          }}
        />
      )}
      <div
        hidden={error !== undefined}
        className="min-h-0 flex-1 overflow-hidden p-3 outline-none focus-within:ring-1 focus-within:ring-inset focus-within:ring-ring"
      >
        <div ref={host} className="h-full w-full" />
      </div>
      <p id="terminal-keyboard-help" className="sr-only">
        Kafka command shell. Enter runs, Tab completes, arrows edit or recall history, Control+C interrupts, Control+L
        clears the screen. Shift+Tab leaves the terminal. Type exit to end the session.
      </p>
    </>
  );
}
