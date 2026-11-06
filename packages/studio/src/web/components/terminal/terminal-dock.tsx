import { lazy, Suspense, useRef, useState } from 'react';
import { Maximize2, Minimize2, Minus, Terminal, X } from 'lucide-react';
import { useQuery } from '@tanstack/react-query';
import { Button } from '../ui/button';
import { ErrorState } from '../ui/error-state';
import { fetchProfiles, profilesQueryKey } from '../../lib/profiles-api';
import { useTerminal } from './terminal-context';

const TerminalSurface = lazy(() => import('./terminal-surface'));
export function TerminalDock() {
  const { mode, mounted, open, minimize, terminate } = useTerminal();
  const [height, setHeight] = useState(42);
  const drag = useRef<{ y: number; height: number; available: number } | null>(null);
  const profiles = useQuery({ queryKey: profilesQueryKey, queryFn: fetchProfiles, enabled: mounted });
  if (!mounted) return null;
  const expanded = mode === 'expanded';
  return (
    <section
      id="terminal-panel"
      aria-label="Kafka terminal"
      hidden={mode === 'hidden'}
      className="min-h-0 shrink-0 border-t border-border bg-background"
      style={{
        height: expanded ? '100%' : `${String(height)}%`,
        display: mode === 'hidden' ? 'none' : 'flex',
        flexDirection: 'column',
      }}
    >
      {!expanded && (
        <div
          role="separator"
          tabIndex={0}
          aria-label="Resize terminal"
          aria-orientation="horizontal"
          aria-valuemin={20}
          aria-valuemax={85}
          aria-valuenow={Math.round(height)}
          aria-controls="terminal-panel"
          className="h-2 shrink-0 cursor-row-resize touch-none bg-muted/30 outline-none hover:bg-primary/30 focus-visible:bg-primary/40 focus-visible:ring-2 focus-visible:ring-ring"
          onKeyDown={(event) => {
            if (event.key === 'ArrowUp' || event.key === 'ArrowDown') {
              event.preventDefault();
              setHeight((value) => Math.max(20, Math.min(85, value + (event.key === 'ArrowUp' ? 5 : -5))));
            }
            if (event.key === 'Home') {
              event.preventDefault();
              setHeight(20);
            }
            if (event.key === 'End') {
              event.preventDefault();
              setHeight(85);
            }
          }}
          onPointerDown={(event) => {
            event.currentTarget.setPointerCapture(event.pointerId);
            drag.current = {
              y: event.clientY,
              height,
              available: event.currentTarget.parentElement?.parentElement?.clientHeight ?? 1,
            };
          }}
          onPointerMove={(event) => {
            const start = drag.current;
            if (start !== null)
              setHeight(Math.max(20, Math.min(85, start.height + ((start.y - event.clientY) / start.available) * 100)));
          }}
          onPointerUp={() => {
            drag.current = null;
          }}
          onLostPointerCapture={() => {
            drag.current = null;
          }}
        />
      )}
      <div className="flex h-10 shrink-0 items-center justify-between gap-3 border-b border-border bg-muted/20 px-3">
        <div className="flex min-w-0 items-center gap-2 text-xs">
          <Terminal className="size-3.5 shrink-0 text-primary" aria-hidden="true" />
          <h2 className="font-medium">Terminal</h2>
          <span className="truncate text-muted-foreground">{profiles.data?.active ?? 'Direct connection'}</span>
        </div>
        <div className="flex gap-1">
          <Button variant="ghost" size="icon-sm" aria-label="Minimize terminal" title="Minimize" onClick={minimize}>
            <Minus aria-hidden="true" />
          </Button>
          <Button
            variant="ghost"
            size="icon-sm"
            aria-label={expanded ? 'Restore terminal panel' : 'Expand terminal'}
            title={expanded ? 'Restore' : 'Expand'}
            onClick={() => open(expanded ? 'docked' : 'expanded')}
          >
            {expanded ? <Minimize2 aria-hidden="true" /> : <Maximize2 aria-hidden="true" />}
          </Button>
          <Button
            variant="ghost"
            size="icon-sm"
            aria-label="End terminal session"
            title="End session"
            onClick={terminate}
          >
            <X aria-hidden="true" />
          </Button>
        </div>
      </div>
      {profiles.isError ? (
        <ErrorState
          className="min-h-0 flex-1 justify-center"
          title="Could not load terminal connection"
          error={profiles.error}
          onRetry={() => void profiles.refetch()}
        />
      ) : (
        profiles.data !== undefined && (
          <Suspense
            fallback={
              <p role="status" className="p-3 font-mono text-xs text-muted-foreground">
                Opening terminal…
              </p>
            }
          >
            <TerminalSurface key={profiles.data.active ?? ''} onExit={terminate} mode={mode} />
          </Suspense>
        )
      )}
    </section>
  );
}
