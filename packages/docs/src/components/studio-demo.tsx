import { useState } from 'react';
import { ExternalLink, Monitor, RotateCcw, Smartphone, Tablet } from 'lucide-react';
import { Button, buttonVariants } from '@/components/ui/button';
import { cn } from '@/lib/utils';

const VIEWPORTS = [
  { id: 'desktop', label: 'Desktop width', icon: Monitor, maxWidth: '100%' },
  { id: 'tablet', label: 'Tablet width', icon: Tablet, maxWidth: '768px' },
  { id: 'mobile', label: 'Mobile width', icon: Smartphone, maxWidth: '390px' },
] as const;

type ViewportId = (typeof VIEWPORTS)[number]['id'];

interface StudioDemoProps {
  /** The demo page, already prefixed with the site base. */
  readonly src: string;
}

/**
 * The Studio web UI running against an in-browser sample cluster, framed like a component preview:
 * a toolbar to switch the frame width, reset the demo, or open it full size. Without JavaScript only
 * the frame and the "open" link render, and both still work.
 */
export function StudioDemo({ src }: StudioDemoProps) {
  const [viewport, setViewport] = useState<ViewportId>('desktop');
  // Remounting the frame restarts the demo with fresh sample data.
  const [session, setSession] = useState(0);
  const maxWidth = VIEWPORTS.find((entry) => entry.id === viewport)?.maxWidth ?? '100%';

  return (
    <figure className="not-prose border-border bg-card my-8 overflow-hidden rounded-xl border">
      <div className="border-border flex flex-wrap items-center gap-2 border-b px-3 py-2">
        <figcaption className="mr-auto min-w-0 text-sm">
          <span className="font-medium">Try Studio</span>
          <span className="text-muted-foreground"> · sample cluster, runs in your browser</span>
        </figcaption>
        <div role="group" aria-label="Preview width" className="flex items-center gap-0.5" data-requires-js>
          {VIEWPORTS.map(({ id, label, icon: Icon }) => (
            <Button
              key={id}
              type="button"
              variant="ghost"
              size="icon-sm"
              aria-label={label}
              aria-pressed={viewport === id}
              className="aria-pressed:bg-muted aria-pressed:text-foreground text-muted-foreground"
              onClick={() => setViewport(id)}
            >
              <Icon aria-hidden="true" />
            </Button>
          ))}
        </div>
        <Button
          type="button"
          variant="ghost"
          size="icon-sm"
          aria-label="Reset demo"
          className="text-muted-foreground"
          data-requires-js
          onClick={() => setSession((value) => value + 1)}
        >
          <RotateCcw aria-hidden="true" />
        </Button>
        <a href={src} target="_blank" rel="noopener" className={buttonVariants({ variant: 'outline', size: 'sm' })}>
          Open full screen
          <ExternalLink aria-hidden="true" />
          <span className="sr-only">(opens in a new tab)</span>
        </a>
      </div>
      <div className="bg-muted/30 flex justify-center">
        <iframe
          key={session}
          src={src}
          title="Kafka Studio interactive demo"
          loading="lazy"
          className={cn(
            'border-border block h-[min(42rem,80dvh)] w-full transition-[max-width] duration-300 ease-out motion-reduce:transition-none',
            viewport !== 'desktop' && 'border-x',
          )}
          style={{ maxWidth }}
        />
      </div>
      <p className="border-border text-muted-foreground border-t px-3 py-2 text-xs">
        Nothing here connects to Kafka: the topics, groups and traffic are generated in this page. Some Debugger tools
        and most terminal commands need an installed Studio.
      </p>
    </figure>
  );
}
