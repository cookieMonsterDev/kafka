import { useEffect, useMemo, useState } from 'react';
import { Command } from 'lucide-react';
import { useNavigate } from '@tanstack/react-router';
import { Button } from '../ui/button';
import { Dialog, DialogContent, DialogDescription, DialogTitle } from '../ui/dialog';
import { cn } from '../../lib/utils';
import { WORKSPACE_PAGES, type WorkspacePage } from './workspace-pages';

/**
 * A keyboard-first way to jump between pages — `⌘K`/`Ctrl+K`, or the trigger button for anyone
 * who never learns the shortcut. Distinct from the per-list search boxes on individual pages: this
 * never filters data, it only navigates.
 */
export function CommandPalette() {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState('');
  const [activeIndex, setActiveIndex] = useState(0);
  const navigate = useNavigate();

  const filtered = useMemo(() => {
    const needle = query.trim().toLowerCase();
    return needle === ''
      ? WORKSPACE_PAGES
      : WORKSPACE_PAGES.filter((page) => page.label.toLowerCase().includes(needle));
  }, [query]);

  // `query` and `activeIndex` reset together, driven directly by the event that opens the
  // dialog — not by a second effect reacting to the first one's state.
  function openPalette(): void {
    setQuery('');
    setActiveIndex(0);
    setOpen(true);
  }

  function handleOpenChange(next: boolean): void {
    if (next) {
      openPalette();
    } else {
      setOpen(false);
    }
  }

  useEffect(() => {
    function onKeyDown(event: KeyboardEvent): void {
      if (!(event.metaKey || event.ctrlKey) || event.key.toLowerCase() !== 'k') return;
      event.preventDefault();
      handleOpenChange(!open);
    }
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [open]);

  function go(item: WorkspacePage | undefined): void {
    if (item === undefined) return;
    setOpen(false);
    void navigate({ to: item.to });
  }

  return (
    <>
      <Button type="button" variant="outline" size="lg" onClick={openPalette} aria-label="Open command palette">
        <Command className="size-4" aria-hidden="true" />
        <span className="max-lg:sr-only">Jump to…</span>
        <kbd
          aria-hidden="true"
          className="ml-1 hidden rounded border border-border bg-muted px-1.5 py-0.5 text-[0.6875rem] text-muted-foreground lg:inline-block"
        >
          ⌘K
        </kbd>
      </Button>
      <Dialog open={open} onOpenChange={handleOpenChange}>
        <DialogContent showClose={false} className="top-[20%] max-w-lg translate-y-0 gap-0 p-0">
          <DialogTitle className="sr-only">Command palette</DialogTitle>
          <DialogDescription className="sr-only">Jump to any page in the studio.</DialogDescription>
          <div className="flex items-center gap-2 border-b border-border px-4 py-3">
            <Command className="size-4 shrink-0 text-muted-foreground" aria-hidden="true" />
            {/* Autofocus here is the WAI-ARIA dialog pattern, not an unexpected page-load autofocus:
                a modal the user just explicitly opened is supposed to move focus onto its primary control.
                (Reviewed against react-doctor's `no-autofocus` finding: false positive for that reason.) */}
            <input
              autoFocus
              aria-label="Jump to a page"
              value={query}
              onChange={(event) => {
                setQuery(event.target.value);
                setActiveIndex(0);
              }}
              onKeyDown={(event) => {
                if (event.key === 'ArrowDown') {
                  event.preventDefault();
                  setActiveIndex((index) => Math.min(index + 1, filtered.length - 1));
                } else if (event.key === 'ArrowUp') {
                  event.preventDefault();
                  setActiveIndex((index) => Math.max(index - 1, 0));
                } else if (event.key === 'Enter') {
                  event.preventDefault();
                  go(filtered[activeIndex]);
                }
              }}
              role="combobox"
              aria-expanded="true"
              aria-controls="command-palette-list"
              aria-activedescendant={filtered[activeIndex] ? `command-palette-item-${String(activeIndex)}` : undefined}
              placeholder="Jump to a page…"
              className="w-full bg-transparent text-sm outline-none placeholder:text-muted-foreground"
            />
          </div>
          {/* `role="listbox"` requires `option` children directly — no intervening `listitem`, so
              this is `div`s, not a `ul`/`li` list, even though it reads as one visually. */}
          <div id="command-palette-list" role="listbox" aria-label="Pages" className="max-h-80 overflow-y-auto p-2">
            {filtered.length === 0 && (
              <p className="px-3 py-6 text-center text-sm text-muted-foreground">No matching pages</p>
            )}
            {filtered.map((item, index) => {
              const Icon = item.icon;
              return (
                <button
                  key={item.to}
                  id={`command-palette-item-${String(index)}`}
                  type="button"
                  role="option"
                  aria-selected={index === activeIndex}
                  onMouseEnter={() => setActiveIndex(index)}
                  onClick={() => go(item)}
                  className={cn(
                    'flex w-full items-center gap-3 rounded-lg px-3 py-2 text-left text-sm outline-none',
                    index === activeIndex ? 'bg-accent text-foreground' : 'text-muted-foreground hover:bg-muted/50',
                  )}
                >
                  <Icon className="size-4 shrink-0" aria-hidden="true" />
                  {item.label}
                </button>
              );
            })}
          </div>
        </DialogContent>
      </Dialog>
    </>
  );
}
