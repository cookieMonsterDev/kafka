import { useEffect, useId, useMemo, useRef, useState } from 'react';
import { Search } from 'lucide-react';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { filterSearch, type SearchDoc } from '@/lib/search';

type Props = { indexUrl: string; currentPackage: string };

const MAX_VISIBLE_RESULTS = 60;

let indexPromise: Promise<SearchDoc[]> | undefined;

async function loadIndex(url: string): Promise<SearchDoc[]> {
  indexPromise ??= fetch(url)
    .then(async (response) => {
      if (!response.ok) throw new Error('Search index unavailable');
      return (await response.json()) as SearchDoc[];
    })
    .catch((error: unknown) => {
      indexPromise = undefined;
      throw error;
    });
  return indexPromise;
}

function searchStatusText(hasLoadFailed: boolean, index: SearchDoc[] | null, resultCount: number): string {
  if (hasLoadFailed) return 'Search could not load. Check your connection and retry.';
  if (index == null) return 'Loading search…';
  const summary = `${resultCount} ${resultCount === 1 ? 'result' : 'results'}`;
  if (resultCount <= MAX_VISIBLE_RESULTS) return summary;
  return `${summary}, showing the first ${MAX_VISIBLE_RESULTS}; refine your search`;
}

export function DocsSearch({ indexUrl, currentPackage }: Props) {
  const id = useId();
  const dialog = useRef<HTMLDialogElement>(null);
  const trigger = useRef<HTMLButtonElement>(null);
  const input = useRef<HTMLInputElement>(null);
  const returnFocus = useRef<HTMLElement | null>(null);
  const [modKey, setModKey] = useState('Ctrl');
  const [open, setOpen] = useState(false);
  const [index, setIndex] = useState<SearchDoc[] | null>(null);
  const [hasLoadFailed, setHasLoadFailed] = useState(false);
  const [attempt, setAttempt] = useState(0);
  const [query, setQuery] = useState('');
  const [scope, setScope] = useState(currentPackage);
  const [active, setActive] = useState(0);
  const results = useMemo(
    () =>
      filterSearch(
        (index ?? []).filter((doc) => scope === 'all' || doc.package === scope),
        query,
      ),
    [index, query, scope],
  );
  const hits = results.slice(0, MAX_VISIBLE_RESULTS);

  useEffect(() => {
    setModKey(/Mac|iPhone|iPad/.test(navigator.userAgent) ? '⌘' : 'Ctrl');
  }, []);

  useEffect(() => {
    if (!open || index != null) return;
    let cancelled = false;
    setHasLoadFailed(false);
    void loadIndex(indexUrl).then(
      (docs) => {
        if (!cancelled) setIndex(docs);
      },
      () => {
        if (!cancelled) setHasLoadFailed(true);
      },
    );
    return () => {
      cancelled = true;
    };
  }, [open, indexUrl, index, attempt]);
  useEffect(() => {
    if (open && !dialog.current?.open) {
      dialog.current?.showModal();
      input.current?.focus();
    }
    if (!open && dialog.current?.open) dialog.current.close();
  }, [open]);
  useEffect(() => {
    function keydown(event: KeyboardEvent) {
      if (event.key.toLowerCase() === 'k' && (event.metaKey || event.ctrlKey)) {
        event.preventDefault();
        if (!dialog.current?.open && document.activeElement instanceof HTMLElement)
          returnFocus.current = document.activeElement;
        setOpen((value) => !value);
      }
    }
    function openFromMenu(event: Event) {
      const opener = (event as CustomEvent<HTMLElement>).detail;
      returnFocus.current = opener ?? trigger.current;
      setOpen(true);
    }
    function closeRestoredDialog(event: PageTransitionEvent) {
      if (event.persisted) setOpen(false);
    }
    window.addEventListener('docs-search-open', openFromMenu);
    window.addEventListener('pageshow', closeRestoredDialog);
    document.addEventListener('keydown', keydown);
    return () => {
      document.removeEventListener('keydown', keydown);
      window.removeEventListener('docs-search-open', openFromMenu);
      window.removeEventListener('pageshow', closeRestoredDialog);
    };
  }, []);
  useEffect(() => {
    document.getElementById(`${id}-result-${active}`)?.scrollIntoView({ block: 'nearest' });
  }, [active, id]);
  return (
    <>
      <button
        ref={trigger}
        type="button"
        onClick={() => {
          returnFocus.current = trigger.current;
          setOpen(true);
        }}
        aria-label="Search documentation"
        aria-haspopup="dialog"
        aria-expanded={open}
        aria-controls={`${id}-dialog`}
        aria-keyshortcuts="Control+K Meta+K"
        className="border-border bg-muted/60 text-muted-foreground hover:bg-muted hover:text-foreground flex h-11 w-full min-w-0 items-center gap-2.5 rounded-lg border px-3.5 text-sm transition-colors lg:w-72"
      >
        <Search className="size-5 shrink-0" aria-hidden="true" />
        <span className="min-w-0 flex-1 truncate text-left">Search documentation…</span>
        <kbd
          className="border-border bg-background pointer-events-none hidden rounded-md border px-2 py-0.5 font-sans text-xs font-medium lg:inline"
          aria-hidden="true"
        >
          {modKey} K
        </kbd>
      </button>
      <dialog
        ref={dialog}
        id={`${id}-dialog`}
        className="docs-search-dialog"
        aria-labelledby={`${id}-title`}
        onCancel={() => setOpen(false)}
        onClick={(event) => {
          // Clicks on the backdrop target the dialog itself; its children cover the rest of the box.
          if (event.target === event.currentTarget) setOpen(false);
        }}
        onClose={() => {
          setOpen(false);
          (returnFocus.current ?? trigger.current)?.focus();
        }}
      >
        <div className="flex items-center justify-between border-b border-border px-4 py-2">
          <h2 id={`${id}-title`} className="font-semibold">
            Search documentation
          </h2>
          <button type="button" className="min-h-11 rounded-md px-2" onClick={() => setOpen(false)}>
            Close search
          </button>
        </div>
        <div className="flex flex-wrap gap-3 border-b border-border p-4">
          <label className="min-w-0 flex-1 text-xs">
            Search terms
            <input
              ref={input}
              type="search"
              value={query}
              placeholder="Command, flag, error or example"
              className="mt-1 min-h-11 w-full rounded-md border border-border bg-background px-2 text-sm"
              onChange={(event) => {
                setQuery(event.target.value);
                setActive(0);
              }}
              role="combobox"
              aria-autocomplete="list"
              aria-expanded={index != null && hits.length > 0}
              aria-controls={`${id}-results`}
              aria-activedescendant={hits[active] ? `${id}-result-${active}` : undefined}
              onKeyDown={(event) => {
                if (event.key === 'Escape') {
                  event.preventDefault();
                  setOpen(false);
                  return;
                }
                if (hits.length === 0) return;
                if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
                  event.preventDefault();
                  setActive((value) => (value + (event.key === 'ArrowDown' ? 1 : hits.length - 1)) % hits.length);
                } else if (event.key === 'Enter' && hits[active]) {
                  event.preventDefault();
                  window.location.assign(hits[active].href);
                }
              }}
            />
          </label>
          <div className="text-xs">
            <label htmlFor={`${id}-scope`}>Scope</label>
            <Select
              value={scope}
              onValueChange={(value) => {
                setScope(value);
                setActive(0);
              }}
            >
              <SelectTrigger id={`${id}-scope`} className="mt-1 min-h-11">
                <SelectValue />
              </SelectTrigger>
              <SelectContent portalContainer={dialog.current} position="popper" align="end">
                <SelectItem value={currentPackage}>This package ({currentPackage})</SelectItem>
                <SelectItem value="all">All packages</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </div>
        <p role="status" className="px-4 py-2 text-sm text-muted-foreground">
          {searchStatusText(hasLoadFailed, index, results.length)}
        </p>
        {hasLoadFailed && (
          <button
            type="button"
            className="m-4 min-h-11 rounded-md border border-border px-3"
            onClick={() => setAttempt((value) => value + 1)}
          >
            Retry search
          </button>
        )}
        <ul id={`${id}-results`} role="listbox" aria-label="Search results" className="max-h-80 overflow-y-auto p-2">
          {hits.map((hit, position) => (
            <li key={hit.href} role="presentation">
              <a
                id={`${id}-result-${position}`}
                href={hit.href}
                role="option"
                aria-selected={position === active}
                onMouseEnter={() => setActive(position)}
                className={`block rounded-md p-3 ${position === active ? 'bg-accent text-accent-foreground' : 'hover:bg-muted'}`}
              >
                <span className="block text-xs font-medium uppercase">
                  {hit.package} · {hit.sectionLabel}
                </span>
                <span className="block text-sm font-semibold">{hit.title}</span>
                <span className="block text-xs">{hit.heading ?? hit.description}</span>
              </a>
            </li>
          ))}
        </ul>
      </dialog>
    </>
  );
}
