import {
  useEffect,
  useId,
  useMemo,
  useRef,
  useState,
  type KeyboardEvent as ReactKeyboardEvent,
  type MouseEvent as ReactMouseEvent,
} from 'react';
import { buttonVariants } from '@/components/ui/button';
import { SECTION_LABELS, SECTION_ORDER } from '@/lib/docs';
import { filterSearch, type SearchDoc, type SearchHit } from '@/lib/search';
import { cn } from '@/lib/utils';

type DocsSearchProps = {
  index: SearchDoc[];
};

export function DocsSearch({ index }: DocsSearchProps) {
  const reactId = useId();
  const dialogRef = useRef<HTMLDialogElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState('');
  const [activeIndex, setActiveIndex] = useState(0);
  const [modKey, setModKey] = useState('⌘');

  const groups = useMemo(() => {
    const filtered = filterSearch(index, query);
    if (query.trim().length > 0) {
      return [
        {
          section: 'results' as const,
          label: undefined,
          entries: filtered.map((hit, hitIndex) => ({ ...hit, index: hitIndex })),
        },
      ];
    }
    return groupHits(filtered);
  }, [index, query]);
  const hits = useMemo(() => groups.flatMap((group) => group.entries), [groups]);
  const activeHit = hits[activeIndex];
  const resultsId = `${reactId}-results`;
  const activeId = activeHit == null ? undefined : `${reactId}-opt-${activeHit.index}`;

  useEffect(() => {
    setModKey(/Mac|iPhone|iPad/.test(navigator.userAgent) ? '⌘' : 'Ctrl');
  }, []);

  useEffect(() => {
    setActiveIndex(0);
  }, [query]);

  useEffect(() => {
    const dialog = dialogRef.current;
    if (dialog == null) {
      return;
    }
    function onClose() {
      setOpen(false);
    }
    dialog.addEventListener('close', onClose);
    return () => dialog.removeEventListener('close', onClose);
  }, []);

  useEffect(() => {
    const dialog = dialogRef.current;
    if (dialog == null) {
      return;
    }
    if (open && !dialog.open) {
      dialog.showModal();
      inputRef.current?.focus();
    } else if (!open && dialog.open) {
      dialog.close();
    }
  }, [open]);

  useEffect(() => {
    const option = document.getElementById(`${reactId}-opt-${activeIndex}`);
    option?.scrollIntoView({ block: 'nearest' });
  }, [activeIndex, query, reactId]);

  useEffect(() => {
    function onKeyDown(event: KeyboardEvent) {
      if (event.key.toLowerCase() !== 'k' || !(event.metaKey || event.ctrlKey)) {
        return;
      }
      event.preventDefault();
      setOpen((current) => !current);
    }
    document.addEventListener('keydown', onKeyDown);
    return () => document.removeEventListener('keydown', onKeyDown);
  }, []);

  function onDialogKeyDown(event: ReactKeyboardEvent<HTMLDialogElement>) {
    if (hits.length === 0) {
      return;
    }
    if (event.key === 'ArrowDown') {
      event.preventDefault();
      setActiveIndex((current) => (current + 1) % hits.length);
    } else if (event.key === 'ArrowUp') {
      event.preventDefault();
      setActiveIndex((current) => (current - 1 + hits.length) % hits.length);
    } else if (event.key === 'Home') {
      event.preventDefault();
      setActiveIndex(0);
    } else if (event.key === 'End') {
      event.preventDefault();
      setActiveIndex(hits.length - 1);
    } else if (event.key === 'Enter' && activeId != null) {
      event.preventDefault();
      const option = document.getElementById(activeId);
      if (option instanceof HTMLAnchorElement) {
        option.click();
      }
    }
  }

  function onBackdropClick(event: ReactMouseEvent<HTMLDialogElement>) {
    if (event.target === event.currentTarget) {
      setOpen(false);
    }
  }

  return (
    <>
      <button
        type="button"
        className={cn(buttonVariants({ variant: 'ghost', size: 'icon' }), 'text-muted-foreground md:hidden')}
        aria-label="Search documentation"
        aria-haspopup="dialog"
        aria-expanded={open}
        onClick={() => setOpen(true)}
      >
        <SearchIcon />
      </button>
      <button
        type="button"
        className="border-border bg-muted/60 text-muted-foreground hover:bg-muted hover:text-foreground focus-visible:border-ring focus-visible:ring-ring/50 hidden h-9 w-56 items-center gap-2 rounded-lg border px-3 text-sm transition-colors outline-none focus-visible:ring-3 md:inline-flex lg:w-64"
        aria-label="Search documentation"
        aria-haspopup="dialog"
        aria-expanded={open}
        onClick={() => setOpen(true)}
      >
        <SearchIcon />
        <span className="min-w-0 flex-1 truncate text-left">Search documentation...</span>
        <kbd className="border-border bg-background pointer-events-none rounded-md border px-1.5 py-0.5 font-sans text-[11px] font-medium">
          {modKey}K
        </kbd>
      </button>

      <dialog
        ref={dialogRef}
        className="docs-search-dialog"
        aria-labelledby={`${reactId}-title`}
        onKeyDown={onDialogKeyDown}
        onClick={onBackdropClick}
      >
        <h2 id={`${reactId}-title`} className="sr-only">
          Search documentation
        </h2>
        <div className="border-border flex items-center gap-2 border-b px-3">
          <SearchIcon className="text-muted-foreground size-4 shrink-0" />
          <input
            ref={inputRef}
            type="search"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Search documentation..."
            autoComplete="off"
            autoCorrect="off"
            autoCapitalize="off"
            spellCheck={false}
            className="placeholder:text-muted-foreground h-12 min-w-0 flex-1 bg-transparent text-sm outline-none"
            role="combobox"
            aria-expanded="true"
            aria-controls={resultsId}
            aria-activedescendant={activeId}
            aria-autocomplete="list"
          />
          <kbd className="border-border text-muted-foreground pointer-events-none hidden rounded-md border px-1.5 py-0.5 font-sans text-[11px] sm:inline">
            Esc
          </kbd>
        </div>
        <div
          id={resultsId}
          role="listbox"
          aria-label="Search results"
          className="max-h-80 overflow-y-auto overscroll-contain p-2"
        >
          {hits.length === 0 ? (
            <p className="text-muted-foreground px-3 py-8 text-center text-sm">No results for “{query.trim()}”.</p>
          ) : (
            groups.map((group) => (
              <section key={group.section} className="mb-2 last:mb-0">
                {group.label != null && (
                  <h3 className="text-muted-foreground px-2 pt-1 pb-1 text-xs font-medium tracking-wide uppercase">
                    {group.label}
                  </h3>
                )}
                <ul>
                  {group.entries.map((hit) => {
                    const selected = hit.index === activeIndex;
                    return (
                      <li key={`${hit.href}-${hit.heading ?? ''}`}>
                        <a
                          id={`${reactId}-opt-${hit.index}`}
                          href={hit.href}
                          role="option"
                          aria-selected={selected}
                          className={cn(
                            'flex flex-col rounded-lg px-3 py-2 no-underline',
                            selected ? 'bg-accent text-accent-foreground' : 'hover:bg-muted',
                          )}
                          onMouseEnter={() => setActiveIndex(hit.index)}
                          onClick={() => setOpen(false)}
                        >
                          <span className="text-sm font-medium">{hit.title}</span>
                          <span
                            className={cn('line-clamp-1 text-xs', selected ? 'opacity-80' : 'text-muted-foreground')}
                          >
                            {hit.heading ?? hit.description}
                          </span>
                        </a>
                      </li>
                    );
                  })}
                </ul>
              </section>
            ))
          )}
        </div>
      </dialog>
    </>
  );
}

function SearchIcon({ className }: { className?: string }) {
  return (
    <svg
      className={cn('size-4 shrink-0', className)}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      aria-hidden="true"
    >
      <circle cx="11" cy="11" r="7"></circle>
      <path d="m20 20-3-3"></path>
    </svg>
  );
}

function groupHits(hits: SearchHit[]) {
  const bySection = new Map<SearchHit['section'], SearchHit[]>();
  for (const hit of hits) {
    const list = bySection.get(hit.section) ?? [];
    list.push(hit);
    bySection.set(hit.section, list);
  }
  let index = 0;
  return SECTION_ORDER.flatMap((section) => {
    const sectionHits = bySection.get(section);
    if (sectionHits == null || sectionHits.length === 0) {
      return [];
    }
    return [
      {
        section,
        label: SECTION_LABELS[section],
        entries: sectionHits.map((hit) => ({ ...hit, index: index++ })),
      },
    ];
  });
}
