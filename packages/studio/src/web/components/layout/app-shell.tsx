import { createContext, useContext, useEffect, useState } from 'react';
import { TooltipProvider } from '../ui/tooltip';
import { MobileNavSheet } from './mobile-nav-sheet';
import { Sidebar } from './sidebar';
import { Topbar } from './topbar';

/** Versioned localStorage key for the desktop/tablet sidebar's expanded-vs-icon-only choice. */
const SIDEBAR_STORAGE_KEY = 'kafka-studio-sidebar:v1';

/** Below this width the sidebar is an off-canvas drawer instead of fixed chrome. */
const NOT_MOBILE_QUERY = '(min-width: 768px)';
/** At or above this width, a first-ever visit defaults the sidebar to expanded instead of collapsed. */
const DESKTOP_QUERY = '(min-width: 1024px)';

function useMediaQuery(query: string): boolean {
  const [matches, setMatches] = useState(() => window.matchMedia(query).matches);

  useEffect(() => {
    const list = window.matchMedia(query);
    setMatches(list.matches);
    function handleChange(event: MediaQueryListEvent) {
      setMatches(event.matches);
    }
    list.addEventListener('change', handleChange);
    return () => list.removeEventListener('change', handleChange);
  }, [query]);

  return matches;
}

function readStoredCollapsed(): boolean | null {
  try {
    const raw = localStorage.getItem(SIDEBAR_STORAGE_KEY);
    if (raw === 'true') return true;
    if (raw === 'false') return false;
    return null;
  } catch {
    return null;
  }
}

interface AppShellContextValue {
  /** Below the `md` breakpoint: sidebar renders as an off-canvas drawer instead of fixed chrome. */
  readonly isMobile: boolean;
  /** Fixed sidebar is icon-only. Meaningless (and ignored) while `isMobile` is true. */
  readonly collapsed: boolean;
  readonly toggleCollapsed: () => void;
  readonly mobileNavOpen: boolean;
  readonly openMobileNav: () => void;
  readonly closeMobileNav: () => void;
}

const AppShellContext = createContext<AppShellContextValue | null>(null);

export function useAppShell(): AppShellContextValue {
  const context = useContext(AppShellContext);
  if (context === null) throw new Error('useAppShell must be used within <AppShell>');
  return context;
}

export interface AppShellProps {
  readonly children: React.ReactNode;
}

/**
 * Owns the one "is the sidebar visible, and how" state machine described in the plan: a single
 * `md` breakpoint switches between an off-canvas drawer and fixed chrome, and a persisted choice
 * (desktop defaults expanded, narrower fixed widths default collapsed on first visit) controls the
 * fixed chrome's icon-only state. Descendants read it through `useAppShell` rather than each
 * re-deriving viewport state.
 */
export function AppShell({ children }: AppShellProps) {
  const isMobile = !useMediaQuery(NOT_MOBILE_QUERY);
  const isDesktop = useMediaQuery(DESKTOP_QUERY);

  const [collapsed, setCollapsed] = useState(() => readStoredCollapsed() ?? !isDesktop);
  const [mobileNavOpen, setMobileNavOpen] = useState(false);

  useEffect(() => {
    try {
      localStorage.setItem(SIDEBAR_STORAGE_KEY, String(collapsed));
    } catch {
      // private mode, quota, or disabled storage
    }
  }, [collapsed]);

  // A resize across the mobile breakpoint shouldn't leave the off-canvas drawer open underneath
  // the now-visible fixed sidebar.
  useEffect(() => {
    if (!isMobile) setMobileNavOpen(false);
  }, [isMobile]);

  const contextValue: AppShellContextValue = {
    isMobile,
    collapsed,
    toggleCollapsed: () => setCollapsed((current) => !current),
    mobileNavOpen,
    openMobileNav: () => setMobileNavOpen(true),
    closeMobileNav: () => setMobileNavOpen(false),
  };

  return (
    <AppShellContext.Provider value={contextValue}>
      <TooltipProvider delayDuration={200}>
        <div className="flex min-h-dvh">
          <a
            href="#main-content"
            className="sr-only focus-visible:not-sr-only focus-visible:absolute focus-visible:top-2 focus-visible:left-2 focus-visible:z-50 focus-visible:rounded-lg focus-visible:bg-background focus-visible:px-3 focus-visible:py-2 focus-visible:text-sm"
          >
            Skip to content
          </a>
          <Sidebar />
          <MobileNavSheet />
          <div className="flex min-w-0 flex-1 flex-col">
            <Topbar />
            <main id="main-content" className="min-w-0 flex-1 p-4 lg:p-6">
              {children}
            </main>
          </div>
        </div>
      </TooltipProvider>
    </AppShellContext.Provider>
  );
}
