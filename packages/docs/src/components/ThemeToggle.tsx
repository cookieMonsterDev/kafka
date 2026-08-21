import { useEffect, useRef } from 'react';
import { Button } from '@/components/ui/button';
import { THEME_COLOR_DARK, THEME_COLOR_LIGHT, THEME_STORAGE_KEY } from '@/lib/theme';

/**
 * Interactive React island. Rendered from .astro with a `client:*` directive,
 * which is what ships and hydrates the React runtime for this component only.
 *
 * Icons swap with CSS (`.dark`) so the first paint matches the FOUC script in
 * BaseLayout. No useState for the glyph — that would flash the wrong icon after hydrate.
 * Accessible name and pressed state are updated on the DOM node so a re-render cannot
 * reset them to the default dark-theme attributes.
 */
export function ThemeToggle() {
  const buttonRef = useRef<HTMLButtonElement>(null);
  const statusRef = useRef<HTMLSpanElement>(null);

  function isDark() {
    return document.documentElement.classList.contains('dark');
  }

  function syncControl(announce: boolean) {
    const button = buttonRef.current;
    const dark = isDark();
    if (button != null) {
      button.setAttribute('aria-label', dark ? 'Switch to light theme' : 'Switch to dark theme');
      button.setAttribute('aria-pressed', dark ? 'true' : 'false');
    }
    if (announce && statusRef.current != null) {
      statusRef.current.textContent = dark ? 'Dark theme' : 'Light theme';
    }
  }

  useEffect(() => {
    syncControl(false);
  }, []);

  function toggle() {
    const nextDark = !isDark();
    document.documentElement.classList.toggle('dark', nextDark);
    try {
      localStorage.setItem(THEME_STORAGE_KEY, nextDark ? 'dark' : 'light');
    } catch {
      // private mode, quota, or disabled storage
    }
    const meta = document.querySelector('meta[name="theme-color"]');
    if (meta != null) {
      meta.setAttribute('content', nextDark ? THEME_COLOR_DARK : THEME_COLOR_LIGHT);
    }
    syncControl(true);
  }

  return (
    <>
      <Button
        ref={buttonRef}
        type="button"
        variant="ghost"
        size="icon-lg"
        onClick={toggle}
        aria-label="Switch to light theme"
        aria-pressed="true"
      >
        <svg
          className="hidden size-5 dark:block"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          aria-hidden="true"
        >
          <circle cx="12" cy="12" r="4"></circle>
          <path d="M12 2v2"></path>
          <path d="M12 20v2"></path>
          <path d="m4.93 4.93 1.41 1.41"></path>
          <path d="m17.66 17.66 1.41 1.41"></path>
          <path d="M2 12h2"></path>
          <path d="M20 12h2"></path>
          <path d="m6.34 17.66-1.41 1.41"></path>
          <path d="m19.07 4.93-1.41 1.41"></path>
        </svg>
        <svg
          className="size-5 dark:hidden"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
          aria-hidden="true"
        >
          <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"></path>
        </svg>
      </Button>
      <span ref={statusRef} className="sr-only" aria-live="polite" />
    </>
  );
}
