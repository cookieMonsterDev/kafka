import { useEffect, useState } from 'react';

const REDUCED_MOTION_QUERY = '(prefers-reduced-motion: reduce)';

/** True whenever the OS/browser setting is on — the board never animates particles against it, only a manual "play" click does. */
export function usePrefersReducedMotion(): boolean {
  const [reduced, setReduced] = useState(() => window.matchMedia(REDUCED_MOTION_QUERY).matches);

  useEffect(() => {
    const list = window.matchMedia(REDUCED_MOTION_QUERY);
    setReduced(list.matches);
    const handleChange = (event: MediaQueryListEvent): void => setReduced(event.matches);
    list.addEventListener('change', handleChange);
    return () => list.removeEventListener('change', handleChange);
  }, []);

  return reduced;
}
