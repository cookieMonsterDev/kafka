/**
 * Removes the splash markup that `web/index.html` paints before the bundle loads.
 *
 * The dismissal is tied to the app actually mounting rather than a fixed timer, so on a fast
 * machine the splash is a blink and on a slow one it covers the real wait. The only timing here
 * is a floor: below it the splash would flash rather than register, which reads as a glitch.
 */
const MINIMUM_VISIBLE_MS = 400;
const FADE_MS = 320;

const startedAt = Date.now();

export function dismissSplash(): void {
  const splash = document.getElementById('studio-splash');
  if (splash === null) return;

  const reduceMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  const hold = reduceMotion ? 0 : Math.max(0, MINIMUM_VISIBLE_MS - (Date.now() - startedAt));

  window.setTimeout(() => {
    if (reduceMotion) {
      splash.remove();
      return;
    }
    splash.setAttribute('data-leaving', '');
    // Removed on a timer rather than `transitionend`, which never fires if the element is
    // hidden (a background tab) and would leave the overlay stuck over the app.
    window.setTimeout(() => {
      splash.remove();
    }, FADE_MS);
  }, hold);
}
