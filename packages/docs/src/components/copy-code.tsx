import { useEffect } from 'react';

/**
 * Click-to-copy for the landing install chip (`[data-copy]`) and Markdown
 * `pre.astro-code` blocks. Hydrated once from BaseLayout.
 */
export function CopyCode() {
  useEffect(() => {
    let timer = 0;

    function onClick(event: MouseEvent) {
      const target = event.target;
      if (!(target instanceof Element)) {
        return;
      }
      const el = target.closest<HTMLElement>('[data-copy], pre.astro-code');
      if (el == null) {
        return;
      }
      const selection = window.getSelection();
      if (selection != null && selection.toString().length > 0 && el.contains(selection.anchorNode)) {
        return;
      }
      const text = (el.getAttribute('data-copy') ?? el.innerText).trim();
      if (text.length === 0) {
        return;
      }
      void navigator.clipboard.writeText(text).then(() => {
        el.dataset.copied = 'true';
        window.clearTimeout(timer);
        timer = window.setTimeout(() => {
          delete el.dataset.copied;
        }, 1500);
      });
    }

    document.addEventListener('click', onClick);
    return () => {
      window.clearTimeout(timer);
      document.removeEventListener('click', onClick);
    };
  }, []);

  return null;
}
