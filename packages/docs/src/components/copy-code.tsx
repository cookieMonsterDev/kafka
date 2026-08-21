import { useEffect, useState } from 'react';

/**
 * Click-to-copy for the landing install chip (`[data-copy]`) and Markdown
 * `pre.astro-code` blocks. Hydrated once from BaseLayout.
 */
export function CopyCode() {
  const [status, setStatus] = useState('');

  useEffect(() => {
    let timer = 0;

    function markCopied(el: HTMLElement) {
      el.dataset.copied = 'true';
      setStatus('Copied');
      window.clearTimeout(timer);
      timer = window.setTimeout(() => {
        delete el.dataset.copied;
        setStatus('');
      }, 1500);
    }

    function copyFrom(el: HTMLElement) {
      const code = el.tagName === 'PRE' ? el.querySelector('code') : null;
      const text = (el.getAttribute('data-copy') ?? code?.innerText ?? el.innerText).trim();
      if (text.length === 0) {
        return;
      }
      void navigator.clipboard.writeText(text).then(
        () => {
          markCopied(el);
        },
        () => {
          setStatus('Copy failed');
        },
      );
    }

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
      copyFrom(el);
    }

    function onKeyDown(event: KeyboardEvent) {
      if (event.key !== 'Enter' && event.key !== ' ') {
        return;
      }
      const el = event.target;
      if (!(el instanceof HTMLElement) || !el.matches('pre.astro-code')) {
        return;
      }
      event.preventDefault();
      copyFrom(el);
    }

    document.querySelectorAll('pre.astro-code').forEach((pre) => {
      if (!(pre instanceof HTMLElement)) {
        return;
      }
      pre.tabIndex = 0;
      pre.setAttribute('role', 'button');
      pre.setAttribute('aria-label', 'Copy code');
    });

    document.addEventListener('click', onClick);
    document.addEventListener('keydown', onKeyDown);
    return () => {
      window.clearTimeout(timer);
      document.removeEventListener('click', onClick);
      document.removeEventListener('keydown', onKeyDown);
    };
  }, []);

  return (
    <div id="copy-status" className="sr-only" aria-live="polite">
      {status}
    </div>
  );
}
