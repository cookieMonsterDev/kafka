import { useEffect, useState } from 'react';

const COPY_ICON = `<svg class="copy-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect width="14" height="14" x="8" y="8" rx="2" ry="2"></rect><path d="M4 16c-1.1 0-2-.9-2-2V4c0-1.1.9-2 2-2h10c1.1 0 2 .9 2 2"></path></svg><svg class="copy-check" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M20 6 9 17l-5-5"></path></svg>`;

/**
 * Click-to-copy for `.copy-btn` (landing install + Markdown `pre.astro-code`).
 * Hydrated once from BaseLayout. Injects an icon button into each code block.
 */
export function CopyCode() {
  const [status, setStatus] = useState('');

  useEffect(() => {
    const wraps: HTMLElement[] = [];
    let timer = 0;

    function markCopied(button: HTMLElement) {
      if (button.dataset.copyLabel == null) {
        button.dataset.copyLabel = button.getAttribute('aria-label') ?? 'Copy';
      }
      button.dataset.copied = 'true';
      button.setAttribute('aria-label', 'Copied');
      setStatus('Copied');
      window.clearTimeout(timer);
      timer = window.setTimeout(() => {
        delete button.dataset.copied;
        button.setAttribute('aria-label', button.dataset.copyLabel ?? 'Copy');
        setStatus('');
      }, 1500);
    }

    function textFrom(button: HTMLElement): string {
      const explicit = button.getAttribute('data-copy');
      if (explicit != null && explicit.length > 0) {
        return explicit.trim();
      }
      const pre = button.closest('.copy-block')?.querySelector('pre.astro-code');
      if (!(pre instanceof HTMLElement)) {
        return '';
      }
      return (pre.querySelector('code')?.innerText ?? pre.innerText).trim();
    }

    function copyFrom(button: HTMLElement) {
      const text = textFrom(button);
      if (text.length === 0) {
        return;
      }
      void navigator.clipboard.writeText(text).then(
        () => {
          markCopied(button);
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
      const trigger = target.closest<HTMLElement>('.copy-command, .copy-btn');
      if (trigger == null) {
        return;
      }
      event.preventDefault();
      event.stopPropagation();
      copyFrom(trigger);
    }

    document.querySelectorAll('pre.astro-code').forEach((pre) => {
      if (!(pre instanceof HTMLElement) || pre.closest('.copy-block') != null) {
        return;
      }
      const wrap = document.createElement('div');
      wrap.className = 'copy-block';
      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'copy-btn';
      button.setAttribute('aria-label', 'Copy');
      button.innerHTML = COPY_ICON;
      pre.replaceWith(wrap);
      wrap.append(pre, button);
      wraps.push(wrap);
    });

    document.addEventListener('click', onClick);
    return () => {
      window.clearTimeout(timer);
      document.removeEventListener('click', onClick);
      wraps.forEach((wrap) => {
        const pre = wrap.querySelector('pre.astro-code');
        if (pre != null) {
          wrap.replaceWith(pre);
        } else {
          wrap.remove();
        }
      });
    };
  }, []);

  return (
    <div id="copy-status" className="sr-only" aria-live="polite">
      {status}
    </div>
  );
}
