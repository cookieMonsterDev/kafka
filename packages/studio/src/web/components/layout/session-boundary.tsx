import { useSyncExternalStore, type ReactNode } from 'react';
import { isSessionExpired, subscribeToSession } from '../../lib/auth';

export function SessionBoundary({ children }: { readonly children: ReactNode }) {
  const expired = useSyncExternalStore(subscribeToSession, isSessionExpired);

  if (!expired) return children;

  return (
    <main id="main-content" className="flex min-h-dvh items-center justify-center p-6">
      <section role="alert" className="w-full max-w-lg rounded-xl border border-border bg-card p-6">
        <h1 className="text-xl font-semibold">Reopen your Studio session</h1>
        <p className="mt-3 text-sm text-muted-foreground">
          This tab has no valid session token. Reloading the page or restarting Studio ends access from this tab.
        </p>
        <ol className="mt-4 list-decimal space-y-2 pl-5 text-sm">
          <li>Return to the terminal where you started Kafka Studio.</li>
          <li>Open the full URL printed there in a new tab, including the token after the # sign.</li>
          <li>If Studio has stopped, start it again and open the newly printed URL.</li>
        </ol>
        <p className="mt-4 text-sm text-muted-foreground">Keep that URL private: it grants access to your cluster.</p>
      </section>
    </main>
  );
}
