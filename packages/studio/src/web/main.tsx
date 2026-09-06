import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { MutationCache, QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { RouterProvider } from '@tanstack/react-router';
import { Toaster, toastError } from './components/ui/toast';
import { dismissSplash } from './lib/splash';
import { router } from './router';
import './styles/app.css';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // The server is on localhost. A failure here means it is down or the request was rejected,
      // and neither gets better on the third try — the v5 default of 3 retries with backoff just
      // held the loading state for several seconds before showing the error.
      retry: 1,
      staleTime: 5_000,
      refetchOnWindowFocus: false,
    },
    mutations: { retry: 0 },
  },
  // A mutation can fail after the dialog that started it has closed, so the failure needs a
  // surface that outlives the form. Routes may still render their own inline message.
  mutationCache: new MutationCache({
    onError: (error) => {
      toastError('Request failed', error);
    },
  }),
});

const root = document.getElementById('root');
if (root) {
  createRoot(root).render(
    <StrictMode>
      <QueryClientProvider client={queryClient}>
        <RouterProvider router={router} />
        <Toaster />
      </QueryClientProvider>
    </StrictMode>,
  );
  dismissSplash();
}
