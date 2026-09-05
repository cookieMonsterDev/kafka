import { Outlet, createRootRoute } from '@tanstack/react-router';
import { AppShell } from '../components/layout/app-shell';

export const rootRoute = createRootRoute({ component: RootLayout });

function RootLayout() {
  return (
    <AppShell>
      <Outlet />
    </AppShell>
  );
}
