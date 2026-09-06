import { useState } from 'react';
import { ArrowLeftRight, Gauge, ShieldCheck } from 'lucide-react';
import { createRoute } from '@tanstack/react-router';
import { PageLayout } from '../components/layout/page';
import { AclTable } from '../components/settings/acl-table';
import { QuotaTable } from '../components/settings/quota-table';
import { TransactionTable } from '../components/settings/transaction-table';
import { cn } from '../lib/utils';
import { rootRoute } from './root';

export const settingsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/settings',
  component: SettingsPage,
});

type Tab = 'acls' | 'quotas' | 'transactions';

const TAB_LABEL: Record<Tab, string> = { acls: 'ACLs', quotas: 'Quotas', transactions: 'Transactions' };
const TAB_ICON: Record<Tab, typeof ShieldCheck> = { acls: ShieldCheck, quotas: Gauge, transactions: ArrowLeftRight };

function TabToggle({ tab, onChange }: { readonly tab: Tab; readonly onChange: (tab: Tab) => void }) {
  return (
    <div role="radiogroup" aria-label="Settings section" className="flex gap-1 rounded-lg border border-border p-1">
      {(Object.keys(TAB_LABEL) as Tab[]).map((value) => (
        <button
          key={value}
          type="button"
          role="radio"
          aria-checked={tab === value}
          onClick={() => onChange(value)}
          className={cn(
            'rounded-md px-3 py-1 text-sm outline-none transition-colors focus-visible:ring-3 focus-visible:ring-ring/50',
            tab === value ? 'bg-accent font-medium text-foreground' : 'text-muted-foreground hover:text-foreground',
          )}
        >
          {TAB_LABEL[value]}
        </button>
      ))}
    </div>
  );
}

/** Read-only cluster inspection: ACLs, client quotas, and in-flight transactions — nothing here mutates anything, so it's exposed the same way whether or not `--read-only` is set. */
function SettingsPage() {
  const [tab, setTab] = useState<Tab>('acls');
  const Icon = TAB_ICON[tab];

  const toolbar = <TabToggle tab={tab} onChange={setTab} />;

  return (
    <PageLayout toolbar={toolbar}>
      <section aria-label={TAB_LABEL[tab]} className="flex flex-col gap-4">
        <h2 className="flex items-center gap-2 text-sm font-semibold text-muted-foreground">
          <Icon className="size-4" aria-hidden="true" />
          {TAB_LABEL[tab]}
        </h2>
        {tab === 'acls' && <AclTable />}
        {tab === 'quotas' && <QuotaTable />}
        {tab === 'transactions' && <TransactionTable />}
      </section>
    </PageLayout>
  );
}
