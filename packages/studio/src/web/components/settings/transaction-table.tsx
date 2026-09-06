import { ArrowLeftRight } from 'lucide-react';
import { useQuery } from '@tanstack/react-query';
import { EmptyState } from '../ui/empty-state';
import { ErrorState } from '../ui/error-state';
import { listTransactions, settingsQueryKeys } from '../../lib/settings-api';
import { LoadingRows, TableShell } from './table-shell';

export function TransactionTable() {
  const { data, isPending, isError, error, refetch } = useQuery({
    queryKey: settingsQueryKeys.transactions,
    queryFn: listTransactions,
  });

  if (isPending) return <LoadingRows />;
  if (isError) return <ErrorState title="Could not load transactions" error={error} onRetry={() => void refetch()} />;
  if (data.transactions.length === 0) {
    return (
      <EmptyState
        icon={ArrowLeftRight}
        title="No transactions"
        description="Nothing is producing transactionally on this cluster right now."
      />
    );
  }

  return (
    <TableShell label="Transactions">
      <thead>
        <tr className="border-b border-border text-left text-xs text-muted-foreground">
          <th scope="col" className="px-3 py-2 font-medium">
            Transactional ID
          </th>
          <th scope="col" className="px-3 py-2 font-medium">
            Producer ID
          </th>
          <th scope="col" className="px-3 py-2 font-medium">
            State
          </th>
        </tr>
      </thead>
      <tbody>
        {data.transactions.map((transaction) => (
          <tr key={transaction.transactionalId} className="border-b border-border last:border-0">
            <td className="px-3 py-2 font-mono text-xs">{transaction.transactionalId}</td>
            <td className="px-3 py-2">{transaction.producerId}</td>
            <td className="px-3 py-2">{transaction.transactionState}</td>
          </tr>
        ))}
      </tbody>
    </TableShell>
  );
}
