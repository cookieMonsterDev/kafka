import { ShieldCheck } from 'lucide-react';
import { useQuery } from '@tanstack/react-query';
import { EmptyState } from '../ui/empty-state';
import { ErrorState } from '../ui/error-state';
import { listAcls, settingsQueryKeys } from '../../lib/settings-api';
import { LoadingRows, TableShell } from './table-shell';

export function AclTable() {
  const { data, isPending, isError, error, refetch } = useQuery({
    queryKey: settingsQueryKeys.acls,
    queryFn: listAcls,
  });

  if (isPending) return <LoadingRows />;
  if (isError) return <ErrorState title="Could not load ACLs" error={error} onRetry={() => void refetch()} />;
  if (data.acls.length === 0) {
    return (
      <EmptyState
        icon={ShieldCheck}
        title="No ACLs configured"
        description="This cluster has no access control entries, or authorization isn't enabled."
      />
    );
  }

  return (
    <TableShell label="ACLs">
      <thead>
        <tr className="border-b border-border text-left text-xs text-muted-foreground">
          <th scope="col" className="px-3 py-2 font-medium">
            Resource
          </th>
          <th scope="col" className="px-3 py-2 font-medium">
            Pattern
          </th>
          <th scope="col" className="px-3 py-2 font-medium">
            Principal
          </th>
          <th scope="col" className="px-3 py-2 font-medium">
            Host
          </th>
          <th scope="col" className="px-3 py-2 font-medium">
            Operation
          </th>
          <th scope="col" className="px-3 py-2 font-medium">
            Permission
          </th>
        </tr>
      </thead>
      <tbody>
        {data.acls.map((acl) => (
          <tr
            key={`${acl.resourceType}:${acl.resourceName}:${acl.resourcePatternType}:${acl.principal}:${acl.host}:${acl.operation}:${acl.permissionType}`}
            className="border-b border-border last:border-0"
          >
            <td className="px-3 py-2">
              <span className="text-xs text-muted-foreground">{acl.resourceType}</span> {acl.resourceName}
            </td>
            <td className="px-3 py-2">{acl.resourcePatternType}</td>
            <td className="px-3 py-2 font-mono text-xs">{acl.principal}</td>
            <td className="px-3 py-2">{acl.host}</td>
            <td className="px-3 py-2">{acl.operation}</td>
            <td className="px-3 py-2">{acl.permissionType}</td>
          </tr>
        ))}
      </tbody>
    </TableShell>
  );
}
