import { useMemo, useRef, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Link, createRoute, useNavigate } from '@tanstack/react-router';
import { createColumnHelper, tableFeatures, useTable } from '@tanstack/react-table';
import { useVirtualizer } from '@tanstack/react-virtual';
import type { TopicListEntry } from '../../shared/contracts/topic';
import { CreateTopicForm, type CreateTopicFormValues } from '../components/topics/create-topic-form';
import { Button } from '../components/ui/button';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '../components/ui/dialog';
import { Input } from '../components/ui/input';
import { createTopic, listTopics, topicQueryKeys } from '../lib/topics-api';
import { rootRoute } from './root';

export const topicsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/topics',
  component: TopicsPage,
});

const ROW_HEIGHT_PX = 40;

// No sorting/filtering/pagination features are registered — this table only ever renders rows the
// page has already filtered client-side, so the core row model (always available) is enough.
const topicTableFeatures = tableFeatures({});
const columnHelper = createColumnHelper<typeof topicTableFeatures, TopicListEntry>();
const columns = columnHelper.columns([
  columnHelper.accessor('name', {
    header: 'Name',
    cell: (info) => (
      <Link
        to="/topics/$name"
        params={{ name: info.row.original.name }}
        className="rounded-sm font-medium outline-none hover:underline focus-visible:ring-3 focus-visible:ring-ring/50"
        onClick={(event) => event.stopPropagation()}
      >
        {info.getValue()}
      </Link>
    ),
  }),
  columnHelper.accessor('partitionCount', { header: 'Partitions' }),
  columnHelper.accessor('replicationFactor', {
    header: 'Replication factor',
    cell: (info) => info.getValue() ?? '—',
  }),
]);

function TopicsPage() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [search, setSearch] = useState('');
  const [createOpen, setCreateOpen] = useState(false);
  const scrollRef = useRef<HTMLDivElement>(null);

  const { data, isPending, isError } = useQuery({ queryKey: topicQueryKeys.list(), queryFn: listTopics });
  const createMutation = useMutation({
    mutationFn: createTopic,
    onSuccess: async (result) => {
      await queryClient.invalidateQueries({ queryKey: topicQueryKeys.list() });
      setCreateOpen(false);
      void navigate({ to: '/topics/$name', params: { name: result.topic } });
    },
  });

  const filtered = useMemo(() => {
    const topics = data?.topics ?? [];
    const query = search.trim().toLowerCase();
    return query === '' ? topics : topics.filter((topic) => topic.name.toLowerCase().includes(query));
  }, [data, search]);

  const table = useTable({ features: topicTableFeatures, columns, data: filtered });
  const rows = table.getRowModel().rows;

  const virtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => ROW_HEIGHT_PX,
    getItemKey: (index) => rows[index]?.id ?? index,
    overscan: 12,
  });

  function handleCreate(values: CreateTopicFormValues): void {
    createMutation.mutate(values);
  }

  return (
    <section aria-labelledby="topics-heading" className="flex flex-col gap-4">
      <div className="flex items-center justify-between gap-3">
        <h1 id="topics-heading" className="text-lg font-semibold">
          Topics
        </h1>
        <Dialog open={createOpen} onOpenChange={setCreateOpen}>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>Create topic</DialogTitle>
            </DialogHeader>
            <CreateTopicForm
              onSubmit={handleCreate}
              onCancel={() => setCreateOpen(false)}
              pending={createMutation.isPending}
            />
            {createMutation.isError && (
              <p className="text-sm text-destructive" role="alert">
                {createMutation.error.message}
              </p>
            )}
          </DialogContent>
        </Dialog>
        <Button type="button" onClick={() => setCreateOpen(true)}>
          Create topic
        </Button>
      </div>

      <label className="flex flex-col gap-1.5 sm:max-w-xs">
        <span className="sr-only">Search topics</span>
        <Input
          type="search"
          placeholder="Search topics…"
          value={search}
          onChange={(event) => setSearch(event.target.value)}
        />
      </label>

      {isPending && (
        <p className="text-sm text-muted-foreground" role="status">
          Loading topics…
        </p>
      )}
      {isError && (
        <p className="text-sm text-destructive" role="alert">
          Could not load topics.
        </p>
      )}

      {data && (
        <div className="overflow-hidden rounded-xl border border-border">
          <table className="w-full table-fixed text-sm">
            <thead>
              {table.getHeaderGroups().map((headerGroup) => (
                <tr key={headerGroup.id} className="border-b border-border bg-muted/40 text-left">
                  {headerGroup.headers.map((header) => (
                    <th key={header.id} scope="col" className="px-3 py-2 text-xs font-medium text-muted-foreground">
                      {header.isPlaceholder ? null : <table.FlexRender header={header} />}
                    </th>
                  ))}
                </tr>
              ))}
            </thead>
          </table>
          <div ref={scrollRef} className="max-h-[60vh] overflow-y-auto">
            {rows.length === 0 ? (
              <p className="px-3 py-6 text-center text-sm text-muted-foreground">
                {search.trim() === '' ? 'No topics yet.' : `No topics match "${search}".`}
              </p>
            ) : (
              <table className="w-full table-fixed text-sm">
                <tbody
                  style={{ height: `${String(virtualizer.getTotalSize())}px`, position: 'relative', display: 'block' }}
                >
                  {virtualizer.getVirtualItems().map((virtualRow) => {
                    const row = rows[virtualRow.index];
                    if (row === undefined) return null;
                    return (
                      <tr
                        key={row.id}
                        data-index={virtualRow.index}
                        ref={virtualizer.measureElement}
                        style={{
                          position: 'absolute',
                          top: 0,
                          left: 0,
                          width: '100%',
                          minHeight: `${String(ROW_HEIGHT_PX)}px`,
                          transform: `translateY(${String(virtualRow.start)}px)`,
                        }}
                        className="flex cursor-pointer items-center border-b border-border last:border-0 hover:bg-muted/40"
                        onClick={() => void navigate({ to: '/topics/$name', params: { name: row.original.name } })}
                      >
                        {row.getAllCells().map((cell) => (
                          <td key={cell.id} className="flex-1 px-3 py-2">
                            <table.FlexRender cell={cell} />
                          </td>
                        ))}
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            )}
          </div>
        </div>
      )}
    </section>
  );
}
