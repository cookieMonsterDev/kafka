import { useState } from 'react';
import { Plus, TriangleAlert } from 'lucide-react';
import { useQuery } from '@tanstack/react-query';
import { CreateTopicDialog } from '../topics/create-topic-dialog';
import { Button } from '../ui/button';
import { errorMessage } from '../../lib/error-message';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../ui/select';
import { listTopics, topicQueryKeys } from '../../lib/topics-api';

export interface TopicPickerProps {
  readonly value: string | null;
  readonly onChange: (topic: string) => void;
  readonly disabled?: boolean;
}

/** A topic select fed by the same topic list the Topics page uses, plus an inline "create topic" escape hatch. */
export function TopicPicker({ value, onChange, disabled = false }: TopicPickerProps) {
  const [createOpen, setCreateOpen] = useState(false);
  const { data, isPending, isError, error, refetch } = useQuery({
    queryKey: topicQueryKeys.list(),
    queryFn: listTopics,
  });

  return (
    <div className="flex flex-wrap items-center gap-2">
      {isError ? (
        // Compact destructive chip — `ErrorState`'s own layout is a centered full-section block.
        <div
          role="alert"
          className="flex items-center gap-2 rounded-lg border border-destructive/30 bg-destructive/10 px-3 py-1.5 text-sm text-destructive"
        >
          <TriangleAlert className="size-4 shrink-0" aria-hidden="true" />
          {/* Fixed cap, not flex-shrink: this sits several toolbar ancestors deep, and truncation needs every one of them at `min-width: 0`. */}
          <span className="max-w-[22rem] truncate sm:max-w-[32rem]">
            Could not load topics: {errorMessage(error) ?? 'unknown error'}
          </span>
          <Button type="button" variant="outline" size="sm" className="shrink-0" onClick={() => void refetch()}>
            Retry
          </Button>
        </div>
      ) : (
        <Select value={value ?? undefined} onValueChange={onChange} disabled={disabled || isPending}>
          <SelectTrigger className="w-56" aria-label="Topic">
            <SelectValue placeholder={isPending ? 'Loading topics…' : 'Select a topic'} />
          </SelectTrigger>
          <SelectContent>
            {(data?.topics ?? []).map((topic) => (
              <SelectItem key={topic.name} value={topic.name}>
                {topic.name}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      )}
      <Button type="button" variant="outline" size="sm" disabled={disabled} onClick={() => setCreateOpen(true)}>
        <Plus className="size-4" aria-hidden="true" />
        New topic
      </Button>

      <CreateTopicDialog open={createOpen} onOpenChange={setCreateOpen} onCreated={onChange} />
    </div>
  );
}
