import { useState } from 'react';
import { Plus } from 'lucide-react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { CreateTopicForm, type CreateTopicFormValues } from '../topics/create-topic-form';
import { Button } from '../ui/button';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '../ui/dialog';
import { errorMessage } from '../ui/error-state';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../ui/select';
import { createTopic, listTopics, topicQueryKeys } from '../../lib/topics-api';

export interface TopicPickerProps {
  readonly value: string | null;
  readonly onChange: (topic: string) => void;
  readonly disabled?: boolean;
}

/** A topic select fed by the same topic list the Topics page uses, plus an inline "create topic" escape hatch. */
export function TopicPicker({ value, onChange, disabled = false }: TopicPickerProps) {
  const [createOpen, setCreateOpen] = useState(false);
  const queryClient = useQueryClient();
  const { data, isPending, isError, error, refetch } = useQuery({
    queryKey: topicQueryKeys.list(),
    queryFn: listTopics,
  });

  const createMutation = useMutation({
    mutationFn: createTopic,
    onSuccess: async (result) => {
      await queryClient.invalidateQueries({ queryKey: topicQueryKeys.list() });
      onChange(result.topic);
      setCreateOpen(false);
    },
  });

  function handleCreate(values: CreateTopicFormValues): void {
    createMutation.mutate(values);
  }

  return (
    <div className="flex flex-wrap items-center gap-2">
      {isError ? (
        <div className="flex items-center gap-2 text-sm text-destructive">
          <span>Could not load topics: {errorMessage(error) ?? 'unknown error'}</span>
          <Button type="button" variant="outline" size="sm" onClick={() => void refetch()}>
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
    </div>
  );
}
