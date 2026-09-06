import { useMutation, useQueryClient } from '@tanstack/react-query';
import { CreateTopicForm, type CreateTopicFormValues } from './create-topic-form';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '../ui/dialog';
import { createTopic, topicQueryKeys } from '../../lib/topics-api';

export interface CreateTopicDialogProps {
  readonly open: boolean;
  readonly onOpenChange: (open: boolean) => void;
  /** Called once the broker has confirmed creation and the topic list has been invalidated. */
  readonly onCreated: (topic: string) => void;
}

/** The "create topic" flow, shared by every entry point that offers it — the Topics page and the Producer's topic picker both create the same way. */
export function CreateTopicDialog({ open, onOpenChange, onCreated }: CreateTopicDialogProps) {
  const queryClient = useQueryClient();
  const createMutation = useMutation({
    mutationFn: createTopic,
    onSuccess: async (result) => {
      await queryClient.invalidateQueries({ queryKey: topicQueryKeys.list() });
      onOpenChange(false);
      onCreated(result.topic);
    },
  });

  function handleCreate(values: CreateTopicFormValues): void {
    createMutation.mutate(values);
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Create topic</DialogTitle>
        </DialogHeader>
        <CreateTopicForm
          onSubmit={handleCreate}
          onCancel={() => onOpenChange(false)}
          pending={createMutation.isPending}
        />
        {createMutation.isError && (
          <p className="text-sm text-destructive" role="alert">
            {createMutation.error.message}
          </p>
        )}
      </DialogContent>
    </Dialog>
  );
}
