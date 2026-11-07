import { SelectInput } from '../ui/base-select';
import { useState } from 'react';
import { Field, type OperationProps } from './fields';
import { Button } from '../ui/button';
import { Input } from '../ui/input';
import { apiSend } from '../../lib/api';
import type { ConfigDifference } from '../../../shared/contracts/debug';

export function ComparePanel({
  running,
  operate,
  setOutput,
  profile,
  profiles,
}: Omit<OperationProps, 'canWrite'> & { readonly profile: string | null; readonly profiles: readonly string[] }) {
  const [target, setTarget] = useState('');
  const [rightProfile, setRightProfile] = useState('');
  return (
    <section aria-label="Environment comparison" className="flex flex-col gap-4 rounded-xl border border-border p-4">
      <Field label="Topic">
        <Input value={target} onChange={(event) => setTarget(event.target.value)} />
      </Field>
      <Field label="Compare active profile against">
        <SelectInput
          aria-label="Compare active profile against"
          value={rightProfile}
          onValueChange={setRightProfile}
          items={[{ value: '', label: 'default' }, ...profiles.map((name) => ({ value: name, label: name }))]}
        />
      </Field>
      <Button
        disabled={running || !target}
        onClick={() =>
          void operate(async (signal) => {
            setOutput(
              await apiSend<{ differences: ConfigDifference[] }>(
                'POST',
                '/api/debug/compare',
                {
                  topic: target,
                  left: profile,
                  right: rightProfile || null,
                },
                signal,
              ),
            );
          })
        }
      >
        Compare configurations and schemas
      </Button>
      <p className="text-sm">
        Reads both profiles without changing the active connection. Sensitive settings are excluded. Schema comparisons
        use the conventional topic-value subject.
      </p>
    </section>
  );
}
