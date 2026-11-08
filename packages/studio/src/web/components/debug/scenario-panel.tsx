import { useState } from 'react';
import { Field, type OperationProps } from './fields';
import { Button } from '../ui/button';
import { Input } from '../ui/input';
import { Textarea } from '../ui/textarea';
import { scenarioSchema } from '../../../shared/contracts/debug';
import { streamDebug, downloadJson } from '../../lib/debug-api';
const EXAMPLE = {
  version: 1,
  name: 'Order flow',
  steps: [
    { kind: 'produce', topic: 'orders', key: '{{id}}', value: '{"orderId":"{{id}}"}' },
    {
      kind: 'expect',
      topic: 'payments',
      predicates: [{ field: 'value.orderId', operator: 'eq', value: '{{id}}' }],
      timeoutMs: 10000,
    },
  ],
};

export function ScenarioPanel({
  running,
  canWrite,
  operate,
  setOutput,
  setError,
}: OperationProps & { readonly setError: (value: string) => void }) {
  const [scenario, setScenario] = useState(JSON.stringify(EXAMPLE, null, 2));
  return (
    <section aria-label="Test scenarios" className="flex flex-col gap-4 rounded-xl border border-border p-4">
      <p className="text-sm">
        Produce and expect steps run in order. {'{{id}}'} is replaced with a new run ID in keys, values, headers, and
        predicates. Expectations read from offsets captured before the first step. Running a scenario produces real
        events.
      </p>
      <Field label="Scenario JSON">
        <Textarea rows={18} value={scenario} onChange={(event) => setScenario(event.target.value)} />
      </Field>
      <Field label="Import scenario JSON">
        <Input
          type="file"
          accept=".json,application/json"
          onChange={(event) => {
            const file = event.target.files?.[0];
            if (!file) return;
            if (file.size > 1000000) {
              setError('Scenario exceeds 1 MB');
              return;
            }
            void file
              .text()
              .then((value) => {
                scenarioSchema.parse(JSON.parse(value) as unknown);
                setScenario(value);
              })
              .catch((failure: unknown) => setError(failure instanceof Error ? failure.message : 'Invalid scenario'));
          }}
        />
      </Field>
      <div className="flex flex-wrap gap-2">
        <Button
          disabled={running || !canWrite}
          onClick={() =>
            void operate(async (signal) => {
              const input = scenarioSchema.parse(JSON.parse(scenario) as unknown);
              const steps: unknown[] = [];
              await streamDebug('/api/debug/scenario', input, signal, (event, value) => {
                if (event === 'step') {
                  steps.push(value);
                  setOutput([...steps]);
                }
                if (event === 'result') setOutput(value);
              });
            })
          }
        >
          Run scenario
        </Button>
        <Button
          variant="outline"
          disabled={running}
          onClick={() =>
            void operate(() => {
              const input = scenarioSchema.parse(JSON.parse(scenario) as unknown);
              downloadJson('kafka-scenario.json', input);
            })
          }
        >
          Export scenario
        </Button>
      </div>
      <p className="text-sm">
        Run the exported file from your terminal with <code>kafka-studio --scenario kafka-scenario.json</code>.
      </p>
    </section>
  );
}
