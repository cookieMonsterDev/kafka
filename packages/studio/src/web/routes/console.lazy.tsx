import { useEffect, useRef, useState } from 'react';
import type { KeyboardEvent } from 'react';
import { useQuery } from '@tanstack/react-query';
import { createLazyRoute } from '@tanstack/react-router';
import { Play, Terminal } from 'lucide-react';
import { PageLayout } from '../components/layout/page';
import { Badge } from '../components/ui/badge';
import { Button } from '../components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/card';
import { ErrorState } from '../components/ui/error-state';
import { Input } from '../components/ui/input';
import { Skeleton } from '../components/ui/skeleton';
import { runConsoleCommand } from '../lib/console-api';
import {
  acceptConsoleSuggestion,
  CONSOLE_COMMANDS,
  suggestConsoleCommands,
  type ConsoleSuggestion,
} from '../lib/console-commands';
import { listGroups } from '../lib/groups-api';
import { fetchProfiles, profilesQueryKey } from '../lib/profiles-api';
import { listTopics } from '../lib/topics-api';
import { cn } from '../lib/utils';

export const Route = createLazyRoute('/console')({ component: ConsolePage });

interface ConsoleResult {
  readonly id: number;
  readonly command: string;
  readonly output: string;
  readonly failed: boolean;
  readonly durationMs: number;
}

function ConsolePage() {
  const profiles = useQuery({ queryKey: profilesQueryKey, queryFn: fetchProfiles });
  return (
    <PageLayout>
      {profiles.isPending && <Skeleton className="h-48 w-full" aria-label="Loading connection profile" />}
      {profiles.isError && (
        <ErrorState
          title="Could not load the connection profile"
          error={profiles.error}
          onRetry={() => void profiles.refetch()}
        />
      )}
      {profiles.data !== undefined && (
        <ConsoleSession key={profiles.data.active ?? ''} profile={profiles.data.active} />
      )}
    </PageLayout>
  );
}

function ConsoleSession({ profile }: { readonly profile: string | null }) {
  const [line, setLine] = useState('');
  const [cursor, setCursor] = useState(0);
  const [suggestionsOpen, setSuggestionsOpen] = useState(false);
  const [activeIndex, setActiveIndex] = useState(0);
  const [history, setHistory] = useState<string[]>([]);
  const [historyIndex, setHistoryIndex] = useState(-1);
  const draft = useRef('');
  const inputRef = useRef<HTMLInputElement>(null);
  const suggestionsRef = useRef<HTMLDivElement>(null);
  const running = useRef(false);
  const nextId = useRef(0);
  const [pending, setPending] = useState(false);
  const [results, setResults] = useState<ConsoleResult[]>([]);
  const [status, setStatus] = useState('Ready. Run help to see available commands.');
  const topics = useQuery({ queryKey: ['console', profile, 'topics'], queryFn: listTopics });
  const groups = useQuery({ queryKey: ['console', profile, 'groups'], queryFn: listGroups });
  const suggestions = suggestionsOpen
    ? suggestConsoleCommands(line, cursor, {
        topics: topics.data?.topics.map((topic) => topic.name) ?? [],
        groups: groups.data?.groups.map((group) => group.groupId) ?? [],
      })
    : [];
  const commandText = line.trimStart().replace(/^kafka\s+/, '');
  const commandHelp = CONSOLE_COMMANDS.find(
    (command) => commandText === command.path || commandText.startsWith(`${command.path} `),
  );
  const selected = suggestions[activeIndex];

  useEffect(() => {
    suggestionsRef.current?.querySelector('[aria-selected="true"]')?.scrollIntoView({ block: 'nearest' });
  }, [activeIndex, line, cursor, suggestionsOpen]);

  function updateLine(value: string, position = value.length): void {
    setLine(value);
    setCursor(position);
    setActiveIndex(0);
  }

  function accept(suggestion: ConsoleSuggestion): void {
    const replacement = acceptConsoleSuggestion(line, suggestion);
    updateLine(replacement.line, replacement.cursor);
    setSuggestionsOpen(false);
    inputRef.current?.focus();
    requestAnimationFrame(() => inputRef.current?.setSelectionRange(replacement.cursor, replacement.cursor));
  }

  async function execute(): Promise<void> {
    const command = line.trim();
    if (command === '' || running.current) return;
    running.current = true;
    setPending(true);
    setSuggestionsOpen(false);
    setHistory((previous) => [command, ...previous.filter((entry) => entry !== command)].slice(0, 50));
    setHistoryIndex(-1);
    draft.current = '';
    setStatus(`Running ${command}…`);
    const started = performance.now();
    let output: string;
    let failed = false;
    try {
      output = JSON.stringify(await runConsoleCommand(command), null, 2) ?? 'null';
    } catch (error) {
      failed = true;
      output = error instanceof Error ? error.message : String(error);
    }
    const durationMs = Math.round(performance.now() - started);
    const result = { id: nextId.current++, command, output, failed, durationMs };
    setResults((previous) => [result, ...previous].slice(0, 20));
    setStatus(`${command} ${failed ? 'failed' : 'completed'} in ${String(durationMs)} ms.`);
    updateLine('');
    running.current = false;
    setPending(false);
  }

  function onKeyDown(event: KeyboardEvent<HTMLInputElement>): void {
    if (pending) return;
    if (event.key === 'Escape') {
      setSuggestionsOpen(false);
    } else if (event.key === 'Tab' && !event.shiftKey && selected !== undefined) {
      event.preventDefault();
      accept(selected);
    } else if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
      event.preventDefault();
      const direction = event.key === 'ArrowDown' ? 1 : -1;
      if (suggestions.length > 0) {
        setActiveIndex((index) => Math.max(0, Math.min(index + direction, suggestions.length - 1)));
      } else {
        if (historyIndex === -1) draft.current = line;
        const index = Math.max(-1, Math.min(historyIndex - direction, history.length - 1));
        setHistoryIndex(index);
        updateLine(index === -1 ? draft.current : (history[index] ?? ''));
        setSuggestionsOpen(false);
      }
    }
  }

  return (
    <div className="flex flex-col gap-4">
      <div className="flex flex-wrap items-center gap-2 text-sm">
        <Terminal className="size-4 text-primary" aria-hidden="true" />
        <span>
          Connection: <strong>{profile ?? 'Direct connection'}</strong>
        </span>
        <Badge variant="outline">Read-only commands</Badge>
      </div>
      <Card>
        <CardHeader>
          <CardTitle>Kafka console</CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-3">
          <p className="text-sm text-muted-foreground">
            Inspect Kafka with CLI-style commands using the active Studio connection.
          </p>
          <form
            onSubmit={(event) => {
              event.preventDefault();
              void execute();
            }}
            className="flex flex-wrap gap-2"
          >
            <label className="sr-only" htmlFor="console-command">
              Kafka command
            </label>
            <Input
              ref={inputRef}
              id="console-command"
              role="combobox"
              aria-autocomplete="list"
              aria-expanded={suggestions.length > 0}
              aria-controls="console-suggestions"
              aria-activedescendant={selected === undefined ? undefined : `console-suggestion-${String(activeIndex)}`}
              aria-describedby="console-shortcuts"
              autoComplete="off"
              autoCapitalize="off"
              spellCheck={false}
              readOnly={pending}
              placeholder="topic describe orders"
              className="h-10 min-w-48 flex-1 font-mono"
              value={line}
              onChange={(event) => {
                updateLine(event.target.value, event.target.selectionStart ?? event.target.value.length);
                setHistoryIndex(-1);
                setSuggestionsOpen(true);
              }}
              onSelect={(event) => {
                setCursor(event.currentTarget.selectionStart ?? line.length);
                setActiveIndex(0);
              }}
              onFocus={() => setSuggestionsOpen(true)}
              onBlur={() => setSuggestionsOpen(false)}
              onKeyDown={onKeyDown}
            />
            <Button type="submit" disabled={pending || line.trim() === ''}>
              <Play className="size-4" aria-hidden="true" />
              {pending ? 'Running…' : 'Run'}
            </Button>
          </form>
          <div
            id="console-suggestions"
            ref={suggestionsRef}
            role="listbox"
            aria-label="Command suggestions"
            className={cn('max-h-60 overflow-y-auto', suggestions.length > 0 && 'rounded-lg border border-border p-1')}
          >
            {suggestions.map((suggestion, index) => (
              <div
                key={suggestion.label}
                id={`console-suggestion-${String(index)}`}
                role="option"
                aria-selected={index === activeIndex}
                onMouseDown={(event) => event.preventDefault()}
                onClick={() => accept(suggestion)}
                className={cn(
                  'flex cursor-pointer flex-wrap justify-between gap-x-4 rounded-md px-3 py-2 text-sm',
                  index === activeIndex ? 'bg-accent' : 'hover:bg-muted/50',
                )}
              >
                <span className="break-all font-mono">{suggestion.label}</span>
                <span className="text-muted-foreground">{suggestion.description}</span>
              </div>
            ))}
          </div>
          {commandHelp !== undefined && (
            <p className="text-sm text-muted-foreground">
              Usage: <code>{commandHelp.usage}</code> · {commandHelp.description}
            </p>
          )}
          <p id="console-shortcuts" className="text-xs text-muted-foreground">
            Tab accepts a suggestion. Escape dismisses suggestions, then ↑ / ↓ recalls history. Enter runs. Escape or
            Shift+Tab lets you leave the editor.
          </p>
          {(topics.isError || groups.isError) && (
            <p className="text-sm text-muted-foreground">
              Live resource suggestions are unavailable for {topics.isError ? 'topics' : ''}
              {topics.isError && groups.isError ? ' and ' : ''}
              {groups.isError ? 'groups' : ''}. You can still type a name and run a command.
            </p>
          )}
          <p role="status" aria-live="polite" className="text-sm text-muted-foreground">
            {status}
          </p>
        </CardContent>
      </Card>
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h2 className="text-sm font-semibold">
          Results <span className="text-muted-foreground">({results.length}/20)</span>
        </h2>
        <Button
          variant="outline"
          size="sm"
          disabled={pending || results.length === 0}
          onClick={() => {
            setResults([]);
            setStatus('Results cleared.');
          }}
        >
          Clear results
        </Button>
      </div>
      {results.map((result, index) => (
        <ConsoleResultCard key={result.id} result={result} open={index === 0} />
      ))}
      {results.length === 0 && (
        <Card>
          <CardHeader>
            <CardTitle>Start with a command</CardTitle>
          </CardHeader>
          <CardContent className="flex flex-wrap gap-2">
            {['help', 'cluster info', 'topic list', 'group list'].map((example) => (
              <Button
                key={example}
                variant="outline"
                size="sm"
                onClick={() => {
                  updateLine(example);
                  setSuggestionsOpen(false);
                  inputRef.current?.focus();
                }}
              >
                {example}
              </Button>
            ))}
          </CardContent>
        </Card>
      )}
      <details className="rounded-xl border border-border bg-card p-4">
        <summary className="cursor-pointer rounded-sm text-sm font-medium focus-visible:ring-3 focus-visible:ring-ring/50">
          Supported commands
        </summary>
        <ul className="mt-3 grid gap-3 md:grid-cols-2">
          {CONSOLE_COMMANDS.map((command) => (
            <li key={command.path}>
              <code className="text-sm break-all">{command.usage}</code>
              <p className="text-xs text-muted-foreground">{command.description}</p>
            </li>
          ))}
        </ul>
        <p className="mt-4 text-xs text-muted-foreground">
          An optional kafka prefix and --help are supported. This console supports the listed inspection commands; shell
          scripts, file arguments, connection overrides and mutations are unavailable. History and results stay in
          memory for this page and clear when you leave or switch profiles.
        </p>
      </details>
    </div>
  );
}

function ConsoleResultCard({ result, open }: { readonly result: ConsoleResult; readonly open: boolean }) {
  const [copyStatus, setCopyStatus] = useState('Copy JSON');
  async function copy(): Promise<void> {
    try {
      await navigator.clipboard.writeText(result.output);
      setCopyStatus('Copied');
    } catch {
      setCopyStatus('Copy failed');
    }
  }
  return (
    <details open={open} className="rounded-xl border border-border bg-card p-4">
      <summary className="cursor-pointer rounded-sm text-sm focus-visible:ring-3 focus-visible:ring-ring/50">
        <code className="break-all">{result.command}</code>{' '}
        <span className={result.failed ? 'text-destructive' : 'text-muted-foreground'}>
          · {result.failed ? 'Failed' : 'Completed'} · {result.durationMs} ms
        </span>
      </summary>
      <div className="mt-3 flex justify-end">
        <Button variant="outline" size="sm" onClick={() => void copy()}>
          {copyStatus}
        </Button>
      </div>
      <span className="sr-only" aria-live="polite">
        {copyStatus === 'Copy JSON' ? '' : copyStatus}
      </span>
      <pre
        tabIndex={0}
        aria-label={`Output for ${result.command}`}
        className="mt-2 max-h-96 overflow-auto rounded-lg bg-muted/30 p-3 font-mono text-xs focus-visible:ring-3 focus-visible:ring-ring/50"
      >
        <code>{result.output}</code>
      </pre>
    </details>
  );
}
