import { useEffect, useRef, useState } from 'react';
import type { KeyboardEvent } from 'react';
import { useQuery } from '@tanstack/react-query';
import { createLazyRoute } from '@tanstack/react-router';
import { Check, CircleAlert, Copy, LoaderCircle, Play, Terminal, Trash2 } from 'lucide-react';
import { PageLayout } from '../components/layout/page';
import { Button } from '../components/ui/button';
import { ErrorState } from '../components/ui/error-state';
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
  const transcriptRef = useRef<HTMLDivElement>(null);
  const followOutput = useRef(true);
  const running = useRef(false);
  const nextId = useRef(0);
  const [pendingCommand, setPendingCommand] = useState<string | null>(null);
  const pending = pendingCommand !== null;
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
    const list = suggestionsRef.current;
    const option = list?.querySelector<HTMLElement>('[aria-selected="true"]');
    if (list === null || option === undefined || option === null) return;
    // Scroll only the menu: scrollIntoView can also move the transcript or the whole page.
    if (option.offsetTop < list.scrollTop) list.scrollTop = option.offsetTop;
    else if (option.offsetTop + option.offsetHeight > list.scrollTop + list.clientHeight) {
      list.scrollTop = option.offsetTop + option.offsetHeight - list.clientHeight;
    }
  }, [activeIndex, line, cursor, suggestionsOpen]);

  useEffect(() => {
    const transcript = transcriptRef.current;
    if (transcript !== null && followOutput.current) transcript.scrollTop = transcript.scrollHeight;
  }, [results, pendingCommand]);

  function clearOutput(): void {
    setResults([]);
    setStatus('Terminal cleared.');
    followOutput.current = true;
  }

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
    setPendingCommand(command);
    followOutput.current = true;
    updateLine('');
    setSuggestionsOpen(false);
    setHistory((previous) => [command, ...previous.filter((entry) => entry !== command)].slice(0, 50));
    setHistoryIndex(-1);
    draft.current = '';
    setStatus(`Running ${command}…`);
    const started = performance.now();
    let output: string;
    let failed = false;
    try {
      const response = await runConsoleCommand(command);
      output = typeof response === 'string' ? response : (JSON.stringify(response, null, 2) ?? 'null');
    } catch (error) {
      failed = true;
      output = error instanceof Error ? error.message : String(error);
    }
    const durationMs = Math.round(performance.now() - started);
    const result = { id: nextId.current++, command, output, failed, durationMs };
    setResults((previous) => [...previous, result].slice(-20));
    setStatus(`${command} ${failed ? 'failed' : 'completed'} in ${String(durationMs)} ms.`);
    running.current = false;
    setPendingCommand(null);
  }

  function onKeyDown(event: KeyboardEvent<HTMLInputElement>): void {
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'l') {
      event.preventDefault();
      clearOutput();
      setSuggestionsOpen(false);
      return;
    }
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

  const resourceWarning =
    topics.isError || groups.isError
      ? 'Live name suggestions unavailable. Enter a topic or group name manually.'
      : undefined;
  const hint =
    resourceWarning ?? (commandHelp === undefined ? 'Type help to explore commands.' : `Usage: ${commandHelp.usage}`);

  return (
    <section
      aria-label="Kafka terminal"
      className="flex h-[clamp(24rem,calc(100dvh-9rem),52rem)] min-w-0 flex-col overflow-hidden rounded-xl border border-border bg-background"
    >
      <div className="flex h-12 shrink-0 items-center justify-between gap-3 border-b border-border bg-muted/20 px-4">
        <div className="flex min-w-0 items-center gap-2 text-sm">
          <Terminal className="size-4 shrink-0 text-primary" aria-hidden="true" />
          <h2 className="shrink-0 font-medium">Kafka console</h2>
          <span className="truncate text-muted-foreground" title={`Connection: ${profile ?? 'Direct connection'}`}>
            {profile ?? 'Direct connection'}
          </span>
        </div>
        <div className="flex shrink-0 items-center gap-1">
          <span className="mr-2 hidden text-xs text-muted-foreground sm:inline">Read-only</span>
          <TerminalCopyButton
            label="Copy terminal transcript"
            disabled={results.length === 0}
            getText={() => results.map((result) => `> ${result.command}\n${result.output}`).join('\n\n')}
          />
          <Button
            variant="ghost"
            size="icon-sm"
            aria-label="Clear terminal output"
            title="Clear output (Ctrl+L)"
            disabled={results.length === 0}
            onClick={clearOutput}
          >
            <Trash2 aria-hidden="true" />
          </Button>
        </div>
      </div>
      <div
        ref={transcriptRef}
        role="region"
        aria-label="Terminal transcript"
        tabIndex={0}
        onScroll={(event) => {
          const transcript = event.currentTarget;
          followOutput.current = transcript.scrollHeight - transcript.scrollTop - transcript.clientHeight < 24;
        }}
        className="min-h-0 flex-1 overflow-y-auto p-4 font-mono text-sm leading-6 outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring"
      >
        <div className="mb-6 text-muted-foreground">
          <p className="font-semibold text-foreground">Kafka Studio</p>
          <p>Read-only console · {profile ?? 'Direct connection'}</p>
          <p>
            Run <code className="text-foreground">help</code> for commands. Tab completes commands and names.
          </p>
        </div>
        <div className="flex flex-col gap-6">
          {results.map((result) => (
            <TerminalEntry key={result.id} result={result} />
          ))}
          {pendingCommand !== null && (
            <div aria-busy="true">
              <p className="break-all">
                <span className="mr-2 text-primary" aria-hidden="true">
                  ❯
                </span>
                {pendingCommand}
              </p>
              <p className="mt-1 flex items-center gap-2 text-muted-foreground">
                <LoaderCircle className="size-3.5 animate-spin motion-reduce:animate-none" aria-hidden="true" />
                Running…
              </p>
            </div>
          )}
        </div>
      </div>
      <div className="relative shrink-0 border-t border-border">
        <div
          id="console-suggestions"
          ref={suggestionsRef}
          role="listbox"
          aria-label="Command suggestions"
          hidden={suggestions.length === 0}
          className="absolute right-3 bottom-full left-3 z-20 mb-2 max-h-52 overflow-y-auto rounded-lg border border-border bg-popover p-1 text-popover-foreground shadow-xl"
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
                'flex cursor-pointer items-center justify-between gap-4 rounded-md px-3 py-2 text-sm',
                index === activeIndex ? 'bg-accent text-accent-foreground' : 'hover:bg-muted/50',
              )}
            >
              <span className="min-w-0 font-mono whitespace-normal break-all">{suggestion.label}</span>
              <span className="hidden min-w-0 truncate text-xs text-muted-foreground sm:block">
                {suggestion.description}
              </span>
            </div>
          ))}
        </div>
        <form
          onSubmit={(event) => {
            event.preventDefault();
            void execute();
          }}
          className="flex h-14 items-center gap-3 px-4"
        >
          <span aria-hidden="true" className="font-mono text-primary">
            ❯
          </span>
          <label className="sr-only" htmlFor="console-command">
            Kafka command
          </label>
          <input
            ref={inputRef}
            id="console-command"
            role="combobox"
            aria-autocomplete="list"
            aria-expanded={suggestions.length > 0}
            aria-controls="console-suggestions"
            aria-activedescendant={selected === undefined ? undefined : `console-suggestion-${String(activeIndex)}`}
            aria-describedby="console-shortcuts console-hint"
            autoComplete="off"
            autoCapitalize="off"
            spellCheck={false}
            readOnly={pending}
            placeholder={pending ? 'Waiting for Kafka…' : 'Enter a command…'}
            className="h-9 min-w-0 flex-1 rounded-sm bg-transparent px-1 font-mono text-sm outline-none placeholder:text-muted-foreground focus-visible:ring-2 focus-visible:ring-ring"
            value={line}
            onChange={(event) => {
              updateLine(event.target.value, event.target.selectionStart ?? event.target.value.length);
              setHistoryIndex(-1);
              setSuggestionsOpen(event.target.value.trim().length > 0);
            }}
            onSelect={(event) => {
              setCursor(event.currentTarget.selectionStart ?? line.length);
              setActiveIndex(0);
            }}
            onFocus={() => setSuggestionsOpen(line.trim().length > 0 && !pending)}
            onBlur={() => setSuggestionsOpen(false)}
            onKeyDown={onKeyDown}
          />
          <Button
            type="submit"
            variant="ghost"
            size="icon-sm"
            aria-label="Run command"
            title="Run (Enter)"
            disabled={pending || line.trim() === ''}
          >
            <Play aria-hidden="true" />
          </Button>
        </form>
      </div>
      <div className="flex h-9 shrink-0 items-center justify-between gap-4 border-t border-border bg-muted/20 px-4 text-xs text-muted-foreground">
        <p id="console-hint" className="min-w-0 truncate" title={hint}>
          {hint}
        </p>
        <span aria-hidden="true" className="hidden shrink-0 sm:inline">
          Tab complete · ↑↓ history · Ctrl+L clear
        </span>
      </div>
      <p id="console-shortcuts" className="sr-only">
        Enter runs. Tab accepts a suggestion. Escape dismisses suggestions, then arrows recall history. Escape then Tab,
        or Shift+Tab, leaves the prompt. Control+L clears output.
      </p>
      <p role="status" aria-live="polite" className="sr-only">
        {status}
      </p>
    </section>
  );
}

function TerminalEntry({ result }: { readonly result: ConsoleResult }) {
  return (
    <div className="relative min-w-0 pr-9">
      <details open>
        <summary className="cursor-pointer rounded-sm break-all outline-none focus-visible:ring-2 focus-visible:ring-ring">
          <span aria-hidden="true" className="mr-2 text-primary">
            ❯
          </span>
          <span>{result.command}</span>
          <span className={cn('ml-3 text-xs', result.failed ? 'text-destructive' : 'text-muted-foreground')}>
            {result.failed ? 'Failed' : 'Completed'} · {result.durationMs} ms
          </span>
        </summary>
        <pre
          aria-label={`Output for ${result.command}`}
          className={cn('mt-2 whitespace-pre-wrap break-all text-xs leading-6', result.failed && 'text-destructive')}
        >
          <code>{result.output}</code>
        </pre>
      </details>
      <TerminalCopyButton
        label={`Copy output for ${result.command}`}
        className="absolute top-0 right-0"
        getText={() => result.output}
      />
    </div>
  );
}

function TerminalCopyButton({
  label,
  getText,
  disabled = false,
  className,
}: {
  readonly label: string;
  readonly getText: () => string;
  readonly disabled?: boolean;
  readonly className?: string;
}) {
  const [status, setStatus] = useState('');
  async function copy(): Promise<void> {
    try {
      await navigator.clipboard.writeText(getText());
      setStatus('Copied.');
    } catch {
      setStatus('Could not copy.');
    }
  }
  let Icon = Copy;
  if (status === 'Copied.') Icon = Check;
  else if (status !== '') Icon = CircleAlert;
  return (
    <>
      <Button
        variant="ghost"
        size="icon-sm"
        className={className}
        aria-label={label}
        title={status || label}
        disabled={disabled}
        onClick={() => void copy()}
      >
        <Icon aria-hidden="true" />
      </Button>
      <span className="sr-only" aria-live="polite">
        {status}
      </span>
    </>
  );
}
