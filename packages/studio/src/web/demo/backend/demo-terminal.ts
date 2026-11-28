import { terminalArgv } from '../../../shared/terminal-input';
import type { DemoCluster } from './demo-cluster';
import type { StreamEmit } from './fake-event-source';
import { DEMO_PROFILE } from './sample-data';

const GREEN = '\x1b[32m';
const DIM = '\x1b[2m';
const RED = '\x1b[31m';
const RESET = '\x1b[0m';
const ESCAPE = '\x1b';
const CLEAR_SCREEN = '\x1b[2J\x1b[3J\x1b[H';
const PROMPT = `${GREEN}kafka@${DEMO_PROFILE}${RESET}:~$ `;

interface Session {
  line: string;
  /** The attached event stream, while a client is connected. */
  stream: { readonly emit: StreamEmit; readonly end: () => void } | null;
}

interface DemoCommand {
  readonly description: string;
  /** Returns the text to print; `null` closes the session. */
  run(cluster: DemoCluster): string | null;
}

/** The shell commands the demo answers, keyed by their words with a leading `kafka` dropped. */
const COMMANDS: Readonly<Record<string, DemoCommand>> = {
  'topic list': {
    description: 'list topics',
    run: (cluster) =>
      cluster
        .listTopics()
        .topics.map((topic) => `${topic.name}\n`)
        .join(''),
  },
  'group list': {
    description: 'list consumer groups',
    run: (cluster) =>
      cluster
        .listGroups()
        .groups.map((group) => `${group.groupId}\n`)
        .join(''),
  },
  clear: { description: 'clear the screen', run: () => CLEAR_SCREEN },
  help: {
    description: 'show these commands',
    run: () =>
      [
        'Commands in this demo:',
        ...Object.entries(COMMANDS).map(
          ([name, command]) => `  ${GREEN}${name.padEnd(12)}${RESET} ${command.description}`,
        ),
        '',
        `${DIM}An installed Studio runs every kafka-cli command here, with completion and history.${RESET}`,
        '',
      ].join('\n'),
  },
  exit: { description: 'close the session', run: () => null },
};

function crlf(text: string): string {
  return text.replace(/\r?\n/g, '\r\n');
}

function notInDemo(command: string): string {
  return `${RED}"${command}" isn't available in the browser demo.${RESET}\nInstall Studio to run it against your own cluster. Type help for the demo commands.\n`;
}

/**
 * A small line editor standing in for the Studio shell. The real shell runs `kafka-cli` on the server;
 * the demo answers a few read-only commands from the sample cluster and explains that the rest need
 * an installed Studio.
 */
export class DemoTerminals {
  private readonly sessions = new Map<string, Session>();
  private readonly cluster: DemoCluster;
  private nextId = 1;

  constructor(cluster: DemoCluster) {
    this.cluster = cluster;
  }

  create(): string {
    const id = `demo-shell-${String(this.nextId)}`;
    this.nextId++;
    this.sessions.set(id, { line: '', stream: null });
    return id;
  }

  close(id: string): void {
    this.sessions.delete(id);
  }

  attach(id: string, emit: StreamEmit, end: () => void): (() => void) | null {
    const session = this.sessions.get(id);
    if (session === undefined) return null;
    session.stream = { emit, end };
    this.write(
      session,
      `Kafka Studio shell · browser demo\nType help for the commands this demo answers; clear clears the screen; exit closes the session.\n\n${PROMPT}`,
    );
    return () => {
      session.stream = null;
    };
  }

  /** `false` when the session no longer exists, which the API reports as a 404 like the real server. */
  input(id: string, data: string): boolean {
    const session = this.sessions.get(id);
    if (session === undefined) return false;
    // Arrow keys, Home/End and other escape sequences (ESC [ … final or ESC O final) have no
    // history or cursor to move here, so they are skipped whole.
    let escape: 'none' | 'introducer' | 'body' = 'none';
    for (const char of data) {
      if (char === ESCAPE) escape = 'introducer';
      else if (escape === 'introducer') escape = 'body';
      else if (escape === 'body') {
        if (/[A-Za-z~]/.test(char)) escape = 'none';
      } else if (char === '\r' || char === '\n') {
        const line = session.line;
        session.line = '';
        this.write(session, '\n');
        this.run(id, session, line);
      } else if (char === '\x7f' || char === '\b') {
        if (session.line.length === 0) continue;
        session.line = session.line.slice(0, -1);
        this.write(session, '\b \b');
      } else if (char === '\x03') {
        session.line = '';
        this.write(session, `^C\n${PROMPT}`);
      } else if (char === '\x0c') {
        session.line = '';
        this.write(session, `${CLEAR_SCREEN}${PROMPT}`);
      } else if (char >= ' ') {
        session.line += char;
        this.write(session, char);
      }
    }
    return true;
  }

  private run(id: string, session: Session, line: string): void {
    let words: string[];
    try {
      words = terminalArgv(line);
    } catch (error) {
      this.write(session, `${RED}${error instanceof Error ? error.message : 'Invalid input'}${RESET}\n${PROMPT}`);
      return;
    }
    if (words.length === 0) {
      this.write(session, PROMPT);
      return;
    }
    const name = words.join(' ');
    const command = Object.hasOwn(COMMANDS, name) ? COMMANDS[name] : undefined;
    const output = command === undefined ? notInDemo(name) : command.run(this.cluster);
    if (output === null) {
      session.stream?.emit('exit', {});
      session.stream?.end();
      this.close(id);
      return;
    }
    this.write(session, `${output}${PROMPT}`);
  }

  private write(session: Session, text: string): void {
    session.stream?.emit('output', { data: crlf(text) });
  }
}
