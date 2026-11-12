// Host-side entry point: `node runner/cli.ts <doctor|probe|run|report|publish|typecheck> [flags]`.

import { randomBytes } from 'node:crypto';
import { clientIds } from '../client/spec.ts';
import type { ClientId, RuntimeId } from '../client/spec.ts';
import { aggregateSession } from './aggregate.ts';
import {
  parseDoctorArgs,
  parseProbeArgs,
  parsePublishArgs,
  parseReportArgs,
  parseRunArgs,
  parseTypecheckArgs,
  usage,
  UsageError,
} from './args.ts';
import type { DoctorArgs, EnvironmentArgs, PublishArgs, ReportArgs, RunArgs, TypecheckArgs } from './args.ts';
import { deleteTopic, startBroker, stopBroker } from './broker.ts';
import { clientContainerName, removeContainer } from './container.ts';
import { resolveCoreSource } from './core-source.ts';
import { hasFailedChecks, runDoctor } from './doctor.ts';
import { measureFootprints } from './footprint.ts';
import { buildImages, typecheckInImage } from './images.ts';
import { acquireSerialLock, signalExitCodes } from './lock.ts';
import type { HandledSignal, SerialLock } from './lock.ts';
import { docker, errorMessage } from './process.ts';
import { probeSupport } from './probe.ts';
import { publishSession } from './publish.ts';
import { writeReport } from './report.ts';
import { executeSchedule, warmUpBroker } from './run.ts';
import type { ActiveRun, RunContext } from './run.ts';
import { buildSchedule, seriesFor } from './schedule.ts';
import {
  createSessionDir,
  createSessionId,
  latestSessionDir,
  probeCacheDir,
  publishedResultsDir,
  repoRoot,
  sessionPaths,
  writeJson,
} from './session.ts';
import type {
  BrokerHandle,
  CoreSource,
  Footprint,
  ResourceBudget,
  RuntimeImage,
  SessionManifest,
  SupportMatrix,
} from './types.ts';

function log(line: string): void {
  const time = new Date().toISOString().slice(11, 19);
  process.stdout.write(`[${time}] ${line}\n`);
}

interface ShutdownState {
  lock: SerialLock | null;
  broker: BrokerHandle | null;
  keepBroker: boolean;
  activeRun: ActiveRun | null;
  isShuttingDown: boolean;
}

const shutdown: ShutdownState = { lock: null, broker: null, keepBroker: false, activeRun: null, isShuttingDown: false };

async function cleanUp(): Promise<void> {
  const { activeRun, broker } = shutdown;
  shutdown.activeRun = null;
  if (activeRun !== null) {
    await removeContainer(clientContainerName(activeRun.runId));
    if (broker !== null) {
      await deleteTopic(broker, activeRun.topic).catch((error: unknown) => {
        log(`cleanup: cannot delete topic ${activeRun.topic}: ${errorMessage(error)}`);
      });
    }
  }
  if (broker !== null && !shutdown.keepBroker) {
    shutdown.broker = null;
    log('stopping the broker');
    await stopBroker(broker).catch((error: unknown) => log(`cleanup: cannot stop the broker: ${errorMessage(error)}`));
  }
}

async function onSignal(signal: HandledSignal): Promise<void> {
  if (shutdown.isShuttingDown) {
    log(`${signal} again; exiting without further cleanup`);
    shutdown.lock?.release();
    process.exit(signalExitCodes[signal]);
  }
  shutdown.isShuttingDown = true;
  log(`${signal} received; cleaning up (press Ctrl+C again to exit immediately)`);
  try {
    await cleanUp();
  } finally {
    shutdown.lock?.release();
    process.exit(signalExitCodes[signal]);
  }
}

function installSignalHandlers(): void {
  for (const signal of Object.keys(signalExitCodes) as HandledSignal[]) {
    process.on(signal, () => void onSignal(signal));
  }
}

async function takeLock(): Promise<SerialLock> {
  const lock = await acquireSerialLock({ handleSignals: false, log });
  shutdown.lock = lock;
  return lock;
}

function formatSupportMatrix(support: SupportMatrix, clients: readonly ClientId[], runtimes: readonly RuntimeId[]) {
  const clientWidth = Math.max(6, ...clients.map((client) => client.length));
  const lines = [`${'client'.padEnd(clientWidth)}  ${runtimes.map((runtime) => runtime.padEnd(8)).join('  ')}`];
  const reasons: string[] = [];
  for (const client of clients) {
    const marks = runtimes.map((runtime) => {
      const cell = support.cells.find((candidate) => candidate.client === client && candidate.runtime === runtime);
      if (cell === undefined) {
        return 'n/a'.padEnd(8);
      }
      if (!cell.supported) {
        reasons.push(`  ${client} on ${runtime}: ${cell.reason}`);
      }
      return (cell.supported ? 'yes' : 'NO').padEnd(8);
    });
    lines.push(`${client.padEnd(clientWidth)}  ${marks.join('  ')}`);
  }
  if (reasons.length > 0) {
    lines.push('Unsupported:', ...reasons);
  }
  return lines.join('\n');
}

async function removeStrayClientContainers(): Promise<void> {
  const stdout = await docker(['ps', '--all', '--filter', 'name=^kafka-bench-client-', '--format', '{{.Names}}'], {
    timeoutMs: 30_000,
  });
  for (const name of stdout.split('\n').filter((line) => line.trim() !== '')) {
    log(`removing stray client container ${name}`);
    await removeContainer(name.trim());
  }
}

/** One footprint per client: the first image where the package is installed wins. */
async function measureAllFootprints(images: readonly RuntimeImage[]): Promise<Footprint[]> {
  const byClient = new Map<ClientId, Footprint>();
  for (const image of images) {
    let footprints: Footprint[];
    try {
      footprints = await measureFootprints(image);
    } catch (error) {
      log(`footprint on ${image.runtime} failed: ${errorMessage(error)}`);
      continue;
    }
    for (const footprint of footprints) {
      const known = byClient.get(footprint.client);
      if (known === undefined || (known.version === null && footprint.version !== null)) {
        byClient.set(footprint.client, footprint);
      }
    }
  }
  return clientIds.flatMap((client) => byClient.get(client) ?? []);
}

interface PreparedEnvironment {
  budget: ResourceBudget;
  core: CoreSource;
  sessionDir: string;
  sessionId: string;
  images: RuntimeImage[];
  broker: BrokerHandle;
  support: SupportMatrix;
}

/** doctor, core source, session directory, images, broker and probe: shared by `probe` and `run`. */
async function prepareEnvironment(options: EnvironmentArgs): Promise<PreparedEnvironment | null> {
  shutdown.keepBroker = options.keepBroker;
  await takeLock();
  const environment = await runDoctor({ force: options.force });
  if (hasFailedChecks(environment)) {
    return null;
  }
  log(`resolving kafka-core source ${options.coreRef}`);
  const core = await resolveCoreSource(options.coreRef, repoRoot);
  const sessionId = createSessionId(core.sha);
  const paths = await createSessionDir(sessionId);
  await writeJson(paths.environment, environment);
  log(`session ${sessionId} (core ${core.sha.slice(0, 12)}${core.isDirty ? ', dirty worktree' : ''}) in ${paths.dir}`);

  await removeStrayClientContainers();
  const images = await buildImages({ core, runtimes: options.runtimes, repoRoot, log });
  log('starting the broker');
  const broker = await startBroker(environment.budget);
  shutdown.broker = broker;
  log(`broker ${broker.image} (${broker.imageDigest ?? 'no digest'}) is ready`);

  const support = await probeSupport({
    images,
    clients: options.clients,
    broker,
    budget: environment.budget,
    cacheDir: probeCacheDir,
    log,
  });
  await writeJson(paths.supportMatrix, support);
  process.stdout.write(`${formatSupportMatrix(support, options.clients, options.runtimes)}\n`);
  return { budget: environment.budget, core, sessionDir: paths.dir, sessionId, images, broker, support };
}

async function commandDoctor(options: DoctorArgs): Promise<number> {
  await takeLock();
  const report = await runDoctor({ force: options.force });
  return hasFailedChecks(report) ? 1 : 0;
}

async function commandProbe(options: EnvironmentArgs): Promise<number> {
  const prepared = await prepareEnvironment(options);
  if (prepared === null) {
    return 1;
  }
  log(`support matrix written to ${sessionPaths(prepared.sessionDir).supportMatrix}`);
  return 0;
}

async function commandRun(options: RunArgs): Promise<number> {
  const prepared = await prepareEnvironment(options);
  if (prepared === null) {
    return 1;
  }
  const { broker, budget, images, support, sessionDir } = prepared;
  const missingRuntimes = options.runtimes.filter((runtime) => !images.some((image) => image.runtime === runtime));
  if (missingRuntimes.length > 0) {
    throw new Error(`No image was built for ${missingRuntimes.join(', ')}`);
  }
  const context: RunContext = {
    broker,
    budget,
    imageByRuntime: new Map(images.map((image) => [image.runtime, image])),
    log,
    setActiveRun: (run) => {
      shutdown.activeRun = run;
    },
    isStopping: () => shutdown.isShuttingDown,
  };
  const paths = sessionPaths(sessionDir);
  await writeJson(paths.footprint, await measureAllFootprints(images));

  const runTag = randomBytes(3).toString('hex');
  await warmUpBroker(context, support, { runtimes: options.runtimes, payload: options.payload, runTag });

  const specs = buildSchedule({
    tier: options.tier,
    scenarios: options.scenarios,
    clients: options.clients,
    runtimes: options.runtimes,
    profiles: options.profiles,
    isAaTest: options.isAaTest,
    repetitions: options.repetitions,
    seed: options.seed,
    support,
    payload: options.payload,
    countScale: options.countScale,
    brokers: [broker.bootstrap],
    runTag,
  });
  if (specs.length === 0) {
    throw new Error('Nothing to run: every selected cell is unsupported or excluded');
  }
  const manifest: SessionManifest = {
    sessionId: prepared.sessionId,
    tier: options.tier,
    seed: options.seed,
    payload: options.payload,
    countScale: options.countScale,
    startedAt: new Date().toISOString(),
    finishedAt: null,
    repetitions: options.repetitions,
    clients: options.clients,
    runtimes: options.runtimes,
    scenarios: options.scenarios,
    profiles: options.profiles,
    series: seriesFor(options.clients, options.isAaTest).map((series) => series.label),
    core: prepared.core,
    images,
    broker: { image: broker.image, digest: broker.imageDigest },
    plannedRuns: specs.length,
  };
  await writeJson(paths.session, manifest);
  log(
    `${specs.length} runs planned (tier ${options.tier}, seed ${options.seed}, ${options.repetitions} repetition(s))`,
  );

  const { failedRuns } = await executeSchedule(specs, context, sessionDir);
  await writeJson(paths.session, { ...manifest, finishedAt: new Date().toISOString() });
  log(`${specs.length - failedRuns}/${specs.length} runs passed verification; aggregating`);
  const summary = await aggregateSession(sessionDir);
  log(`report: ${await writeReport(sessionDir, summary)}`);
  return failedRuns === 0 ? 0 : 1;
}

async function resolveSessionDir(session: string | undefined): Promise<string> {
  if (session !== undefined) {
    return session;
  }
  const latest = await latestSessionDir();
  if (latest === null) {
    throw new UsageError('No session found under results/raw/; pass --session <dir>');
  }
  return latest;
}

async function commandReport(options: ReportArgs): Promise<number> {
  const sessionDir = await resolveSessionDir(options.session);
  const summary = await aggregateSession(sessionDir, options.noise);
  log(`report: ${await writeReport(sessionDir, summary)}`);
  return 0;
}

async function commandPublish(options: PublishArgs): Promise<number> {
  const sessionDir = await resolveSessionDir(options.session);
  const publishedPath = await publishSession(sessionDir, {
    publishedDir: publishedResultsDir,
    repoRoot,
    minRepetitions: options.minRepetitions,
    ...options.noise,
  });
  log(`published: ${publishedPath}`);
  return 0;
}

async function commandTypecheck(options: TypecheckArgs): Promise<number> {
  const core = await resolveCoreSource(options.coreRef, repoRoot);
  const result = await typecheckInImage({ core, repoRoot, log });
  if (!result.passed) {
    process.stderr.write(`${result.output}\n`);
    return 1;
  }
  log('typecheck passed');
  return 0;
}

type Command = (args: string[]) => Promise<number>;

/** Parses the flags, prints the usage for `--help`, and otherwise runs the command. */
function command<Options>(parse: (args: string[]) => Options | null, execute: (options: Options) => Promise<number>) {
  return async (args: string[]): Promise<number> => {
    const options = parse(args);
    if (options === null) {
      process.stdout.write(usage);
      return 0;
    }
    return execute(options);
  };
}

const commands: Readonly<Record<string, Command>> = {
  doctor: command(parseDoctorArgs, commandDoctor),
  probe: command(parseProbeArgs, commandProbe),
  run: command(parseRunArgs, commandRun),
  report: command(parseReportArgs, commandReport),
  publish: command(parsePublishArgs, commandPublish),
  typecheck: command(parseTypecheckArgs, commandTypecheck),
};

async function main(argv: string[]): Promise<number> {
  const [commandName, ...args] = argv;
  if (commandName === undefined || commandName === '--help' || commandName === '-h' || commandName === 'help') {
    process.stdout.write(usage);
    return commandName === undefined ? 2 : 0;
  }
  const selectedCommand = commands[commandName];
  if (selectedCommand === undefined) {
    throw new UsageError(`Unknown command "${commandName}"`);
  }
  installSignalHandlers();
  try {
    return await selectedCommand(args);
  } finally {
    if (!shutdown.isShuttingDown) {
      await cleanUp();
      shutdown.lock?.release();
    }
  }
}

try {
  process.exitCode = await main(process.argv.slice(2));
} catch (error) {
  const code = (error as { code?: unknown }).code;
  const isUsageError = error instanceof UsageError || (typeof code === 'string' && code.startsWith('ERR_PARSE_ARGS_'));
  process.stderr.write(`error: ${errorMessage(error)}\n`);
  if (isUsageError) {
    process.stderr.write('Run `node runner/cli.ts --help` for usage.\n');
  }
  process.exitCode = isUsageError ? 2 : 1;
}
