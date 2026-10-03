#!/usr/bin/env node
import process from 'node:process';
import { main } from './main';
import { createRuntime } from './runtime';
import { unsupportedRuntimeMessage } from './runtime-version';

const unsupported = unsupportedRuntimeMessage(process.versions);
if (unsupported !== undefined) {
  process.stderr.write(`${unsupported}\n`);
  process.exitCode = 70;
} else {
  const runtime = createRuntime(process);
  process.exitCode = await main(runtime);
}
