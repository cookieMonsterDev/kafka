#!/usr/bin/env node
import process from 'node:process';
import { main } from './main';
import { createRuntime } from './runtime';

const REQUIRED_MAJOR_VERSION = 24;

function checkNodeVersion(versions: NodeJS.ProcessVersions, stderr: NodeJS.WriteStream): boolean {
  const major = Number.parseInt(versions.node.split('.')[0] ?? '0', 10);
  if (major >= REQUIRED_MAJOR_VERSION) return true;
  stderr.write(`kafka: requires Node.js >=${String(REQUIRED_MAJOR_VERSION)}.0.0, found ${versions.node}\n`);
  return false;
}

if (!checkNodeVersion(process.versions, process.stderr)) {
  process.exitCode = 70;
} else {
  const runtime = createRuntime(process);
  process.exitCode = await main(runtime);
}
