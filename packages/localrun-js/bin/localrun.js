#!/usr/bin/env node
/**
 * CLI wrapper for LocalRun.
 *
 * Forwards all arguments to the `aws-local-run` binary if it is available,
 * otherwise falls back to `python -m localrun`.
 *
 * Examples:
 *   localrun start
 *   localrun start --port 4566
 *   localrun stop
 *   localrun status
 */

'use strict';

const { spawn } = require('child_process');

const args = process.argv.slice(2);

/**
 * Attempt to run `command` with `args`. Returns a Promise that resolves with
 * the exit code, or rejects with an ENOENT error if the binary was not found.
 */
function run(command, cmdArgs) {
  return new Promise((resolve, reject) => {
    const proc = spawn(command, cmdArgs, { stdio: 'inherit' });

    proc.on('error', (err) => {
      reject(err);
    });

    proc.on('close', (code) => {
      resolve(code ?? 0);
    });
  });
}

async function main() {
  // Try the installed binary first
  try {
    const code = await run('aws-local-run', args);
    process.exit(code);
  } catch (err) {
    if (err.code !== 'ENOENT') {
      // Unexpected error — surface it
      console.error('localrun: unexpected error:', err.message);
      process.exit(1);
    }
  }

  // Binary not found — try the Python module
  try {
    const code = await run('python', ['-m', 'localrun', ...args]);
    process.exit(code);
  } catch (err) {
    if (err.code === 'ENOENT') {
      console.error(
        'localrun: could not find `aws-local-run` or `python` on PATH.\n' +
          'Install LocalRun with: pip install localrun',
      );
    } else {
      console.error('localrun: error running python -m localrun:', err.message);
    }
    process.exit(1);
  }
}

main();
