/**
 * Jest setupFile: forwards LocalRun endpoint env vars so that AWS SDK
 * clients created inside test files automatically target LocalRun.
 *
 * This file is registered via `jestPreset.setupFiles` and runs once per
 * test suite (worker process), before any test file is imported.
 *
 * The variables are set by `globalSetup` in the main process; Jest copies
 * `process.env` into each worker, so they are available here.
 */

const endpoint =
  process.env['LOCALRUN_ENDPOINT'] ||
  process.env['LOCALSTACK_ENDPOINT'] ||
  process.env['AWS_ENDPOINT_URL'];

if (endpoint) {
  // Ensure all three names are consistent
  process.env['AWS_ENDPOINT_URL'] = endpoint;
  process.env['LOCALSTACK_ENDPOINT'] = endpoint;
  process.env['LOCALRUN_ENDPOINT'] = endpoint;
}

// Provide dummy credentials if none have been set, so SDK clients don't
// attempt real credential resolution (which can slow down tests or fail).
if (!process.env['AWS_ACCESS_KEY_ID']) {
  process.env['AWS_ACCESS_KEY_ID'] = 'test';
}
if (!process.env['AWS_SECRET_ACCESS_KEY']) {
  process.env['AWS_SECRET_ACCESS_KEY'] = 'test';
}
if (!process.env['AWS_REGION']) {
  process.env['AWS_REGION'] = process.env['AWS_DEFAULT_REGION'] ?? 'us-east-1';
}
