/**
 * Jest global setup/teardown helpers for LocalRun.
 *
 * Usage in jest.config.js / jest.config.ts:
 *
 *   const { setup, teardown } = require('localrun/jest');
 *
 *   module.exports = {
 *     globalSetup:    setup,
 *     globalTeardown: teardown,
 *   };
 *
 * Or, to customise the LocalRun config, wrap the helpers:
 *
 *   globalSetup:    () => setup({ port: 4566, region: 'eu-west-1' }),
 *   globalTeardown: teardown,
 *
 * The `jestPreset` export wires up the AWS environment variables so that
 * the AWS SDK uses LocalRun without any extra configuration:
 *
 *   // jest.config.js
 *   module.exports = {
 *     ...require('localrun/jest').jestPreset,
 *     globalSetup:    require('localrun/jest').setup,
 *     globalTeardown: require('localrun/jest').teardown,
 *   };
 */

import { LocalRunServer, LocalRunConfig } from './index';

// The server instance is held in the global scope so that the Jest worker
// process that runs `globalSetup` can share it with `globalTeardown`.
// (Both run in the same process in standard Jest setups.)
let _server: LocalRunServer | null = null;

/**
 * Jest `globalSetup` function.
 * Starts LocalRun and sets the AWS_* environment variables so every test
 * worker automatically points at the local endpoint.
 */
export async function setup(config?: LocalRunConfig): Promise<void> {
  const server = new LocalRunServer(config);
  await server.start();
  _server = server;

  const awsCfg = server.getAwsConfig();

  // Inject env vars for AWS SDKs
  process.env['AWS_ACCESS_KEY_ID'] = awsCfg.credentials.accessKeyId;
  process.env['AWS_SECRET_ACCESS_KEY'] = awsCfg.credentials.secretAccessKey;
  process.env['AWS_REGION'] = awsCfg.region;
  process.env['AWS_DEFAULT_REGION'] = awsCfg.region;

  // SDK v2 / v3 endpoint override
  process.env['AWS_ENDPOINT_URL'] = awsCfg.endpoint;
  // LocalStack-compatible variable (also works with many community wrappers)
  process.env['LOCALSTACK_ENDPOINT'] = awsCfg.endpoint;
  process.env['LOCALRUN_ENDPOINT'] = awsCfg.endpoint;
}

/**
 * Jest `globalTeardown` function.
 * Stops the LocalRun server started by `setup`.
 */
export async function teardown(): Promise<void> {
  if (_server) {
    await _server.stop();
    _server = null;
  }
}

/**
 * A minimal Jest preset that:
 *  - Uses Node as the test environment (sensible default for backend tests)
 *  - Loads a setup file that sets AWS environment variables before each suite
 *
 * Merge this into your jest.config.js alongside globalSetup/globalTeardown:
 *
 *   module.exports = {
 *     ...require('localrun/jest').jestPreset,
 *     globalSetup:    require('localrun/jest').setup,
 *     globalTeardown: require('localrun/jest').teardown,
 *   };
 */
export const jestPreset: {
  testEnvironment: string;
  setupFiles: string[];
} = {
  testEnvironment: 'node',
  // Points at the compiled JS file so consumers don't need ts-jest for this file
  setupFiles: [require.resolve('./jest-env-setup')],
};
