/**
 * Vitest integration for LocalRun.
 *
 * Usage in vitest.config.ts:
 *
 *   import { localrunSetup } from 'localrun/vitest';
 *
 *   export default defineConfig({
 *     test: {
 *       globalSetup: [localrunSetup()],
 *       // or with custom config:
 *       // globalSetup: [localrunSetup({ port: 4566, region: 'eu-west-1' })],
 *     },
 *   });
 *
 * The returned function follows Vitest's globalSetup contract:
 *   - Call with no arguments to get the setup function.
 *   - The setup function returns a teardown function (Vitest calls it automatically).
 *   - AWS environment variables are set so SDK clients work without extra config.
 */

import { LocalRunServer, LocalRunConfig } from './index';

/**
 * Returns a Vitest-compatible globalSetup function.
 *
 * @param config  Optional LocalRun configuration overrides.
 * @returns       An async setup function whose return value is the teardown.
 */
export function localrunSetup(
  config?: LocalRunConfig,
): () => Promise<() => Promise<void>> {
  return async function vitestLocalRunSetup(): Promise<() => Promise<void>> {
    const server = new LocalRunServer(config);
    await server.start();

    const awsCfg = server.getAwsConfig();

    // Set env vars so test files pick up the local endpoint automatically
    process.env['AWS_ACCESS_KEY_ID'] = awsCfg.credentials.accessKeyId;
    process.env['AWS_SECRET_ACCESS_KEY'] = awsCfg.credentials.secretAccessKey;
    process.env['AWS_REGION'] = awsCfg.region;
    process.env['AWS_DEFAULT_REGION'] = awsCfg.region;
    process.env['AWS_ENDPOINT_URL'] = awsCfg.endpoint;
    process.env['LOCALSTACK_ENDPOINT'] = awsCfg.endpoint;
    process.env['LOCALRUN_ENDPOINT'] = awsCfg.endpoint;

    // Return the teardown function
    return async function vitestLocalRunTeardown(): Promise<void> {
      await server.stop();
    };
  };
}
