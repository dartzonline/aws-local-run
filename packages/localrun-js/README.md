# localrun

Node.js client and test helpers for [LocalRun](https://github.com/anilbodepudi/localrun) — a lightweight local AWS emulator that runs on port 4566 (API-compatible with LocalStack).

## Installation

```bash
npm install localrun
# or
yarn add localrun
```

LocalRun itself must be installed separately:

```bash
pip install localrun
```

---

## Quick start

```typescript
import { startLocalRun } from 'localrun';

const server = await startLocalRun();

// Use getAwsConfig() to point your AWS SDK at LocalRun
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const client = new DynamoDBClient(server.getAwsConfig());

await server.stop();
```

### `withLocalRun` helper

```typescript
import { withLocalRun } from 'localrun';

await withLocalRun(async (server) => {
  const config = server.getAwsConfig();
  // ... run your code
});
// server is automatically stopped when the callback returns (or throws)
```

---

## API

### `LocalRunConfig`

| Property   | Type       | Default         | Description                              |
|------------|------------|-----------------|------------------------------------------|
| `port`     | `number`   | `4566`          | Port LocalRun listens on                 |
| `host`     | `string`   | `'127.0.0.1'`  | Host LocalRun binds to                   |
| `services` | `string[]` | `[]` (all)      | Restrict enabled services, e.g. `['s3']`|
| `region`   | `string`   | `'us-east-1'`  | AWS region reported by LocalRun          |
| `debug`    | `boolean`  | `false`         | Pipe LocalRun stdout/stderr to console   |

### `LocalRunServer`

```typescript
const server = new LocalRunServer(config?: LocalRunConfig);

await server.start();          // Spawn process, wait up to 30s for /health
await server.stop();           // SIGTERM the process (SIGKILL after 5s)
await server.reset();          // POST /_localrun/reset — wipe all state
server.getUrl();               // 'http://127.0.0.1:4566'
server.getAwsConfig();         // { endpoint, region, credentials }
server.isRunning();            // boolean
```

### `startLocalRun(config?)`

Convenience function — creates a `LocalRunServer`, calls `start()`, and returns it.

### `withLocalRun(fn, config?)`

Starts LocalRun, calls `fn(server)`, stops LocalRun (even on error).

---

## Jest integration

### Global setup / teardown

```javascript
// jest.config.js
const { setup, teardown } = require('localrun/jest');

module.exports = {
  globalSetup: setup,
  globalTeardown: teardown,
  testEnvironment: 'node',
};
```

With custom config:

```javascript
// jest.config.js
const { setup, teardown } = require('localrun/jest');

module.exports = {
  globalSetup: () => setup({ region: 'eu-west-1', services: ['s3', 'sqs'] }),
  globalTeardown: teardown,
  testEnvironment: 'node',
};
```

### `jestPreset`

The `jestPreset` export pre-configures:
- `testEnvironment: 'node'`
- A `setupFiles` entry that forwards `AWS_*` environment variables into each test worker

```javascript
// jest.config.js
const { setup, teardown, jestPreset } = require('localrun/jest');

module.exports = {
  ...jestPreset,
  globalSetup: setup,
  globalTeardown: teardown,
};
```

### Inside a test file

After globalSetup runs, `AWS_ENDPOINT_URL`, `AWS_REGION`, `AWS_ACCESS_KEY_ID`, and `AWS_SECRET_ACCESS_KEY` are set. AWS SDK v3 respects `AWS_ENDPOINT_URL` automatically (SDK ≥ 3.363).

```typescript
// my.test.ts
import { S3Client, ListBucketsCommand } from '@aws-sdk/client-s3';

// No extra config needed — env vars point the SDK at LocalRun
const s3 = new S3Client({});

test('lists buckets', async () => {
  const res = await s3.send(new ListBucketsCommand({}));
  expect(res.Buckets).toBeDefined();
});
```

---

## Vitest integration

```typescript
// vitest.config.ts
import { defineConfig } from 'vitest/config';
import { localrunSetup } from 'localrun/vitest';

export default defineConfig({
  test: {
    globalSetup: [localrunSetup()],
    // or with options:
    // globalSetup: [localrunSetup({ port: 4566, region: 'eu-west-1' })],
  },
});
```

Vitest automatically calls the teardown function returned by the setup function, so you don't need a separate `globalTeardown`.

### Inside a test file

```typescript
// my.test.ts
import { describe, it, expect } from 'vitest';
import { SQSClient, ListQueuesCommand } from '@aws-sdk/client-sqs';

const sqs = new SQSClient({});  // AWS_ENDPOINT_URL is set by globalSetup

describe('SQS', () => {
  it('lists queues', async () => {
    const res = await sqs.send(new ListQueuesCommand({}));
    expect(res.QueueUrls).toBeDefined();
  });
});
```

---

## CLI

The package ships a `localrun` binary that forwards commands to the Python CLI:

```bash
npx localrun start
npx localrun start --port 4566
npx localrun stop
npx localrun status
```

---

## Environment variables set by the helpers

| Variable                | Value                         |
|-------------------------|-------------------------------|
| `AWS_ACCESS_KEY_ID`     | `test`                        |
| `AWS_SECRET_ACCESS_KEY` | `test`                        |
| `AWS_REGION`            | config `region` (default `us-east-1`) |
| `AWS_DEFAULT_REGION`    | same as `AWS_REGION`          |
| `AWS_ENDPOINT_URL`      | `http://127.0.0.1:4566`       |
| `LOCALSTACK_ENDPOINT`   | same as `AWS_ENDPOINT_URL`    |
| `LOCALRUN_ENDPOINT`     | same as `AWS_ENDPOINT_URL`    |

---

## License

MIT
