import { spawn, ChildProcess } from 'child_process';
import * as http from 'http';

export interface LocalRunConfig {
  /** Port to run LocalRun on. Default: 4566 */
  port?: number;
  /** Host to bind to. Default: '127.0.0.1' */
  host?: string;
  /** AWS services to enable, e.g. ['s3', 'sqs']. Default: all supported */
  services?: string[];
  /** AWS region. Default: 'us-east-1' */
  region?: string;
  /** Enable debug logging from LocalRun. Default: false */
  debug?: boolean;
}

const DEFAULT_CONFIG: Required<LocalRunConfig> = {
  port: 4566,
  host: '127.0.0.1',
  services: [],
  region: 'us-east-1',
  debug: false,
};

/** Resolved config with all defaults applied. */
type ResolvedConfig = Required<LocalRunConfig>;

function resolveConfig(config?: LocalRunConfig): ResolvedConfig {
  return { ...DEFAULT_CONFIG, ...config };
}

/**
 * Perform a single HTTP GET and return the status code.
 * Resolves with the status code on any response, rejects on network error.
 */
function httpGetStatus(url: string): Promise<number> {
  return new Promise((resolve, reject) => {
    const req = http.get(url, (res) => {
      // Drain the body so the socket is released
      res.resume();
      resolve(res.statusCode ?? 0);
    });
    req.on('error', reject);
    req.setTimeout(1000, () => {
      req.destroy(new Error(`Request to ${url} timed out`));
    });
  });
}

/**
 * Perform an HTTP POST with no body and return the status code.
 */
function httpPost(urlStr: string): Promise<number> {
  return new Promise((resolve, reject) => {
    const parsed = new URL(urlStr);
    const options: http.RequestOptions = {
      hostname: parsed.hostname,
      port: parsed.port,
      path: parsed.pathname + parsed.search,
      method: 'POST',
      headers: { 'Content-Length': '0' },
    };
    const req = http.request(options, (res) => {
      res.resume();
      resolve(res.statusCode ?? 0);
    });
    req.on('error', reject);
    req.setTimeout(5000, () => {
      req.destroy(new Error(`POST to ${urlStr} timed out`));
    });
    req.end();
  });
}

/**
 * Poll the /health endpoint until it returns 200 or the timeout expires.
 */
function waitForHealth(
  healthUrl: string,
  intervalMs = 500,
  timeoutMs = 30_000,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const deadline = Date.now() + timeoutMs;

    const attempt = () => {
      httpGetStatus(healthUrl)
        .then((status) => {
          if (status === 200) {
            resolve();
          } else if (Date.now() >= deadline) {
            reject(
              new Error(
                `LocalRun health check failed: last status ${status} after ${timeoutMs}ms`,
              ),
            );
          } else {
            setTimeout(attempt, intervalMs);
          }
        })
        .catch(() => {
          if (Date.now() >= deadline) {
            reject(
              new Error(
                `LocalRun did not become healthy within ${timeoutMs}ms`,
              ),
            );
          } else {
            setTimeout(attempt, intervalMs);
          }
        });
    };

    attempt();
  });
}

/**
 * Try to spawn the LocalRun server process.
 * Prefers `aws-local-run` binary; falls back to `python -m localrun`.
 */
function spawnProcess(cfg: ResolvedConfig): ChildProcess {
  const portStr = String(cfg.port);
  const args = ['start', '--port', portStr, '--host', cfg.host];

  if (cfg.services.length > 0) {
    args.push('--services', cfg.services.join(','));
  }
  if (cfg.debug) {
    args.push('--debug');
  }

  // Try the installed binary first
  try {
    const proc = spawn('aws-local-run', args, {
      stdio: cfg.debug ? 'inherit' : 'pipe',
      detached: false,
    });

    // If the binary doesn't exist, the 'error' event fires synchronously
    // (ENOENT) before the next tick. We return the process and let the
    // caller handle the error event if needed.
    return proc;
  } catch {
    // Shouldn't normally reach here; spawn() itself rarely throws.
    return spawn('python', ['-m', 'localrun', ...args], {
      stdio: cfg.debug ? 'inherit' : 'pipe',
      detached: false,
    });
  }
}

export class LocalRunServer {
  private cfg: ResolvedConfig;
  private process: ChildProcess | null = null;
  private running = false;

  constructor(config?: LocalRunConfig) {
    this.cfg = resolveConfig(config);
  }

  /**
   * Start the LocalRun server and wait until it is healthy.
   * Throws if the server does not become healthy within 30 seconds.
   */
  async start(): Promise<void> {
    if (this.running) {
      return;
    }

    let proc = spawnProcess(this.cfg);

    // If `aws-local-run` is not found, fall back to `python -m localrun`
    await new Promise<void>((resolve, reject) => {
      let resolved = false;

      proc.on('error', (err: NodeJS.ErrnoException) => {
        if (err.code === 'ENOENT' && !resolved) {
          // Binary not found – try the Python fallback
          proc.removeAllListeners();
          proc = spawn(
            'python',
            [
              '-m',
              'localrun',
              'start',
              '--port',
              String(this.cfg.port),
              '--host',
              this.cfg.host,
              ...(this.cfg.services.length > 0
                ? ['--services', this.cfg.services.join(',')]
                : []),
              ...(this.cfg.debug ? ['--debug'] : []),
            ],
            { stdio: this.cfg.debug ? 'inherit' : 'pipe', detached: false },
          );

          proc.on('error', (fallbackErr) => {
            if (!resolved) {
              resolved = true;
              reject(
                new Error(
                  `Failed to start LocalRun (tried aws-local-run and python -m localrun): ${fallbackErr.message}`,
                ),
              );
            }
          });

          // Fallback process spawned successfully – proceed to health check
          resolved = true;
          this.process = proc;
          resolve();
        } else if (!resolved) {
          resolved = true;
          reject(err);
        }
      });

      // Give the error event a chance to fire (ENOENT is synchronous)
      setImmediate(() => {
        if (!resolved) {
          resolved = true;
          this.process = proc;
          resolve();
        }
      });
    });

    this.running = true;

    try {
      await waitForHealth(`${this.getUrl()}/health`);
    } catch (err) {
      // Clean up the process if health check fails
      await this.stop();
      throw err;
    }
  }

  /**
   * Stop the LocalRun server process.
   */
  async stop(): Promise<void> {
    if (!this.process) {
      this.running = false;
      return;
    }

    const proc = this.process;
    this.process = null;
    this.running = false;

    await new Promise<void>((resolve) => {
      proc.on('close', () => resolve());
      proc.kill('SIGTERM');

      // Force-kill after 5 seconds if it hasn't exited
      setTimeout(() => {
        try {
          proc.kill('SIGKILL');
        } catch {
          // Process may have already exited
        }
        resolve();
      }, 5000);
    });
  }

  /**
   * Reset all LocalRun state by calling POST /_localrun/reset.
   */
  async reset(): Promise<void> {
    const status = await httpPost(`${this.getUrl()}/_localrun/reset`);
    if (status !== 200) {
      throw new Error(`LocalRun reset failed with status ${status}`);
    }
  }

  /**
   * Returns the base URL for this LocalRun instance.
   */
  getUrl(): string {
    return `http://${this.cfg.host}:${this.cfg.port}`;
  }

  /**
   * Returns an AWS SDK-compatible config object pointing at this LocalRun instance.
   * Works with both AWS SDK v2 and v3.
   */
  getAwsConfig(): {
    endpoint: string;
    region: string;
    credentials: { accessKeyId: string; secretAccessKey: string };
  } {
    return {
      endpoint: this.getUrl(),
      region: this.cfg.region,
      credentials: {
        accessKeyId: 'test',
        secretAccessKey: 'test',
      },
    };
  }

  /**
   * Returns true if the server process is currently running.
   */
  isRunning(): boolean {
    return this.running;
  }
}

/**
 * Convenience function: create a LocalRunServer, start it, and return it.
 */
export async function startLocalRun(
  config?: LocalRunConfig,
): Promise<LocalRunServer> {
  const server = new LocalRunServer(config);
  await server.start();
  return server;
}

/**
 * Run `fn` with a fresh LocalRun server, then stop the server.
 * The server instance is passed to `fn`.
 * The server is stopped even if `fn` throws.
 */
export async function withLocalRun<T>(
  fn: (server: LocalRunServer) => Promise<T>,
  config?: LocalRunConfig,
): Promise<T> {
  const server = await startLocalRun(config);
  try {
    return await fn(server);
  } finally {
    await server.stop();
  }
}
