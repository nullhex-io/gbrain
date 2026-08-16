/**
 * The normal local CLI dispatch invokes operation handlers directly rather
 * than through MCP dispatch. Pin the __all__ write fence on that route with a
 * real PGLite brain: writes fail before the handler, ordinary reads continue.
 */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const CLI = join(import.meta.dir, '..', 'src', 'cli.ts');
let home: string;

function runCli(args: string[], input?: string, allSources = true) {
  const env: NodeJS.ProcessEnv = { ...process.env, GBRAIN_HOME: home, GBRAIN_SWEEP: '0' };
  delete env.DATABASE_URL;
  delete env.GBRAIN_DATABASE_URL;
  if (allSources) env.GBRAIN_SOURCE = '__all__';
  else delete env.GBRAIN_SOURCE;
  return spawnSync('bun', [CLI, ...args], {
    cwd: join(import.meta.dir, '..'),
    env,
    input,
    encoding: 'utf8',
  });
}

describe('direct local CLI __all__ fence', () => {
  beforeAll(() => {
    home = mkdtempSync(join(tmpdir(), 'gbrain-all-cli-fence-'));
    const init = runCli(['init', '--pglite', '--no-embedding', '--non-interactive'], undefined, false);
    expect(init.status).toBe(0);
  }, 30_000);

  afterAll(() => {
    if (home) rmSync(home, { recursive: true, force: true });
  });

  test('put_page and capture writes are rejected while a read still runs', () => {
    const write = runCli(['put', 'fence-test'], '# fence test\n');
    expect(write.status).toBe(1);
    expect(write.stderr).toContain('source_binding_required');

    const capture = runCli(['capture', 'fence test capture']);
    expect(capture.status).toBe(1);
    expect(capture.stderr).toContain('source_binding_required');

    const read = runCli(['search', 'absent-fence-token']);
    expect(read.status).toBe(0);
  }, 30_000);
});
