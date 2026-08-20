/**
 * `__all__` is a read-only federation sentinel, never a write-job source.
 *
 * Submitters must refuse it before a queue row is created, while workers must
 * dead-letter legacy or externally-written rows before a handler can run.
 */
import { afterAll, beforeAll, beforeEach, describe, expect, test } from 'bun:test';
import { PGLiteEngine } from '../src/core/pglite-engine.ts';
import { operationsByName } from '../src/core/operations.ts';
import { runJobs } from '../src/commands/jobs.ts';
import { MinionQueue } from '../src/core/minions/queue.ts';
import { MinionWorker } from '../src/core/minions/worker.ts';

let engine: PGLiteEngine;
let queue: MinionQueue;

beforeAll(async () => {
  engine = new PGLiteEngine();
  await engine.connect({});
  await engine.initSchema();
  queue = new MinionQueue(engine);
});

afterAll(async () => {
  await engine.disconnect();
});

beforeEach(async () => {
  await engine.executeRaw('DELETE FROM minion_jobs');
});

const allSourcesBindings = [
  ['camelCase', { sourceId: '__all__' }],
  ['snake_case', { source_id: '__all__' }],
] as const;

const allowedBindings = [
  ['source-free', {}],
  ['concrete camelCase', { sourceId: 'default' }],
  ['concrete snake_case', { source_id: 'default' }],
  ['nested user content', { metadata: { sourceId: '__all__' } }],
] as const;

function submitContext() {
  return {
    engine,
    config: {},
    logger: console,
    dryRun: false,
    remote: false,
  } as never;
}

async function rowCount(): Promise<number> {
  const rows = await engine.executeRaw<{ count: string }>(
    'SELECT count(*)::text AS count FROM minion_jobs',
  );
  return Number(rows[0]?.count ?? '0');
}

describe('__all__ source binding job fences', () => {
  const submitJob = operationsByName.submit_job;

  for (const [label, data] of allSourcesBindings) {
    test(`MCP submission rejects top-level ${label} before enqueue`, async () => {
      await expect(submitJob.handler(submitContext(), { name: 'fence-op', data })).rejects.toMatchObject({
        code: 'invalid_params',
      });
      expect(await rowCount()).toBe(0);
    });

    test(`CLI submission rejects top-level ${label} before enqueue`, async () => {
      await expect(runJobs(engine, [
        'submit', 'fence-cli', '--params', JSON.stringify(data),
      ])).rejects.toThrow('__all__');
      expect(await rowCount()).toBe(0);
    });
  }

  for (const [label, data] of allowedBindings) {
    test(`MCP submission preserves ${label} payloads`, async () => {
      const result = await submitJob.handler(submitContext(), { name: 'fence-op', data }) as { id: number };
      expect(result.id).toBeGreaterThan(0);
      expect((await queue.getJob(result.id))?.data).toEqual(data);
    });
  }

  for (const [label, data, blocked] of [
    ...allSourcesBindings.map(([name, payload]) => [`rejects ${name}`, payload, true] as const),
    ...allowedBindings.map(([name, payload]) => [`allows ${name}`, payload, false] as const),
  ]) {
    test(`worker execution boundary ${label}`, async () => {
      const job = await queue.add('fence-worker', {});
      // Simulate a legacy or externally inserted row that bypassed queue.add.
      await engine.executeRaw(
        'UPDATE minion_jobs SET data = $2::jsonb WHERE id = $1',
        [job.id, JSON.stringify(data)],
      );

      let handlerCalls = 0;
      const worker = new MinionWorker(engine, { concurrency: 1, pollInterval: 10 });
      worker.register('fence-worker', async () => {
        handlerCalls++;
        return { ok: true };
      });
      const workerPromise = worker.start();
      await new Promise((resolve) => setTimeout(resolve, 100));
      worker.stop();
      await workerPromise;

      const finished = await queue.getJob(job.id);
      if (blocked) {
        expect(handlerCalls).toBe(0);
        expect(finished?.status).toBe('dead');
        expect(finished?.error_text).toContain('__all__');
      } else {
        expect(handlerCalls).toBe(1);
        expect(finished?.status).toBe('completed');
      }
    });
  }
});
