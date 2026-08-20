/**
 * v0.34 W3b — code_traversal_cache module tests.
 *
 * Hermetic PGLite test suite covering:
 *  - cache hit returns memoized response (after migration v59)
 *  - cache miss triggers compute
 *  - D3: cluster_generation bump invalidates cached rows
 *  - clearTraversalCache: source-scoped clear deletes the right rows
 *  - clearTraversalCache: --all-sources gate requires explicit opt-out
 *  - getCachedOrCompute: try-cache-then-compute happy path
 */
import { describe, test, expect, beforeAll, afterAll, beforeEach } from 'bun:test';
import { PGLiteEngine } from '../../src/core/pglite-engine.ts';
import type { BrainEngine } from '../../src/core/engine.ts';
import { operations, type OperationContext } from '../../src/core/operations.ts';
import { resetPgliteState } from '../helpers/reset-pglite.ts';
import {
  getCachedTraversal,
  putCachedTraversal,
  getClusterGeneration,
  bumpClusterGeneration,
  clearTraversalCache,
  getCachedOrCompute,
  type CacheKey,
} from '../../src/core/code-intel/traversal-cache.ts';

let engine: PGLiteEngine;

beforeAll(async () => {
  engine = new PGLiteEngine();
  await engine.connect({});
  await engine.initSchema();
});

afterAll(async () => {
  await engine.disconnect();
});

beforeEach(async () => {
  await resetPgliteState(engine);
});

const baseKey = (over: Partial<CacheKey> = {}): CacheKey => ({
  symbol_qualified: 'src/foo::bar',
  depth: 5,
  source_id: 'default',
  cluster_generation: 0,
  ...over,
});

const clearOperation = operations.find((operation) => operation.name === 'code_traversal_cache_clear');
if (!clearOperation) throw new Error('code_traversal_cache_clear operation missing');

function operationContext(
  engine: BrainEngine,
  dryRun = false,
  sourceId = 'default',
): OperationContext {
  return {
    engine,
    config: {} as OperationContext['config'],
    logger: { info: () => {}, warn: () => {}, error: () => {} },
    dryRun,
    remote: false,
    sourceId,
  };
}

describe('W3b: getClusterGeneration / bumpClusterGeneration', () => {
  test('defaults to 0 when never set', async () => {
    const g = await getClusterGeneration(engine);
    expect(g).toBe(0);
  });

  test('bump increments by 1 and persists', async () => {
    const next = await bumpClusterGeneration(engine);
    expect(next).toBe(1);
    const read = await getClusterGeneration(engine);
    expect(read).toBe(1);
  });

  test('multiple bumps are monotonic', async () => {
    await bumpClusterGeneration(engine);
    await bumpClusterGeneration(engine);
    const final = await bumpClusterGeneration(engine);
    expect(final).toBe(3);
  });
});

describe('W3b: putCachedTraversal / getCachedTraversal', () => {
  test('cache miss returns null', async () => {
    const hit = await getCachedTraversal(engine, baseKey());
    expect(hit).toBeNull();
  });

  test('cache hit returns the response after put', async () => {
    const key = baseKey();
    const payload = { result: 'ok', depth_groups: [{ depth: 1, nodes: [] }] };
    await putCachedTraversal(engine, key, payload, new Date().toISOString(), 0);
    const hit = await getCachedTraversal(engine, key);
    expect(hit).not.toBeNull();
    expect(hit?.response).toEqual(payload);
  });

  test('D3: cluster_generation mismatch returns null (cache miss)', async () => {
    const key = baseKey({ cluster_generation: 1 });
    await putCachedTraversal(engine, key, { v: 'fresh' }, new Date().toISOString(), 0);
    // Now look up with a stale generation
    const staleKey = baseKey({ cluster_generation: 0 });
    const hit = await getCachedTraversal(engine, staleKey);
    expect(hit).toBeNull();
  });

  test('UPSERT on conflict replaces older row', async () => {
    const key = baseKey();
    await putCachedTraversal(engine, key, { v: 1 }, new Date().toISOString(), 0);
    await putCachedTraversal(engine, key, { v: 2 }, new Date().toISOString(), 0);
    const hit = await getCachedTraversal<{ v: number }>(engine, key);
    expect(hit?.response).toEqual({ v: 2 });
  });
});

describe('W3b: clearTraversalCache', () => {
  test('operation rejects source_id=__all__ before touching the engine', async () => {
    const untouched = new Proxy({} as BrainEngine, {
      get() {
        throw new Error('sentinel fence must run before engine access');
      },
    });

    const result = clearOperation.handler(operationContext(untouched), {
      source_id: '__all__',
    });

    await expect(result).rejects.toMatchObject({ code: 'source_binding_required' });
  });

  test('operation rejects an ambient __all__ source unless full wipe is explicit', async () => {
    const untouched = new Proxy({} as BrainEngine, {
      get() {
        throw new Error('ambient sentinel fence must run before engine access');
      },
    });

    const result = clearOperation.handler(operationContext(untouched, false, '__all__'), {});

    await expect(result).rejects.toMatchObject({ code: 'source_binding_required' });
    await expect(clearOperation.handler(
      operationContext({} as BrainEngine, true, '__all__'),
      { all_sources: true },
    )).resolves.toMatchObject({ dry_run: true, all_sources: true });
  });

  test('operation preserves concrete and explicit all-source dry runs', async () => {
    const ctx = operationContext({} as BrainEngine, true);
    await expect(clearOperation.handler(ctx, { source_id: 'src-a' })).resolves.toMatchObject({
      dry_run: true,
      source_id: 'src-a',
      all_sources: false,
    });
    await expect(clearOperation.handler(ctx, { all_sources: true })).resolves.toMatchObject({
      dry_run: true,
      all_sources: true,
    });
  });

  test('refuses without source_id or all_sources', async () => {
    await expect(clearTraversalCache(engine, {})).rejects.toThrow(/specify source_id/);
  });

  test('source-scoped clear deletes only that source', async () => {
    await putCachedTraversal(engine, baseKey({ source_id: 'src-a' }), { v: 1 }, new Date().toISOString(), 0);
    await putCachedTraversal(engine, baseKey({ source_id: 'src-b' }), { v: 2 }, new Date().toISOString(), 0);
    const deleted = await clearTraversalCache(engine, { sourceId: 'src-a' });
    expect(deleted).toBe(1);
    const a = await getCachedTraversal(engine, baseKey({ source_id: 'src-a' }));
    const b = await getCachedTraversal(engine, baseKey({ source_id: 'src-b' }));
    expect(a).toBeNull();
    expect(b).not.toBeNull();
  });

  test('all_sources clears everything', async () => {
    await putCachedTraversal(engine, baseKey({ source_id: 'src-a' }), { v: 1 }, new Date().toISOString(), 0);
    await putCachedTraversal(engine, baseKey({ source_id: 'src-b' }), { v: 2 }, new Date().toISOString(), 0);
    const deleted = await clearTraversalCache(engine, { allSources: true });
    expect(deleted).toBe(2);
  });
});

describe('W3b: getCachedOrCompute', () => {
  test('miss: runs compute and caches result', async () => {
    let computeCalls = 0;
    const result = await getCachedOrCompute(
      engine,
      { symbol_qualified: 'foo', depth: 3, source_id: 'default' },
      async () => {
        computeCalls += 1;
        return { x: 42 };
      },
    );
    expect(result).toEqual({ x: 42 });
    expect(computeCalls).toBe(1);
  });

  test('hit: skips compute on second call', async () => {
    let computeCalls = 0;
    const compute = async () => {
      computeCalls += 1;
      return { x: 42 };
    };
    await getCachedOrCompute(
      engine,
      { symbol_qualified: 'foo', depth: 3, source_id: 'default' },
      compute,
    );
    await getCachedOrCompute(
      engine,
      { symbol_qualified: 'foo', depth: 3, source_id: 'default' },
      compute,
    );
    expect(computeCalls).toBe(1);
  });

  test('D3: bumping cluster_generation invalidates the cache', async () => {
    let computeCalls = 0;
    const compute = async () => {
      computeCalls += 1;
      return { x: computeCalls };
    };
    await getCachedOrCompute(
      engine,
      { symbol_qualified: 'foo', depth: 3, source_id: 'default' },
      compute,
    );
    expect(computeCalls).toBe(1);
    // Bump generation — next call should recompute.
    await bumpClusterGeneration(engine);
    const second = await getCachedOrCompute(
      engine,
      { symbol_qualified: 'foo', depth: 3, source_id: 'default' },
      compute,
    );
    expect(computeCalls).toBe(2);
    expect(second).toEqual({ x: 2 });
  });
});
