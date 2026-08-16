/**
 * The operation handler is also called by trusted local capture paths, which
 * bypass the CLI and MCP dispatch fences. Keep `__all__` from becoming a
 * write target at the final handler boundary.
 */
import { describe, expect, test } from 'bun:test';
import { operations, OperationError, type OperationContext } from '../src/core/operations.ts';
import type { BrainEngine } from '../src/core/engine.ts';

function putPageHandler() {
  const operation = operations.find(op => op.name === 'put_page');
  if (!operation) throw new Error('put_page operation missing');
  return operation.handler;
}

function context(sourceId: string, engine: BrainEngine, dryRun = false): OperationContext {
  return {
    engine,
    config: {} as OperationContext['config'],
    logger: { info: () => {}, warn: () => {}, error: () => {} },
    dryRun,
    remote: false,
    sourceId,
  };
}

describe('put_page direct handler __all__ fence', () => {
  test('rejects the sentinel before touching the engine', async () => {
    const engine = new Proxy({} as BrainEngine, {
      get() {
        throw new Error('the all-sources fence must run before any engine call');
      },
    });

    const result = putPageHandler()(context('__all__', engine), {
      slug: 'fence-test',
      content: '# fence test',
    });

    await expect(result).rejects.toMatchObject({
      code: 'source_binding_required',
    } satisfies Partial<OperationError>);
  });

  test('does not block a concrete source before the existing handler boundary', async () => {
    const result = await putPageHandler()(context('default', {} as BrainEngine, true), {
      slug: 'concrete-source',
      content: '# concrete source',
    });

    expect(result).toEqual({ dry_run: true, action: 'put_page', slug: 'concrete-source' });
  });
});
