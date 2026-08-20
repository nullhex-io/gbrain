import { describe, test, expect, afterEach } from 'bun:test';
import { chmodSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import type { BrainEngine } from '../src/core/engine.ts';
import {
  buildPgliteIpcHandlers,
  resolveMcpStdioSourceScope,
  shouldArmStartupSweep,
} from '../src/mcp/server.ts';
import { withEnv } from './helpers/with-env.ts';

function makeEngine(registeredSources: string[]): BrainEngine {
  return {
    kind: 'pglite',
    executeRaw: async <T>(sql: string, params?: unknown[]): Promise<T[]> => {
      if (sql.includes('SELECT id FROM sources WHERE id = $1')) {
        const id = params?.[0];
        return (typeof id === 'string' && registeredSources.includes(id)
          ? [{ id } as T]
          : []);
      }
      if (sql.includes('SELECT id, local_path FROM sources')) return [];
      if (sql.includes('SELECT id, config, archived FROM sources')) {
        return registeredSources.map(id => ({ id, config: null, archived: false }) as T);
      }
      if (sql.includes('SELECT id, name, local_path, last_commit, last_sync_at, config, created_at')) {
        return registeredSources.map(id => ({
          id,
          name: id,
          local_path: null,
          last_commit: null,
          last_sync_at: null,
          config: id === 'default' || id === 'team-alpha' ? { federated: true } : {},
          created_at: new Date(),
          archived: false,
        }) as T);
      }
      return [];
    },
    getConfig: async () => null,
  } as unknown as BrainEngine;
}

function writeSourceDotfile(dir: string, sourceId: string): void {
  const path = join(dir, '.gbrain-source');
  writeFileSync(path, `${sourceId}\n`);
  chmodSync(path, 0o600);
}

describe('stdio MCP source resolution', () => {
  const scratchDirs: string[] = [];

  afterEach(() => {
    for (const dir of scratchDirs.splice(0)) {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test('honors .gbrain-source when GBRAIN_SOURCE is absent', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'gbrain-mcp-source-'));
    scratchDirs.push(dir);
    writeSourceDotfile(dir, 'team-alpha');

    await withEnv({ GBRAIN_SOURCE: undefined }, async () => {
      const scope = await resolveMcpStdioSourceScope(
        makeEngine(['default', 'team-alpha']),
        dir,
      );

      expect(scope).toEqual({ sourceId: 'team-alpha', tier: 'dotfile' });
    });
  });

  test('GBRAIN_SOURCE wins over .gbrain-source', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'gbrain-mcp-source-'));
    scratchDirs.push(dir);
    writeSourceDotfile(dir, 'team-alpha');

    await withEnv({ GBRAIN_SOURCE: 'env-source' }, async () => {
      const scope = await resolveMcpStdioSourceScope(
        makeEngine(['default', 'team-alpha', 'env-source']),
        dir,
      );

      expect(scope).toEqual({ sourceId: 'env-source', tier: 'env' });
    });
  });

  test('GBRAIN_SOURCE=__all__ issues every active source, including isolated ones', async () => {
    await withEnv({ GBRAIN_SOURCE: '__all__' }, async () => {
      const scope = await resolveMcpStdioSourceScope(
        makeEngine(['default', 'team-alpha', 'private']),
        '/nonexistent',
      );

      expect(scope).toEqual({
        sourceId: '__all__',
        localFederatedSourceIds: ['default', 'private', 'team-alpha'],
        tier: 'env',
      });
    });
  });

  test('GBRAIN_SOURCE=__all__ fails visibly when active-source enumeration fails', async () => {
    const brokenEngine = {
      executeRaw: async () => { throw new Error('sources unavailable'); },
    } as unknown as BrainEngine;
    await withEnv({ GBRAIN_SOURCE: '__all__' }, async () => {
      await expect(resolveMcpStdioSourceScope(brokenEngine, '/nonexistent'))
        .rejects.toThrow('Unable to enumerate active sources');
    });
  });

  test('ambient __all__ PGLite IPC exposes only stateless handlers and skips sweep', async () => {
    const handlers = await buildPgliteIpcHandlers(
      makeEngine(['default', 'team-alpha']) as BrainEngine,
      { sourceId: '__all__', localFederatedSourceIds: ['default', 'team-alpha'], tier: 'env' },
    );
    expect(handlers.resolve).toBeDefined();
    expect(handlers.turn_context).toBeDefined();
    expect(handlers.context_pack).toBeUndefined();
    expect(handlers.sync_start).toBeUndefined();
    expect(handlers.sync_status).toBeUndefined();
    expect(handlers.sync_abort).toBeUndefined();
    expect(shouldArmStartupSweep('__all__')).toBe(false);
  });
});
