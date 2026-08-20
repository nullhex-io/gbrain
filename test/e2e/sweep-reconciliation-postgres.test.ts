/**
 * Real-Postgres regression for maintenance-sweep link reconciliation.
 *
 * The transaction-scoped reconciler must remain compatible with optional RLS
 * scope binding. Calling a scoped engine read from the transaction clone would
 * try to open a nested postgres.js transaction and fail before pruning stale
 * links. The self-retrying batch helper also cannot be called from inside the
 * outer transaction. This test enables the production flag, forbids that
 * helper, and proves that managed rows are removed while manual provenance
 * survives.
 */
import { afterAll, beforeAll, beforeEach, describe, expect, test } from 'bun:test';

import type { BrainEngine } from '../../src/core/engine.ts';
import type { CapabilityReport } from '../../src/core/capability.ts';
import { runMaintenanceSweep } from '../../src/core/sweep.ts';
import { getEngine, hasDatabase, setupDB, teardownDB } from './helpers.ts';

const describePostgres = hasDatabase() ? describe : describe.skip;

const KEYLESS: CapabilityReport = {
  embeddings: { available: false },
  extraction: { available: false },
  search: 'keyword-only',
  mode: 'keyless',
};

describePostgres('maintenance sweep link reconciliation on Postgres', () => {
  let engine: BrainEngine;

  beforeAll(async () => {
    await setupDB();
    engine = getEngine();
  }, 90_000);

  afterAll(async () => {
    await teardownDB();
  }, 30_000);

  beforeEach(async () => {
    const cursorKeys = await engine.listConfigKeys('sweep.links_timeline.cursor.v1.');
    await Promise.all(cursorKeys.map(key => engine.unsetConfig(key)));
  });

  test('uses an RLS-safe transaction-local batch and prunes stale managed edges', async () => {
    await engine.putPage('concepts/pg-sweep-target', {
      type: 'concept',
      title: 'Postgres sweep target',
      compiled_truth: 'Target page.',
      timeline: '',
    });
    await engine.putPage('notes/pg-sweep-writer', {
      type: 'note',
      title: 'Postgres sweep writer',
      compiled_truth: 'References [the target](concepts/pg-sweep-target).',
      timeline: '',
    });
    await engine.executeRaw(
      `UPDATE pages
          SET links_extracted_at = updated_at
        WHERE slug = 'concepts/pg-sweep-target'
          AND source_id = 'default'`,
    );

    const previousRls = process.env.GBRAIN_RLS_SCOPE_BINDING;
    const ownAddLinksBatch = Object.getOwnPropertyDescriptor(engine, 'addLinksBatch');
    process.env.GBRAIN_RLS_SCOPE_BINDING = '1';
    Object.defineProperty(engine, 'addLinksBatch', {
      configurable: true,
      value: async () => {
        throw new Error('self-retrying addLinksBatch is forbidden in a sweep transaction');
      },
    });
    try {
      const initial = await runMaintenanceSweep(engine, {
        sourceId: 'default',
        capabilities: KEYLESS,
      });
      expect(initial.linksExtracted).toBe(1);
      await engine.addLink(
        'notes/pg-sweep-writer',
        'concepts/pg-sweep-target',
        'Operator-authored edge',
        'mentions',
        'manual',
      );
      await engine.executeRaw(
        `UPDATE pages
            SET compiled_truth = 'The reference is gone.',
                updated_at = $1
          WHERE slug = 'notes/pg-sweep-writer'
            AND source_id = 'default'`,
        [new Date(Date.now() + 1_000).toISOString()],
      );

      const reconciled = await runMaintenanceSweep(engine, {
        sourceId: 'default',
        capabilities: KEYLESS,
      });
      expect(reconciled.skipped.map(item => item.reason)).not.toContain('links_timeline_error');
      expect(reconciled.linksRemoved).toBe(1);
    } finally {
      if (ownAddLinksBatch) Object.defineProperty(engine, 'addLinksBatch', ownAddLinksBatch);
      else delete (engine as unknown as { addLinksBatch?: unknown }).addLinksBatch;
      if (previousRls === undefined) delete process.env.GBRAIN_RLS_SCOPE_BINDING;
      else process.env.GBRAIN_RLS_SCOPE_BINDING = previousRls;
    }

    const remaining = await engine.executeRaw<{ link_source: string }>(
      `SELECT l.link_source
         FROM links l
         JOIN pages f ON f.id = l.from_page_id
         JOIN pages t ON t.id = l.to_page_id
        WHERE f.slug = 'notes/pg-sweep-writer'
          AND f.source_id = 'default'
          AND t.slug = 'concepts/pg-sweep-target'
          AND t.source_id = 'default'
        ORDER BY l.link_source`,
    );
    expect(remaining.map(row => row.link_source)).toEqual(['manual']);
  }, 60_000);

  test('an exact legacy NULL-provenance row satisfies a desired markdown edge', async () => {
    await engine.putPage('concepts/pg-legacy-null-target', {
      type: 'concept',
      title: 'Postgres legacy NULL target',
      compiled_truth: 'Target page.',
      timeline: '',
    });
    await engine.putPage('notes/pg-legacy-null-writer', {
      type: 'note',
      title: 'Postgres legacy NULL writer',
      compiled_truth: 'References [the target](concepts/pg-legacy-null-target).',
      timeline: '',
    });
    await engine.executeRaw(
      `INSERT INTO links (from_page_id, to_page_id, link_type, context, link_source)
       SELECT f.id, t.id, 'mentions', 'Legacy edge', NULL
         FROM pages f
         JOIN pages t ON true
        WHERE f.slug = 'notes/pg-legacy-null-writer'
          AND f.source_id = 'default'
          AND t.slug = 'concepts/pg-legacy-null-target'
          AND t.source_id = 'default'`,
    );

    const reconciled = await runMaintenanceSweep(engine, {
      sourceId: 'default',
      capabilities: KEYLESS,
    });
    expect(reconciled.linksExtracted).toBe(0);
    const rows = await engine.executeRaw<{ link_source: string | null }>(
      `SELECT l.link_source
         FROM links l
         JOIN pages f ON f.id = l.from_page_id
         JOIN pages t ON t.id = l.to_page_id
        WHERE f.slug = 'notes/pg-legacy-null-writer'
          AND f.source_id = 'default'
          AND t.slug = 'concepts/pg-legacy-null-target'
          AND t.source_id = 'default'`,
    );
    expect(rows).toEqual([{ link_source: null }]);
  }, 60_000);

  test('a soft-deleted local target shadows the active default fallback', async () => {
    await engine.executeRaw(
      `INSERT INTO sources (id, name)
       VALUES ('custom', 'custom')
       ON CONFLICT (id) DO NOTHING`,
    );
    await engine.putPage('concepts/pg-shadow-same', {
      type: 'concept',
      title: 'Postgres default target',
      compiled_truth: 'Default target.',
      timeline: '',
    });
    await engine.putPage('concepts/pg-shadow-same', {
      type: 'concept',
      title: 'Postgres custom target',
      compiled_truth: 'Custom target.',
      timeline: '',
    }, { sourceId: 'custom' });
    await engine.putPage('notes/pg-shadow-writer', {
      type: 'note',
      title: 'Postgres custom writer',
      compiled_truth: 'References [the target](concepts/pg-shadow-same).',
      timeline: '',
    }, { sourceId: 'custom' });
    await engine.softDeletePage('concepts/pg-shadow-same', { sourceId: 'custom' });

    const whileDeleted = await runMaintenanceSweep(engine, {
      sourceId: 'custom',
      capabilities: KEYLESS,
    });
    expect(whileDeleted.linksExtracted).toBe(0);
    const deferred = await engine.executeRaw<{ links: string; stale: string }>(
      `SELECT COUNT(l.id) AS links,
              COUNT(*) FILTER (
                WHERE p.links_extracted_at IS NULL
                   OR p.links_extracted_at < p.updated_at
              ) AS stale
         FROM pages p
         LEFT JOIN links l ON l.from_page_id = p.id
        WHERE p.slug = 'notes/pg-shadow-writer'
          AND p.source_id = 'custom'
          AND p.deleted_at IS NULL`,
    );
    expect(parseInt(deferred[0].links, 10)).toBe(0);
    expect(parseInt(deferred[0].stale, 10)).toBe(1);

    expect(await engine.restorePage('concepts/pg-shadow-same', { sourceId: 'custom' })).toBe(true);
    const recovered = await runMaintenanceSweep(engine, {
      sourceId: 'custom',
      capabilities: KEYLESS,
    });
    expect(recovered.linksExtracted).toBe(1);
    const target = await engine.executeRaw<{ source_id: string }>(
      `SELECT t.source_id
         FROM links l
         JOIN pages f ON f.id = l.from_page_id
         JOIN pages t ON t.id = l.to_page_id
        WHERE f.slug = 'notes/pg-shadow-writer'
          AND f.source_id = 'custom'`,
    );
    expect(target).toEqual([{ source_id: 'custom' }]);
  }, 60_000);

  test('a restored soft target retries stale writer reconciliation before stamping', async () => {
    await engine.putPage('concepts/pg-soft-sweep-target', {
      type: 'concept',
      title: 'Postgres soft sweep target',
      compiled_truth: 'Target page.',
      timeline: '',
    });
    await engine.putPage('notes/pg-soft-sweep-writer', {
      type: 'note',
      title: 'Postgres soft sweep writer',
      compiled_truth: 'References [the target](concepts/pg-soft-sweep-target).',
      timeline: '',
    });
    await engine.executeRaw(
      `UPDATE pages
          SET links_extracted_at = updated_at
        WHERE slug = 'concepts/pg-soft-sweep-target'
          AND source_id = 'default'`,
    );

    const initial = await runMaintenanceSweep(engine, {
      sourceId: 'default',
      capabilities: KEYLESS,
    });
    expect(initial.linksExtracted).toBe(1);
    await engine.softDeletePage('concepts/pg-soft-sweep-target', {
      sourceId: 'default',
    });
    await engine.executeRaw(
      `UPDATE pages
          SET compiled_truth = 'The reference is gone.',
              updated_at = $1
        WHERE slug = 'notes/pg-soft-sweep-writer'
          AND source_id = 'default'`,
      [new Date(Date.now() + 1_000).toISOString()],
    );

    const whileDeleted = await runMaintenanceSweep(engine, {
      sourceId: 'default',
      capabilities: KEYLESS,
    });
    expect(whileDeleted.linksRemoved).toBe(0);
    const preserved = await engine.executeRaw<{ links: string; stale: string }>(
      `SELECT COUNT(l.id) AS links,
              COUNT(*) FILTER (WHERE p.links_extracted_at < p.updated_at) AS stale
         FROM pages p
         LEFT JOIN links l ON l.from_page_id = p.id
        WHERE p.slug = 'notes/pg-soft-sweep-writer'
          AND p.source_id = 'default'
          AND p.deleted_at IS NULL`,
    );
    expect(parseInt(preserved[0].links, 10)).toBe(1);
    expect(parseInt(preserved[0].stale, 10)).toBe(1);

    expect(await engine.restorePage('concepts/pg-soft-sweep-target', {
      sourceId: 'default',
    })).toBe(true);
    const recovered = await runMaintenanceSweep(engine, {
      sourceId: 'default',
      capabilities: KEYLESS,
    });
    expect(recovered.linksRemoved).toBe(1);
    const reconciled = await engine.executeRaw<{ links: string; stamped: string }>(
      `SELECT COUNT(l.id) AS links,
              COUNT(*) FILTER (WHERE p.links_extracted_at >= p.updated_at) AS stamped
         FROM pages p
         LEFT JOIN links l ON l.from_page_id = p.id
        WHERE p.slug = 'notes/pg-soft-sweep-writer'
          AND p.source_id = 'default'
          AND p.deleted_at IS NULL`,
    );
    expect(parseInt(reconciled[0].links, 10)).toBe(0);
    expect(parseInt(reconciled[0].stamped, 10)).toBe(1);
  }, 60_000);

  test('a target deleted before the first sweep leaves its writer retryable', async () => {
    await engine.putPage('concepts/pg-first-sweep-soft-target', {
      type: 'concept',
      title: 'Postgres first-sweep soft target',
      compiled_truth: 'Target page.',
      timeline: '',
    });
    await engine.putPage('notes/pg-first-sweep-soft-writer', {
      type: 'note',
      title: 'Postgres first-sweep soft writer',
      compiled_truth: 'References [the target](concepts/pg-first-sweep-soft-target).',
      timeline: '',
    });
    await engine.softDeletePage('concepts/pg-first-sweep-soft-target', {
      sourceId: 'default',
    });

    const whileDeleted = await runMaintenanceSweep(engine, {
      sourceId: 'default',
      capabilities: KEYLESS,
    });
    expect(whileDeleted.linksExtracted).toBe(0);
    const deferred = await engine.executeRaw<{ links: string; stale: string }>(
      `SELECT COUNT(l.id) AS links,
              COUNT(*) FILTER (
                WHERE p.links_extracted_at IS NULL
                   OR p.links_extracted_at < p.updated_at
              ) AS stale
         FROM pages p
         LEFT JOIN links l ON l.from_page_id = p.id
        WHERE p.slug = 'notes/pg-first-sweep-soft-writer'
          AND p.source_id = 'default'
          AND p.deleted_at IS NULL`,
    );
    expect(parseInt(deferred[0].links, 10)).toBe(0);
    expect(parseInt(deferred[0].stale, 10)).toBe(1);

    expect(await engine.restorePage('concepts/pg-first-sweep-soft-target', {
      sourceId: 'default',
    })).toBe(true);
    const recovered = await runMaintenanceSweep(engine, {
      sourceId: 'default',
      capabilities: KEYLESS,
    });
    expect(recovered.linksExtracted).toBe(1);
    const reconciled = await engine.executeRaw<{ links: string; stamped: string }>(
      `SELECT COUNT(l.id) AS links,
              COUNT(*) FILTER (WHERE p.links_extracted_at >= p.updated_at) AS stamped
         FROM pages p
         LEFT JOIN links l ON l.from_page_id = p.id
        WHERE p.slug = 'notes/pg-first-sweep-soft-writer'
          AND p.source_id = 'default'
          AND p.deleted_at IS NULL`,
    );
    expect(parseInt(reconciled[0].links, 10)).toBe(1);
    expect(parseInt(reconciled[0].stamped, 10)).toBe(1);
  }, 60_000);
});
