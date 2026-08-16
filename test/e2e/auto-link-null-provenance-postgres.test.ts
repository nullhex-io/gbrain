/**
 * Real-Postgres regression for legacy NULL link provenance reconciliation.
 *
 * `runAutoLink` must pass a returned NULL provenance through to removeLink so
 * it removes only that legacy row. Omitting the argument intentionally means
 * "any provenance" for callers that need the historical broad-delete API.
 */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { PostgresEngine } from '../../src/core/postgres-engine.ts';
import { operations } from '../../src/core/operations.ts';
import { hasDatabase, setupDB, teardownDB } from './helpers.ts';

const RUN = hasDatabase();
const d = RUN ? describe : describe.skip;
const putPage = operations.find((operation) => operation.name === 'put_page')!;

let engine: PostgresEngine;

d('put_page preserves manual links beside stale legacy NULL rows', () => {
  beforeAll(async () => {
    engine = await setupDB();
  }, 60_000);

  afterAll(async () => {
    await teardownDB();
  }, 60_000);

  test('deletes only the NULL-provenance row', async () => {
    const writerSlug = 'notes/legacy-null-provenance';
    const targetSlug = 'concepts/legacy-null-provenance-target';
    await engine.putPage(writerSlug, {
      type: 'note', title: 'Legacy provenance writer', compiled_truth: '', timeline: '',
    });
    await engine.putPage(targetSlug, {
      type: 'concept', title: 'Legacy provenance target', compiled_truth: '', timeline: '',
    });
    await engine.executeRaw(
      `INSERT INTO links (from_page_id, to_page_id, link_type, context, link_source)
       SELECT f.id, t.id, $1, '', NULL
       FROM pages f
       JOIN pages t ON t.slug = $3 AND t.source_id = 'default'
       WHERE f.slug = $2 AND f.source_id = 'default'`,
      ['mentions', writerSlug, targetSlug],
    );
    await engine.addLink(writerSlug, targetSlug, '', 'mentions', 'manual');

    const response = await putPage.handler({ engine, remote: false } as never, {
      slug: writerSlug,
      content: '---\ntitle: Legacy provenance writer\ntype: note\n---\n\nNo managed references remain.\n',
    }) as { auto_links?: { removed: number } };

    expect(response.auto_links?.removed).toBe(1);
    expect(await engine.getLinks(writerSlug, { sourceId: 'default' })).toEqual([
      expect.objectContaining({
        to_slug: targetSlug,
        link_type: 'mentions',
        link_source: 'manual',
      }),
    ]);
  });

  test('preserves a same-tuple frontmatter edge authored by another page', async () => {
    const personSlug = 'people/postgres-origin-isolated-person';
    const companySlug = 'companies/postgres-origin-isolated-company';
    await engine.putPage(personSlug, {
      type: 'person', title: 'Postgres origin-isolated person', compiled_truth: '', timeline: '',
    });
    await engine.putPage(companySlug, {
      type: 'company', title: 'Postgres origin-isolated company', compiled_truth: '', timeline: '',
    });

    await putPage.handler({ engine, remote: false } as never, {
      slug: companySlug,
      content: `---\ntitle: Postgres origin-isolated company\ntype: company\nkey_people:\n  - ${personSlug}\n---\n`,
    });
    await putPage.handler({ engine, remote: false } as never, {
      slug: personSlug,
      content: `---\ntitle: Postgres origin-isolated person\ntype: person\ncompany: ${companySlug}\n---\n`,
    });

    const response = await putPage.handler({ engine, remote: false } as never, {
      slug: personSlug,
      content: '---\ntitle: Postgres origin-isolated person\ntype: person\n---\n',
    }) as { auto_links?: { removed: number } };

    expect(response.auto_links?.removed).toBe(1);
    expect(await engine.getLinks(personSlug, { sourceId: 'default' })).toEqual([
      expect.objectContaining({
        to_slug: companySlug,
        link_type: 'works_at',
        link_source: 'frontmatter',
        origin_slug: companySlug,
        origin_field: 'key_people',
      }),
    ]);
  });
});
