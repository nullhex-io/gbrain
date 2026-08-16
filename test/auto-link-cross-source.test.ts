/**
 * runAutoLink must remove managed edges by the exact endpoint sources returned
 * by getLinks. A source-scoped writer can retain a managed edge to a target in
 * another source, even when the target slug also exists in the writer source.
 */

import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { PGLiteEngine } from '../src/core/pglite-engine.ts';
import { runSources } from '../src/commands/sources.ts';
import { operations } from '../src/core/operations.ts';

const WRITER_SOURCE = 'media-corpus';
const WRITER_SLUG = 'notes/source';
const TARGET_SLUG = 'concepts/target';

const putPage = operations.find((operation) => operation.name === 'put_page')!;

describe('put_page auto-link cross-source reconciliation', () => {
  let engine: PGLiteEngine;

  beforeAll(async () => {
    engine = new PGLiteEngine();
    await engine.connect({ engine: 'pglite' } as never);
    await engine.initSchema();
    await runSources(engine, ['add', WRITER_SOURCE, '--no-federated']);
  }, 60_000);

  afterAll(async () => {
    await engine.disconnect();
  }, 60_000);

  test('removes every stale managed target-source variant and preserves manual links', async () => {
    await engine.putPage(WRITER_SLUG, {
      type: 'note', title: 'Writer', compiled_truth: '', timeline: '',
    }, { sourceId: WRITER_SOURCE });
    await engine.putPage(TARGET_SLUG, {
      type: 'concept', title: 'Default target', compiled_truth: '', timeline: '',
    });
    await engine.putPage(TARGET_SLUG, {
      type: 'concept', title: 'Writer-source target', compiled_truth: '', timeline: '',
    }, { sourceId: WRITER_SOURCE });

    await engine.addLink(WRITER_SLUG, TARGET_SLUG, '', 'mentions', 'markdown', undefined, undefined, {
      fromSourceId: WRITER_SOURCE, toSourceId: 'default',
    });
    await engine.addLink(WRITER_SLUG, TARGET_SLUG, '', 'mentions', 'markdown', undefined, undefined, {
      fromSourceId: WRITER_SOURCE, toSourceId: WRITER_SOURCE,
    });
    await engine.addLink(WRITER_SLUG, TARGET_SLUG, '', 'mentions', 'manual', undefined, undefined, {
      fromSourceId: WRITER_SOURCE, toSourceId: 'default',
    });

    const response = await putPage.handler({ engine, remote: false, sourceId: WRITER_SOURCE } as never, {
      slug: WRITER_SLUG,
      content: '---\ntitle: Writer\ntype: note\n---\n\nNo managed references remain.\n',
    }) as { auto_links?: { removed: number } };

    expect(response.auto_links?.removed).toBe(2);
    const links = await engine.getLinks(WRITER_SLUG, { sourceId: WRITER_SOURCE });
    expect(links.filter((link) => link.link_source === 'markdown')).toEqual([]);
    expect(links).toEqual([
      expect.objectContaining({
        to_slug: TARGET_SLUG,
        to_source_id: 'default',
        link_source: 'manual',
      }),
    ]);
  });

  test('removes only a stale legacy NULL-provenance edge', async () => {
    const writerSlug = 'notes/source-legacy-null-provenance';
    const targetSlug = 'concepts/legacy-null-provenance-target';
    await engine.putPage(writerSlug, {
      type: 'note', title: 'Legacy provenance writer', compiled_truth: '', timeline: '',
    }, { sourceId: WRITER_SOURCE });
    await engine.putPage(targetSlug, {
      type: 'concept', title: 'Legacy provenance target', compiled_truth: '', timeline: '',
    });

    // addLink intentionally normalizes an omitted provenance to markdown, so
    // seed the pre-v0.13 row directly. Its endpoint/type tuple intentionally
    // collides with a manual edge to reproduce the reconciliation hazard.
    await engine.executeRaw(
      `INSERT INTO links (from_page_id, to_page_id, link_type, context, link_source)
       SELECT f.id, t.id, $1, '', NULL
       FROM pages f
       JOIN pages t ON t.slug = $3 AND t.source_id = 'default'
       WHERE f.slug = $2 AND f.source_id = $4`,
      ['mentions', writerSlug, targetSlug, WRITER_SOURCE],
    );
    await engine.addLink(writerSlug, targetSlug, '', 'mentions', 'manual', undefined, undefined, {
      fromSourceId: WRITER_SOURCE, toSourceId: 'default',
    });

    const response = await putPage.handler({ engine, remote: false, sourceId: WRITER_SOURCE } as never, {
      slug: writerSlug,
      content: '---\ntitle: Legacy provenance writer\ntype: note\n---\n\nNo managed references remain.\n',
    }) as { auto_links?: { removed: number } };

    expect(response.auto_links?.removed).toBe(1);
    expect(await engine.getLinks(writerSlug, { sourceId: WRITER_SOURCE })).toEqual([
      expect.objectContaining({
        to_slug: targetSlug,
        to_source_id: 'default',
        link_type: 'mentions',
        link_source: 'manual',
      }),
    ]);
  });

  test('removes only the writer-owned frontmatter edge when origins share an endpoint tuple', async () => {
    const personSlug = 'people/origin-isolated-person';
    const companySlug = 'companies/origin-isolated-company';
    await engine.putPage(personSlug, {
      type: 'person', title: 'Origin-isolated person', compiled_truth: '', timeline: '',
    });
    await engine.putPage(companySlug, {
      type: 'company', title: 'Origin-isolated company', compiled_truth: '', timeline: '',
    });

    // A company's incoming key_people edge and a person's outgoing company
    // edge legitimately share endpoints/type/provenance but have different
    // frontmatter origins, so they occupy distinct composite identities.
    await putPage.handler({ engine, remote: false } as never, {
      slug: companySlug,
      content: `---\ntitle: Origin-isolated company\ntype: company\nkey_people:\n  - ${personSlug}\n---\n`,
    });
    await putPage.handler({ engine, remote: false } as never, {
      slug: personSlug,
      content: `---\ntitle: Origin-isolated person\ntype: person\ncompany: ${companySlug}\n---\n`,
    });

    const response = await putPage.handler({ engine, remote: false } as never, {
      slug: personSlug,
      content: '---\ntitle: Origin-isolated person\ntype: person\n---\n',
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

  test('keeps only the local target when the same slug exists in both sources', async () => {
    const writerSlug = 'notes/source-still-references-target';
    await engine.putPage(writerSlug, {
      type: 'note', title: 'Writer with reference', compiled_truth: '', timeline: '',
    }, { sourceId: WRITER_SOURCE });

    await engine.addLink(writerSlug, TARGET_SLUG, '', 'mentions', 'markdown', undefined, undefined, {
      fromSourceId: WRITER_SOURCE, toSourceId: 'default',
    });
    await engine.addLink(writerSlug, TARGET_SLUG, '', 'mentions', 'markdown', undefined, undefined, {
      fromSourceId: WRITER_SOURCE, toSourceId: WRITER_SOURCE,
    });

    const response = await putPage.handler({ engine, remote: false, sourceId: WRITER_SOURCE } as never, {
      slug: writerSlug,
      content: '---\ntitle: Writer with reference\ntype: note\n---\n\nSee [Target](concepts/target).\n',
    }) as { auto_links?: { removed: number } };

    expect(response.auto_links?.removed).toBe(1);
    expect(await engine.getLinks(writerSlug, { sourceId: WRITER_SOURCE })).toEqual([
      expect.objectContaining({
        to_slug: TARGET_SLUG,
        to_source_id: WRITER_SOURCE,
        link_source: 'markdown',
      }),
    ]);
  });

  test('preserves the default-source fallback when no local target exists', async () => {
    const writerSlug = 'notes/source-default-fallback';
    const targetSlug = 'concepts/default-fallback-target';
    await engine.putPage(writerSlug, {
      type: 'note', title: 'Fallback writer', compiled_truth: '', timeline: '',
    }, { sourceId: WRITER_SOURCE });
    await engine.putPage(targetSlug, {
      type: 'concept', title: 'Default fallback target', compiled_truth: '', timeline: '',
    });
    await engine.addLink(writerSlug, targetSlug, '', 'mentions', 'markdown', undefined, undefined, {
      fromSourceId: WRITER_SOURCE, toSourceId: 'default',
    });

    const response = await putPage.handler({ engine, remote: false, sourceId: WRITER_SOURCE } as never, {
      slug: writerSlug,
      content: '---\ntitle: Fallback writer\ntype: note\n---\n\nSee [Fallback target](concepts/default-fallback-target).\n',
    }) as { auto_links?: { removed: number } };

    expect(response.auto_links?.removed).toBe(0);
    expect(await engine.getLinks(writerSlug, { sourceId: WRITER_SOURCE })).toEqual([
      expect.objectContaining({ to_slug: targetSlug, to_source_id: 'default', link_source: 'markdown' }),
    ]);
  });

  test('creates the default-source fallback when no managed edge exists yet', async () => {
    const writerSlug = 'notes/source-creates-default-fallback';
    const targetSlug = 'concepts/created-default-fallback-target';
    await engine.putPage(writerSlug, {
      type: 'note', title: 'Fallback creation writer', compiled_truth: '', timeline: '',
    }, { sourceId: WRITER_SOURCE });
    await engine.putPage(targetSlug, {
      type: 'concept', title: 'Created default fallback target', compiled_truth: '', timeline: '',
    });

    const response = await putPage.handler({ engine, remote: false, sourceId: WRITER_SOURCE } as never, {
      slug: writerSlug,
      content: '---\ntitle: Fallback creation writer\ntype: note\n---\n\nSee [Created fallback target](concepts/created-default-fallback-target).\n',
    }) as { auto_links?: { created: number } };

    expect(response.auto_links?.created).toBe(1);
    expect(await engine.getLinks(writerSlug, { sourceId: WRITER_SOURCE })).toEqual([
      expect.objectContaining({ to_slug: targetSlug, to_source_id: 'default', link_source: 'markdown' }),
    ]);
  });

  test('honors a qualified target source over the local-first fallback', async () => {
    const writerSlug = 'notes/source-qualified-target';
    const targetSlug = 'concepts/qualified-target';
    await engine.putPage(writerSlug, {
      type: 'note', title: 'Qualified writer', compiled_truth: '', timeline: '',
    }, { sourceId: WRITER_SOURCE });
    await engine.putPage(targetSlug, {
      type: 'concept', title: 'Default qualified target', compiled_truth: '', timeline: '',
    });
    await engine.putPage(targetSlug, {
      type: 'concept', title: 'Local qualified target', compiled_truth: '', timeline: '',
    }, { sourceId: WRITER_SOURCE });
    await engine.addLink(writerSlug, targetSlug, '', 'mentions', 'markdown', undefined, undefined, {
      fromSourceId: WRITER_SOURCE, toSourceId: 'default',
    });
    await engine.addLink(writerSlug, targetSlug, '', 'mentions', 'markdown', undefined, undefined, {
      fromSourceId: WRITER_SOURCE, toSourceId: WRITER_SOURCE,
    });

    const response = await putPage.handler({ engine, remote: false, sourceId: WRITER_SOURCE } as never, {
      slug: writerSlug,
      content: '---\ntitle: Qualified writer\ntype: note\n---\n\nSee [[default:concepts/qualified-target]].\n',
    }) as { auto_links?: { removed: number } };

    expect(response.auto_links?.removed).toBe(1);
    expect(await engine.getLinks(writerSlug, { sourceId: WRITER_SOURCE })).toEqual([
      expect.objectContaining({ to_slug: targetSlug, to_source_id: 'default', link_source: 'markdown' }),
    ]);
  });

  test('drops a managed edge for a qualified target source that is missing', async () => {
    const writerSlug = 'notes/source-missing-qualified-target';
    const targetSlug = 'concepts/missing-qualified-target';
    await engine.putPage(writerSlug, {
      type: 'note', title: 'Missing qualified writer', compiled_truth: '', timeline: '',
    }, { sourceId: WRITER_SOURCE });
    await engine.putPage(targetSlug, {
      type: 'concept', title: 'Only local target', compiled_truth: '', timeline: '',
    }, { sourceId: WRITER_SOURCE });
    await engine.addLink(writerSlug, targetSlug, '', 'mentions', 'markdown', undefined, undefined, {
      fromSourceId: WRITER_SOURCE, toSourceId: WRITER_SOURCE,
    });

    const response = await putPage.handler({ engine, remote: false, sourceId: WRITER_SOURCE } as never, {
      slug: writerSlug,
      content: '---\ntitle: Missing qualified writer\ntype: note\n---\n\nSee [[unknown-source:concepts/missing-qualified-target]].\n',
    }) as { auto_links?: { removed: number } };

    expect(response.auto_links?.removed).toBe(1);
    expect(await engine.getLinks(writerSlug, { sourceId: WRITER_SOURCE })).toEqual([]);
  });

  test('drops a managed edge for a qualified target source that is soft-deleted', async () => {
    const writerSlug = 'notes/source-deleted-qualified-target';
    const targetSlug = 'concepts/deleted-qualified-target';
    await engine.putPage(writerSlug, {
      type: 'note', title: 'Deleted qualified writer', compiled_truth: '', timeline: '',
    }, { sourceId: WRITER_SOURCE });
    await engine.putPage(targetSlug, {
      type: 'concept', title: 'Deleted default target', compiled_truth: '', timeline: '',
    });
    await engine.putPage(targetSlug, {
      type: 'concept', title: 'Live local target', compiled_truth: '', timeline: '',
    }, { sourceId: WRITER_SOURCE });
    await engine.softDeletePage(targetSlug, { sourceId: 'default' });
    await engine.addLink(writerSlug, targetSlug, '', 'mentions', 'markdown', undefined, undefined, {
      fromSourceId: WRITER_SOURCE, toSourceId: WRITER_SOURCE,
    });

    const response = await putPage.handler({ engine, remote: false, sourceId: WRITER_SOURCE } as never, {
      slug: writerSlug,
      content: '---\ntitle: Deleted qualified writer\ntype: note\n---\n\nSee [[default:concepts/deleted-qualified-target]].\n',
    }) as { auto_links?: { removed: number } };

    expect(response.auto_links?.removed).toBe(1);
    expect(await engine.getLinks(writerSlug, { sourceId: WRITER_SOURCE })).toEqual([]);
  });

  test('anchors incoming frontmatter edges to the writer source', async () => {
    const companySlug = 'companies/incoming-source-anchor';
    const personSlug = 'people/incoming-source-person';
    await engine.putPage(companySlug, {
      type: 'company', title: 'Default company', compiled_truth: '', timeline: '',
    });
    await engine.putPage(companySlug, {
      type: 'company', title: 'Writer-source company', compiled_truth: '', timeline: '',
    }, { sourceId: WRITER_SOURCE });
    await engine.putPage(personSlug, {
      type: 'person', title: 'Default person', compiled_truth: '', timeline: '',
    });

    const content = [
      '---',
      'title: Writer-source company',
      'type: company',
      'key_people:',
      `  - ${personSlug}`,
      '---',
      '',
      'Body.',
    ].join('\n');
    const first = await putPage.handler({ engine, remote: false, sourceId: WRITER_SOURCE } as never, {
      slug: companySlug,
      content,
    }) as { auto_links?: { created: number; removed: number } };

    expect(first.auto_links).toMatchObject({ created: 1, removed: 0 });
    expect(await engine.getBacklinks(companySlug, { sourceId: WRITER_SOURCE })).toEqual([
      expect.objectContaining({
        from_slug: personSlug,
        from_source_id: 'default',
        to_source_id: WRITER_SOURCE,
        link_source: 'frontmatter',
      }),
    ]);
    expect(await engine.getBacklinks(companySlug, { sourceId: 'default' })).toEqual([]);

    const second = await putPage.handler({ engine, remote: false, sourceId: WRITER_SOURCE } as never, {
      slug: companySlug,
      content,
    }) as { auto_links?: { created: number; removed: number } };
    expect(second.auto_links).toMatchObject({ created: 0, removed: 0 });

    const removed = await putPage.handler({ engine, remote: false, sourceId: WRITER_SOURCE } as never, {
      slug: companySlug,
      content: '---\ntitle: Writer-source company\ntype: company\n---\n\nBody.\n',
    }) as { auto_links?: { removed: number } };
    expect(removed.auto_links?.removed).toBe(1);
    expect(await engine.getBacklinks(companySlug, { sourceId: WRITER_SOURCE })).toEqual([]);
  });
});
