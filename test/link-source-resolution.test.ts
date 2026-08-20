import { expect, test } from 'bun:test';
import {
  lookupLinkCandidateSources,
  lookupRecoverableLinkCandidateSources,
} from '../src/core/link-source-resolution.ts';

test('looks up candidate sources in listAllPageRefs order', async () => {
  let query = '';
  const engine = {
    async executeRaw(sql: string) {
      query = sql;
      return [];
    },
  };

  await lookupLinkCandidateSources(engine as never, ['people/alice']);

  expect(query.replace(/\s+/g, ' ').trim()).toContain('ORDER BY source_id, slug');
});

test('recoverable lookup includes soft-deleted candidate endpoints', async () => {
  let query = '';
  const engine = {
    async executeRaw(sql: string) {
      query = sql;
      return sql.includes('deleted_at IS NULL')
        ? [{ slug: 'people/alice', source_id: 'default' }]
        : [
          { slug: 'people/alice', source_id: 'default' },
          { slug: 'people/alice', source_id: 'archive' },
        ];
    },
  };

  const recoverable = await lookupRecoverableLinkCandidateSources(
    engine as never,
    ['people/alice'],
  );

  expect(query.replace(/\s+/g, ' ').trim()).not.toContain('deleted_at IS NULL');
  expect(recoverable.allSlugs).toEqual(new Set(['people/alice']));
  expect(recoverable.slugToSources.get('people/alice')).toEqual(['default', 'archive']);
});
