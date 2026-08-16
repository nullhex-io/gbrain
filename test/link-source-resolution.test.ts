import { expect, test } from 'bun:test';
import { lookupLinkCandidateSources } from '../src/core/link-source-resolution.ts';

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
