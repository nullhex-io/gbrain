/**
 * Source-aware endpoint resolution shared by extract, the maintenance sweep,
 * and inline put_page reconciliation. Lookup stays bounded to the slugs a
 * caller already extracted; never replace it with a whole-brain scan.
 */

import type { BrainEngine } from './engine.ts';
import type { LinkCandidate } from './link-extraction.ts';

export interface LinkSourceLookup {
  allSlugs: Set<string>;
  slugToSources: Map<string, string[]>;
}

/**
 * Loads active `(slug, source_id)` pairs for exactly the requested slugs.
 * This matches listAllPageRefs visibility without scanning every page.
 */
export async function lookupLinkCandidateSources(
  engine: BrainEngine,
  slugs: Iterable<string>,
): Promise<LinkSourceLookup> {
  const requested = [...new Set(slugs)];
  const allSlugs = new Set<string>();
  const slugToSources = new Map<string, string[]>();
  if (requested.length === 0) return { allSlugs, slugToSources };
  const CHUNK = 200;
  for (let i = 0; i < requested.length; i += CHUNK) {
    const chunk = requested.slice(i, i + CHUNK);
    const placeholders = chunk.map((_, j) => `$${j + 1}`).join(', ');
    const rows = await engine.executeRaw<{ slug: string; source_id: string }>(
      `SELECT slug, source_id FROM pages
        WHERE deleted_at IS NULL AND slug IN (${placeholders})
        ORDER BY source_id, slug`,
      chunk,
    );
    for (const ref of rows) {
      allSlugs.add(ref.slug);
      const sources = slugToSources.get(ref.slug) ?? [];
      sources.push(ref.source_id);
      slugToSources.set(ref.slug, sources);
    }
  }
  return { allSlugs, slugToSources };
}

/**
 * Resolves an extracted candidate to exact endpoint source identities.
 * An explicitly qualified target never falls back to local/default sources.
 */
export function resolveCandidateSources(
  candidate: LinkCandidate,
  pageSlug: string,
  pageSourceId: string,
  lookupOrAllSlugs: LinkSourceLookup | Set<string>,
  legacySlugToSources?: Map<string, string[]>,
): { fromSlug: string; fromSourceId: string; toSourceId: string } | null {
  const lookup: LinkSourceLookup = lookupOrAllSlugs instanceof Set
    ? { allSlugs: lookupOrAllSlugs, slugToSources: legacySlugToSources ?? new Map() }
    : lookupOrAllSlugs;
  const fromSlug = candidate.fromSlug ?? pageSlug;
  if (!lookup.allSlugs.has(candidate.targetSlug) || !lookup.allSlugs.has(fromSlug)) return null;

  const fromSources = lookup.slugToSources.get(fromSlug) ?? [];
  const fromSourceId = fromSources.includes(pageSourceId) ? pageSourceId
    : (fromSources.includes('default') ? 'default' : fromSources[0]);
  if (!fromSourceId) return null;

  const targetSources = lookup.slugToSources.get(candidate.targetSlug) ?? [];
  // Incoming frontmatter edges are authored by the page being processed, but
  // their `fromSlug` names the foreign page. Resolve that foreign endpoint
  // local-first/default as usual while pinning the writer (the TO endpoint) to
  // its actual source. Applying the generic target fallback here instead
  // would look for the writer in the foreign endpoint's source and make a
  // valid cross-source backlink disappear.
  const isIncomingFrontmatter = candidate.linkSource === 'frontmatter'
    && candidate.originSlug === pageSlug
    && candidate.fromSlug != null
    && candidate.fromSlug !== pageSlug
    && candidate.targetSlug === pageSlug;
  if (isIncomingFrontmatter) {
    return targetSources.includes(pageSourceId)
      ? { fromSlug, fromSourceId, toSourceId: pageSourceId }
      : null;
  }

  if (candidate.targetSourceId) {
    return targetSources.includes(candidate.targetSourceId)
      ? { fromSlug, fromSourceId, toSourceId: candidate.targetSourceId }
      : null;
  }
  if (targetSources.includes(fromSourceId)) {
    return { fromSlug, fromSourceId, toSourceId: fromSourceId };
  }
  if (targetSources.includes('default')) {
    return { fromSlug, fromSourceId, toSourceId: 'default' };
  }
  return null;
}
