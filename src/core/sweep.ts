/**
 * Serve-resident maintenance sweep [CX-P0.1, CX-P0.3, CX2-4].
 *
 * Nothing previously ingested the transcript corpus into a live brain
 * (dream is disabled by default; the CLI can't open PGLite under a live
 * serve), and remote `put_page` deliberately skips auto link/timeline
 * extraction — so the graph never compounded from harness writes. The
 * sweep is the serve process's (the lock owner's) bounded, spend-gated
 * answer. Three passes, each individually fail-soft:
 *
 *   1. FACTS-FENCE RECONCILIATION [CX2-4] — zero-LLM. Recently-modified
 *      pages carrying a `## Facts` fence get reconciled into the facts DB
 *      index by the SAME cycle extractor the dream cycle uses
 *      (src/core/cycle/extract-facts.ts:runExtractFacts, scoped via
 *      opts.slugs). Fence rows carry explicit per-row visibility; the
 *      corpus pass below resolves unset visibility through
 *      resolveDefaultVisibility inside the shared pipeline (backstop.ts).
 *
 *   2. LINK/TIMELINE EXTRACTION [CX-P0.3] — zero-LLM, deterministic. The
 *      same per-page cores `gbrain extract links|timeline --source db`
 *      runs: extractPageLinks + parseTimelineEntries, endpoint-validated
 *      through resolveCandidateSources, reconciled against managed links,
 *      and written alongside addTimelineEntriesBatch before watermarking.
 *
 *   3. CORPUS INGEST [CX-P0.1] — LLM-backed, spend-gated. Unprocessed
 *      `.txt` files in the dream corpus dir run through the narrowest
 *      one-transcript entry (runFactsPipeline: extract → resolve → dedup
 *      → insert). KEYLESS RULE [CX-P0.5]: no extraction provider ⇒ skip
 *      with {reason:'keyless'} — agent-authored fences cover it. A
 *      `<file>.ingested` sidecar (written AFTER success — crash-safe,
 *      exactly-once) marks completion; a `<file>.in-progress` claim
 *      sidecar (O_EXCL) fences concurrent sweeps off the same file so
 *      two processes never double-pay one transcript's LLM call.
 *
 * Budget: a wall-clock budget aborts BETWEEN items (and threads an
 * AbortSignal into the fence pass + corpus extraction); the report
 * carries the partial counts. runMaintenanceSweep NEVER throws.
 *
 * Heavy dependencies (cycle extractor, extract command cores, facts
 * pipeline, gateway) are lazy-imported inside each pass — the
 * backstop.ts:335 precedent — so importing this module at serve boot is
 * cheap and a broken optional dep degrades to a skip, never a crash.
 */

import { join } from 'node:path';
import { readdir, readFile, writeFile, rm, stat } from 'node:fs/promises';
import type { BrainEngine, LinkBatchInput, TimelineBatchInput } from './engine.ts';
import type { FactsBackstopCtx } from './facts/backstop.ts';
import { detectCapabilities, type CapabilityReport } from './capability.ts';
import { buildLinkRows } from './batch-rows.ts';
import { executeRawJsonb } from './sql-query.ts';

/** Delay before the serve-startup sweep fires (post-connect settle). */
export const STARTUP_SWEEP_DELAY_MS = 3_000;

/** Fence begin marker — duplicated from facts-fence.ts to keep this module light. */
const FACTS_FENCE_BEGIN_MARKER = 'gbrain:facts:begin';

/**
 * Cap on the candidate set the pass-1 leading-wildcard LIKE scans: the fence
 * marker can't use an index, so the LIKE runs over a recency-bounded subquery
 * (newest N rows in the window) instead of every matching-window row — a
 * bulk-sync day can't turn pass 1 into a whole-table scan.
 */
export const FENCE_LIKE_SCAN_CAP = 500;

/** Prefix for the durable, source-qualified pass-2 continuation cursor. */
const LINKS_TIMELINE_CURSOR_KEY_PREFIX = 'sweep.links_timeline.cursor.v1.';

/** Sidecar suffix marking a corpus file as processed. */
export const CORPUS_INGESTED_SUFFIX = '.ingested';

/**
 * Claim sidecar marking a corpus file as being ingested RIGHT NOW. Created
 * with O_EXCL (`wx`) before the LLM call so a manual `gbrain sweep --once`
 * and the serve-idle sweep (separate processes on a Postgres brain) can't
 * both pay for the same transcript. Replaced by the `.ingested` sidecar on
 * success; removed on failure so the next sweep retries.
 */
export const CORPUS_CLAIM_SUFFIX = '.in-progress';

/** Claims older than this belong to dead sweeps and are reclaimable. */
export const CORPUS_CLAIM_STALE_MS = 60 * 60 * 1000;

export interface SweepOpts {
  /** Source to sweep. Default 'default' (the serve's registered source). */
  sourceId?: string;
  /** Max pages per pass / corpus files per sweep. Default 20. */
  batchLimit?: number;
  /** Wall-clock budget; the sweep stops between items when exceeded. Default 5000. */
  budgetMs?: number;
  /** Recency window (days) for "recently-modified pages". Default 7. */
  recentDays?: number;
  /** Diagnostic sink (stderr in serve contexts). Default: silent. */
  log?: (msg: string) => void;
  /**
   * Capability report override (test seam / caller already computed one).
   * Default: detectCapabilities() — config-plane, no network.
   */
  capabilities?: CapabilityReport;
}

export interface SweepSkip {
  reason: string;
  count: number;
}

export interface SweepReport {
  corpusIngested: number;
  factsReconciled: number;
  linksExtracted: number;
  linksRemoved: number;
  timelineExtracted: number;
  skipped: SweepSkip[];
  durationMs: number;
}

/**
 * Run one bounded maintenance sweep. Never throws — failures land in
 * report.skipped with a per-pass reason.
 */
export async function runMaintenanceSweep(
  engine: BrainEngine,
  opts: SweepOpts = {},
): Promise<SweepReport> {
  const started = Date.now();
  const sourceId = opts.sourceId ?? 'default';
  const batchLimit = Math.max(1, opts.batchLimit ?? 20);
  const budgetMs = Math.max(0, opts.budgetMs ?? 5_000);
  const recentDays = Math.max(1, opts.recentDays ?? 7);
  const log = opts.log ?? (() => {});
  const deadline = started + budgetMs;

  const report: SweepReport = {
    corpusIngested: 0,
    factsReconciled: 0,
    linksExtracted: 0,
    linksRemoved: 0,
    timelineExtracted: 0,
    skipped: [],
    durationMs: 0,
  };

  const skip = (reason: string, count = 1): void => {
    if (count <= 0) return;
    const existing = report.skipped.find(s => s.reason === reason);
    if (existing) existing.count += count;
    else report.skipped.push({ reason, count });
  };
  const overBudget = () => Date.now() >= deadline;

  // Budget abort signal: threads into the fence pass's per-page loop and
  // the corpus extraction's network call so a long item can be interrupted
  // at its own checkpoints. unref'd — the sweep must never hold the
  // process open (the serve unref convention).
  const budgetController = new AbortController();
  const budgetTimer = setTimeout(
    () => budgetController.abort(),
    Math.max(0, deadline - Date.now()),
  );
  budgetTimer.unref?.();

  const cutoffIso = new Date(started - recentDays * 86_400_000).toISOString();

  try {
    // ── Pass 1: facts-fence reconciliation [CX2-4] — zero-LLM ─────────
    try {
      if (overBudget()) {
        skip('budget_exhausted:facts_fence');
      } else {
        // The leading-wildcard LIKE can't use an index, so it scans only the
        // newest FENCE_LIKE_SCAN_CAP rows in the recency window (inner
        // subquery) rather than every row a bulk-sync day may have touched.
        const rows = await engine.executeRaw<{ slug: string }>(
          `SELECT slug FROM (
             SELECT slug, compiled_truth, updated_at FROM pages
              WHERE source_id = $1
                AND deleted_at IS NULL
                AND updated_at >= $2::timestamptz
              ORDER BY updated_at DESC
              LIMIT $4
           ) AS recent
            WHERE compiled_truth LIKE $3
            ORDER BY updated_at DESC
            LIMIT $5`,
          [sourceId, cutoffIso, `%${FACTS_FENCE_BEGIN_MARKER}%`, FENCE_LIKE_SCAN_CAP, batchLimit],
        );
        if (rows.length > 0) {
          const { runExtractFacts } = await import('./cycle/extract-facts.ts');
          const r = await runExtractFacts(engine, {
            slugs: rows.map(row => row.slug),
            sourceId,
            signal: budgetController.signal,
          });
          report.factsReconciled = r.factsInserted;
          if (r.guardTriggered) skip('facts_fence_guard');
          if (budgetController.signal.aborted) skip('budget_exhausted:facts_fence');
        }
      }
    } catch (e) {
      skip('facts_fence_error');
      log(`[sweep] facts-fence pass failed: ${e instanceof Error ? e.message : String(e)}`);
    }

    // ── Pass 2: link/timeline extraction [CX-P0.3] — zero-LLM ─────────
    try {
      if (overBudget()) {
        skip('budget_exhausted:links_timeline');
      } else {
        await runLinksTimelinePass(engine, {
          sourceId,
          batchLimit,
          cutoffIso,
          overBudget,
          report,
          skip,
          log,
        });
      }
    } catch (e) {
      skip('links_timeline_error');
      log(`[sweep] links/timeline pass failed: ${e instanceof Error ? e.message : String(e)}`);
    }

    // ── Pass 3: corpus ingest [CX-P0.1] — spend-gated [CX-P0.5] ───────
    try {
      if (overBudget()) {
        skip('budget_exhausted:corpus');
      } else {
        await runCorpusIngestPass(engine, {
          sourceId,
          batchLimit,
          overBudget,
          signal: budgetController.signal,
          capabilities: opts.capabilities,
          report,
          skip,
          log,
        });
      }
    } catch (e) {
      skip('corpus_error');
      log(`[sweep] corpus pass failed: ${e instanceof Error ? e.message : String(e)}`);
    }
  } catch (e) {
    // Structural failure outside every per-pass catch (should be
    // unreachable). The never-throw contract holds regardless.
    skip('sweep_error');
    log(`[sweep] sweep failed: ${e instanceof Error ? e.message : String(e)}`);
  } finally {
    clearTimeout(budgetTimer);
  }

  report.durationMs = Date.now() - started;
  return report;
}

interface PassCtx {
  sourceId: string;
  batchLimit: number;
  overBudget: () => boolean;
  report: SweepReport;
  skip: (reason: string, count?: number) => void;
  log: (msg: string) => void;
}

interface LinksTimelineCursor {
  updatedAt: string;
  id: number;
}

function linksTimelineCursorKey(sourceId: string): string {
  // Config is brain-global, while the sweep is source-scoped. Encoding keeps
  // arbitrary source ids from changing the key structure.
  return LINKS_TIMELINE_CURSOR_KEY_PREFIX + encodeURIComponent(sourceId);
}

function parseLinksTimelineCursor(value: string | null): LinksTimelineCursor | null {
  if (!value) return null;
  try {
    const parsed: unknown = JSON.parse(value);
    if (
      typeof parsed !== 'object' || parsed === null ||
      !('updatedAt' in parsed) || !('id' in parsed)
    ) return null;
    const { updatedAt, id } = parsed as { updatedAt?: unknown; id?: unknown };
    if (
      typeof updatedAt !== 'string' || !Number.isFinite(Date.parse(updatedAt)) ||
      typeof id !== 'number' || !Number.isInteger(id) || id < 1
    ) return null;
    return { updatedAt, id };
  } catch {
    return null;
  }
}

/**
 * Select at most one bounded batch, continuing after the prior page cursor.
 * The first seek walks older rows in the normal newest-first order. If it
 * reaches the end, the one allowed wrap query starts at the cursor itself so
 * an unstamped failed page is retried before newer rows can monopolise it.
 */
async function selectLinksTimelineCandidates(
  engine: BrainEngine,
  sourceId: string,
  cutoffIso: string,
  batchLimit: number,
  cursor: LinksTimelineCursor | null,
): Promise<Array<{ id: number; slug: string; updated_at_iso: string }>> {
  if (!cursor) {
    return engine.executeRaw<{ id: number; slug: string; updated_at_iso: string }>(
      `SELECT id, slug,
              to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS updated_at_iso
         FROM pages
        WHERE source_id = $1
          AND deleted_at IS NULL
          AND updated_at >= $2::timestamptz
          AND (links_extracted_at IS NULL OR updated_at > links_extracted_at)
        ORDER BY updated_at DESC, id DESC
        LIMIT $3`,
      [sourceId, cutoffIso, batchLimit],
    );
  }
  const select = async (
    predicate: string,
    order: 'ASC' | 'DESC',
    limit: number,
  ): Promise<Array<{ id: number; slug: string; updated_at_iso: string }>> =>
    engine.executeRaw<{ id: number; slug: string; updated_at_iso: string }>(
      `SELECT id, slug,
              to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS updated_at_iso
         FROM pages
        WHERE source_id = $1
          AND deleted_at IS NULL
          AND updated_at >= $2::timestamptz
          AND (links_extracted_at IS NULL OR updated_at > links_extracted_at)
          AND ${predicate}
        ORDER BY updated_at ${order}, id ${order}
        LIMIT $5`,
      [sourceId, cutoffIso, cursor.updatedAt, cursor.id, limit],
    );

  const older = await select(
    '(updated_at < $3::timestamptz OR (updated_at = $3::timestamptz AND id < $4::integer))',
    'DESC',
    batchLimit,
  );
  if (older.length === batchLimit) return older;

  // The bounded wrap includes an unstamped cursor row, which makes failed
  // pages retryable without letting a newest persistent failure pin every
  // later sweep.
  const wrapped = await select(
    '(updated_at > $3::timestamptz OR (updated_at = $3::timestamptz AND id >= $4::integer))',
    'DESC',
    batchLimit - older.length,
  );
  // A concurrent edit can move an older-arm row across the cursor between
  // the two reads. Keep this pass bounded and idempotent by attempting each
  // page at most once; a short batch simply resumes on the next sweep.
  const seen = new Set(older.map(row => row.id));
  return [...older, ...wrapped.filter(row => !seen.has(row.id))];
}

/**
 * Pass 2 body. Calls the SAME per-page cores as `gbrain extract
 * links|timeline --source db` (extract.ts:extractLinksFromDB /
 * extractTimelineFromDB): extractPageLinks + parseTimelineEntries, with
 * resolveCandidateSources doing the multi-source endpoint validation and
 * the selected revision advancing links_extracted_at only after both kinds
 * complete (the shared watermark contract from extract.ts C3/D6).
 */
async function runLinksTimelinePass(
  engine: BrainEngine,
  ctx: PassCtx & { cutoffIso: string },
): Promise<void> {
  const { sourceId, batchLimit, cutoffIso, overBudget, report, skip, log } = ctx;

  const {
    extractPageLinks,
    parseTimelineEntries,
    makeResolver,
    isGlobalBasenameEnabled,
    isAutoLinkEnabled,
    isAutoTimelineEnabled,
  } = await import('./link-extraction.ts');

  // Respect the same operator kill switches put_page's inline hooks honor.
  const [linksEnabled, timelineEnabled] = await Promise.all([
    isAutoLinkEnabled(engine),
    isAutoTimelineEnabled(engine),
  ]);
  if (!linksEnabled) skip('auto_link_disabled');
  if (!timelineEnabled) skip('auto_timeline_disabled');
  if (!linksEnabled && !timelineEnabled) return;

  const cursorKey = linksTimelineCursorKey(sourceId);
  let cursor: LinksTimelineCursor | null = null;
  try {
    cursor = parseLinksTimelineCursor(await engine.getConfig(cursorKey));
  } catch (e) {
    skip('links_timeline_cursor_read_error');
    log(
      `[sweep] failed to read links/timeline cursor for ${sourceId}: ` +
      (e instanceof Error ? e.message : String(e)),
    );
  }
  const recent = await selectLinksTimelineCandidates(
    engine, sourceId, cutoffIso, batchLimit, cursor,
  );
  if (recent.length === 0) return;

  // resolveCandidateSources is the shared helper the extract command exports
  // precisely so sibling walkers cannot drift from its F10 multi-source
  // resolution (see extract.ts:114).
  const { resolveCandidateSources } = await import('../commands/extract.ts');

  const resolver = makeResolver(engine, { mode: 'batch', sourceId });
  const globalBasename = await isGlobalBasenameEnabled(engine);

  type Extracted = Awaited<ReturnType<typeof extractPageLinks>>;

  const tlBatch: TimelineBatchInput[] = [];
  const processedRefs: Array<{ slug: string; source_id: string; extractedAt: string }> = [];
  const pageCandidates: Array<{ slug: string; candidates: Extracted['candidates'] }> = [];
  let extractionBudgetStopped = false;
  let lastAttempted: LinksTimelineCursor | null = null;

  // Phase 1: per-page extraction. The per-slug getPage loop stays a loop —
  // BrainEngine has no batch read-by-slug-list primitive (resolveSlugsByPaths
  // is path→slug only), and the loop is bounded by batchLimit (default 20).
  for (let i = 0; i < recent.length; i++) {
    if (overBudget()) {
      skip('budget_exhausted:links_timeline', recent.length - i);
      extractionBudgetStopped = true;
      break;
    }
    const slug = recent[i].slug;
    // Advance the round-robin cursor only after a real attempt. This keeps a
    // failed page unstamped and retryable, while its older neighbours get a
    // bounded turn on the next sweep instead of being pinned behind it.
    lastAttempted = { updatedAt: recent[i].updated_at_iso, id: recent[i].id };
    try {
      const page = await engine.getPage(slug, { sourceId });
      if (!page) continue;

      const fullContent = page.compiled_truth + '\n' + page.timeline;
      const pageCandidatesForSlug: Extracted['candidates'] = [];
      const pageTimelineRows: TimelineBatchInput[] = [];

      if (linksEnabled) {
        // skipFrontmatter matches user-invoked `gbrain extract links`
        // (frontmatter backfill stays a migration-orchestrator concern).
        const extracted = await extractPageLinks(
          slug, fullContent, page.frontmatter, page.type, resolver,
          { skipFrontmatter: true, globalBasename },
        );
        pageCandidatesForSlug.push(...extracted.candidates);
      }

      if (timelineEnabled) {
        for (const entry of parseTimelineEntries(fullContent)) {
          // Same row shape as extractTimelineFromDB's batch push (extract.ts):
          // no explicit source (engine default applies), detail '' when empty.
          pageTimelineRows.push({
            slug,
            date: entry.date,
            summary: entry.summary,
            detail: entry.detail || '',
            source_id: sourceId,
          });
        }
      }

      if (pageCandidatesForSlug.length > 0) {
        pageCandidates.push({ slug, candidates: pageCandidatesForSlug });
      }
      tlBatch.push(...pageTimelineRows);
      processedRefs.push({
        slug,
        source_id: sourceId,
        // Stamp the exact full-microsecond value selected for this page. If a
        // concurrent write advances updated_at after the SELECT, the older stamp
        // leaves the page stale so a later sweep reconciles the newer content.
        extractedAt: recent[i].updated_at_iso,
      });
    } catch (e) {
      skip('page_extraction_error');
      log(
        `[sweep] page extraction failed for ${sourceId}:${slug}: ` +
        (e instanceof Error ? e.message : String(e)),
      );
    }
  }
  if (lastAttempted) {
    try {
      await engine.setConfig(cursorKey, JSON.stringify(lastAttempted));
    } catch (e) {
      skip('links_timeline_cursor_write_error');
      log(
        `[sweep] failed to write links/timeline cursor for ${sourceId}: ` +
        (e instanceof Error ? e.message : String(e)),
      );
    }
  }
  if (overBudget()) {
    if (!extractionBudgetStopped) {
      skip('budget_exhausted:links_timeline', processedRefs.length || 1);
    }
    return;
  }

  // Phase 2: endpoint validation is scoped to the slugs the candidates
  // actually name — NOT engine.listAllPageRefs() (the full (slug, source_id)
  // map, O(all pages) per sweep; the extract command amortizes that cost over
  // a whole-brain run, a recurring bounded sweep must not). The targeted
  // lookup keeps listAllPageRefs' visibility semantics (deleted_at IS NULL),
  // so resolveCandidateSources' F10 resolution is unchanged — it just sees
  // only the rows it can possibly use. Zero candidates ⇒ zero queries.
  const desiredLinks = new Map<string, LinkBatchInput[]>();
  for (const ref of processedRefs) desiredLinks.set(ref.slug, []);
  if (pageCandidates.length > 0) {
    const needed = new Set<string>();
    for (const { slug, candidates } of pageCandidates) {
      needed.add(slug);
      for (const c of candidates) {
        needed.add(c.targetSlug);
        if (c.fromSlug) needed.add(c.fromSlug);
      }
    }
    const { allSlugs, slugToSources } = await lookupRefsForSlugs(engine, [...needed]);
    for (const { slug, candidates } of pageCandidates) {
      for (const c of candidates) {
        const resolved = resolveCandidateSources(c, slug, sourceId, allSlugs, slugToSources);
        if (!resolved) continue;
        if (resolved.fromSlug !== slug || resolved.fromSourceId !== sourceId) {
          throw new Error(
            `sweep extraction emitted a non-outgoing edge for ${sourceId}:${slug}`,
          );
        }
        desiredLinks.get(slug)!.push({
          from_slug: resolved.fromSlug,
          to_slug: c.targetSlug,
          link_type: c.linkType,
          context: c.context,
          link_source: c.linkSource,
          origin_slug: c.originSlug,
          origin_field: c.originField,
          from_source_id: resolved.fromSourceId,
          to_source_id: resolved.toSourceId,
          origin_source_id: sourceId,
        });
      }
    }
  }

  const reconciledRefs: typeof processedRefs = [];
  let reconciliationBudgetStopped = false;
  if (linksEnabled) {
    for (let i = 0; i < processedRefs.length; i++) {
      if (overBudget()) {
        skip('budget_exhausted:links_timeline', processedRefs.length - i);
        reconciliationBudgetStopped = true;
        break;
      }
      const ref = processedRefs[i];
      try {
        const reconciled = await reconcileSweepLinks(
          engine,
          ref.slug,
          sourceId,
          desiredLinks.get(ref.slug) ?? [],
        );
        report.linksExtracted += reconciled.created;
        report.linksRemoved += reconciled.removed;
        reconciledRefs.push(ref);
      } catch (e) {
        skip('link_reconcile_error');
        log(
          `[sweep] link reconciliation failed for ${sourceId}:${ref.slug}: ` +
          (e instanceof Error ? e.message : String(e)),
        );
      }
    }
  }
  if (overBudget()) {
    if (!reconciliationBudgetStopped) {
      skip('budget_exhausted:links_timeline', processedRefs.length || 1);
    }
    return;
  }
  // Timeline batch primitive self-retries; the default auditSite label applies
  // (BATCH_AUDIT_SITES is a closed enum owned by retry.ts).
  if (tlBatch.length > 0) {
    report.timelineExtracted += await engine.addTimelineEntriesBatch(tlBatch); // gbrain-allow-direct-insert: same extract-path rationale as addLinksBatch above [CX-P0.3]
  }
  // Stamp only when BOTH kinds ran for these pages (extract.ts C3/D6:
  // links_extracted_at covers links AND timeline).
  if (linksEnabled && timelineEnabled && reconciledRefs.length > 0) {
    // Unlike inline extraction, this watermark is the sweep's resume cursor.
    // Fail loudly to the pass-level catch so an unstamped page retries instead
    // of swallowing a stamp failure and re-chewing an invisible partial batch.
    await engine.markPagesExtractedBatch(reconciledRefs, new Date().toISOString());
  }
}

/**
 * Reconcile the sweep-owned outgoing links for one source-qualified page.
 * Manual, frontmatter, custom-provenance, and legacy NULL-provenance rows are
 * outside this sweep's ownership and are never removed.
 */
async function reconcileSweepLinks(
  engine: BrainEngine,
  slug: string,
  sourceId: string,
  desired: LinkBatchInput[],
): Promise<{ created: number; removed: number }> {
  return engine.transaction(async (tx) => {
    // Match runAutoLink's existing lock key so a local put_page and the serve
    // sweep cannot reconcile the same slug concurrently.
    await tx.executeRaw(
      `SELECT pg_advisory_xact_lock(hashtext($1)::bigint)`,
      [`auto_link:${slug}`],
    );

    // This trusted maintenance transaction performs its own exact source
    // predicate below. Bind the RLS defense-in-depth scope to the trusted
    // wildcard so a legitimate cross-source target remains visible. Avoid
    // tx.getLinks(): with RLS binding enabled it opens its own transaction,
    // but postgres.js transaction handles expose savepoint(), not begin().
    await tx.executeRaw(`SELECT set_config('app.scopes', '*', true)`);

    // addLinksBatch owns connection-level retry and must not run inside an
    // outer transaction. Use its shared row builder and one transaction-local
    // statement so the advisory lock, inserts, stale reads, and deletes remain
    // one atomic unit even when the connection fails.
    const created = desired.length > 0
      ? (await executeRawJsonb<{ inserted: number }>(
          tx,
          `INSERT INTO links (
             from_page_id, to_page_id, link_type, context, link_source,
             link_kind, origin_page_id, origin_field
           )
           SELECT f.id, t.id, v.link_type, v.context, v.link_source,
                  v.link_kind, o.id, v.origin_field
             FROM jsonb_to_recordset(($1::jsonb)->'rows') AS v(
               from_slug text, to_slug text, link_type text, context text,
               link_source text, origin_slug text, origin_field text,
               from_source_id text, to_source_id text,
               origin_source_id text, link_kind text
             )
             JOIN pages f
               ON f.slug = v.from_slug AND f.source_id = v.from_source_id
             JOIN pages t
               ON t.slug = v.to_slug AND t.source_id = v.to_source_id
             LEFT JOIN pages o
               ON o.slug = v.origin_slug AND o.source_id = v.origin_source_id
           ON CONFLICT (
             from_page_id, to_page_id, link_type, link_source, origin_page_id
           ) DO NOTHING
           RETURNING 1 AS inserted`,
          [],
          [{ rows: buildLinkRows(desired) }],
        )).length
      : 0;

    const managed = await tx.executeRaw<{
      id: number;
      to_slug: string;
      to_source_id: string;
      link_type: string;
      link_source: string;
    }>(
      `SELECT l.id, t.slug AS to_slug, t.source_id AS to_source_id,
              l.link_type, l.link_source
         FROM links l
         JOIN pages f ON f.id = l.from_page_id
         JOIN pages t ON t.id = l.to_page_id
        WHERE f.slug = $1
          AND f.source_id = $2
          AND l.link_source IN ('markdown', 'wikilink-resolved')
          AND l.origin_page_id IS NULL
          AND t.deleted_at IS NULL`,
      [slug, sourceId],
    );
    const keyForDesired = (link: LinkBatchInput): string =>
      `${link.to_source_id || 'default'}\u0000${link.to_slug}\u0000${link.link_type || ''}\u0000${link.link_source || 'markdown'}`;
    const keyForExisting = (link: (typeof managed)[number]): string =>
      `${link.to_source_id}\u0000${link.to_slug}\u0000${link.link_type}\u0000${link.link_source}`;
    const desiredKeys = new Set(desired.map(keyForDesired));
    const staleIds = managed
      .filter(link => !desiredKeys.has(keyForExisting(link)))
      .map(link => link.id);
    const removed = staleIds.length > 0
      ? (await tx.executeRaw<{ id: number }>(
          `DELETE FROM links l
            USING pages t
           WHERE l.id = ANY($1::int[])
             AND t.id = l.to_page_id
             AND t.deleted_at IS NULL
           RETURNING l.id AS id`,
          [staleIds],
        )).length
      : 0;

    return { created, removed };
  });
}

/**
 * (slug, source_id) refs for EXACTLY the given slugs, chunked IN-list —
 * the bounded replacement for listAllPageRefs in the sweep's pass 2. Same
 * visibility as listAllPageRefs (deleted_at IS NULL).
 */
async function lookupRefsForSlugs(
  engine: BrainEngine,
  slugs: string[],
): Promise<{ allSlugs: Set<string>; slugToSources: Map<string, string[]> }> {
  const allSlugs = new Set<string>();
  const slugToSources = new Map<string, string[]>();
  const CHUNK = 200;
  for (let i = 0; i < slugs.length; i += CHUNK) {
    const chunk = slugs.slice(i, i + CHUNK);
    const placeholders = chunk.map((_, j) => `$${j + 1}`).join(', ');
    const rows = await engine.executeRaw<{ slug: string; source_id: string }>(
      `SELECT slug, source_id FROM pages
        WHERE deleted_at IS NULL AND slug IN (${placeholders})`,
      chunk,
    );
    for (const ref of rows) {
      allSlugs.add(ref.slug);
      const list = slugToSources.get(ref.slug) ?? [];
      list.push(ref.source_id);
      slugToSources.set(ref.slug, list);
    }
  }
  return { allSlugs, slugToSources };
}

/**
 * Pass 3 body. One `runFactsPipeline` call per unprocessed corpus file —
 * the narrowest existing entry that takes raw transcript text through
 * extract → resolve → dedup → insert. Visibility left unset so the
 * pipeline resolves the operator default via resolveDefaultVisibility
 * (backstop.ts:359, [ENG-8]). Sidecar written AFTER success only.
 *
 * Concurrency: a `<file>.in-progress` claim sidecar (O_EXCL create) fences
 * each file before its LLM call — a manual `gbrain sweep --once` racing the
 * serve-idle sweep never double-spends on the same transcript. Success
 * replaces the claim with the `.ingested` sidecar; failure removes the claim
 * (next sweep retries); claims older than CORPUS_CLAIM_STALE_MS belong to
 * dead sweeps and are reclaimed.
 */
async function runCorpusIngestPass(
  engine: BrainEngine,
  ctx: PassCtx & {
    signal: AbortSignal;
    capabilities?: CapabilityReport;
    log: (msg: string) => void;
  },
): Promise<void> {
  const { sourceId, batchLimit, overBudget, signal, report, skip, log } = ctx;

  // Corpus dir: dream's session corpus (transcripts.ts:66 precedent);
  // default ~/.gbrain/transcripts/corpus (GBRAIN_HOME-aware via configDir).
  let dir = await engine.getConfig('dream.synthesize.session_corpus_dir');
  if (!dir) {
    const { configDir } = await import('./config.ts');
    dir = join(configDir(), 'transcripts', 'corpus');
  }

  let entries: string[];
  try {
    entries = await readdir(dir);
  } catch {
    return; // no corpus dir = nothing to ingest (not an error)
  }

  // ONE readdir feeds both the .txt listing and the sidecar checks (the old
  // shape ran two existsSync probes per file on top of the readdir).
  const entrySet = new Set(entries);
  const txtFiles = entries.filter(n => n.endsWith('.txt')).sort();
  if (txtFiles.length === 0) return;

  const alreadyIngested = txtFiles.filter(n => entrySet.has(n + CORPUS_INGESTED_SUFFIX));
  skip('already_ingested', alreadyIngested.length);

  const candidates = txtFiles
    .filter(n => !entrySet.has(n + CORPUS_INGESTED_SUFFIX))
    .slice(0, batchLimit);
  if (candidates.length === 0) return;

  // [CX-P0.5] Keyless rule: no extraction provider configured ⇒ skip the
  // whole pass. Agent-authored fences (pass 1) carry keyless memory.
  const caps = ctx.capabilities ?? detectCapabilities();
  if (!caps.extraction.available) {
    skip('keyless', candidates.length);
    return;
  }

  // Existing spend gate: operators flip facts.extraction_enabled off to
  // stop ALL fact extraction brain-wide (facts/extract.ts:43).
  const { isFactsExtractionEnabled } = await import('./facts/extract.ts');
  if (!(await isFactsExtractionEnabled(engine))) {
    skip('extraction_disabled', candidates.length);
    return;
  }

  const { runFactsPipeline } = await import('./facts/backstop.ts');
  const { isDreamOutput } = await import('./cycle/transcript-discovery.ts');

  for (let i = 0; i < candidates.length; i++) {
    if (overBudget()) {
      skip('budget_exhausted:corpus', candidates.length - i);
      break;
    }
    const name = candidates[i];
    const full = join(dir, name);

    // Atomic claim BEFORE any spend — the losing sweep skips, never re-pays.
    const claimPath = full + CORPUS_CLAIM_SUFFIX;
    if (!(await acquireCorpusClaim(claimPath))) {
      skip('corpus_in_progress');
      continue;
    }

    let abortLoop = false;
    try {
      // Re-check under the claim: another sweep may have finished this file
      // between our readdir and our claim (it releases its claim only after
      // writing the .ingested sidecar, so this closes the double-spend gap).
      const doneAlready = await stat(full + CORPUS_INGESTED_SUFFIX).then(() => true, () => false);
      if (doneAlready) {
        skip('already_ingested');
        continue;
      }

      const raw = await readFile(full, 'utf-8');

      // Anti-loop: never ingest dream-generated outputs. Marking them
      // processed is safe — the classification is deterministic and
      // permanent, and the sidecar stops the sweep re-reading them forever.
      if (isDreamOutput(raw)) {
        await writeFile(
          full + CORPUS_INGESTED_SUFFIX,
          JSON.stringify({ ingested_at: new Date().toISOString(), skipped: 'dream_output' }) + '\n',
        );
        skip('dream_output');
        continue;
      }

      const r = await runFactsPipeline(raw, {
        engine,
        sourceId,
        sessionId: `sweep:corpus:${name}`,
        // Provenance tag outside FactsBackstopCtx's enumerated writers —
        // facts.source is free text at the DB layer; the cast only
        // side-steps the ctx union, which predates the sweep.
        source: 'sweep:corpus' as FactsBackstopCtx['source'],
        mode: 'inline',
        remote: false,
        abortSignal: signal,
        // visibility deliberately unset → resolveDefaultVisibility [ENG-8]
      });

      // Sidecar AFTER success — a crash before this line re-processes the
      // file next sweep (dedup absorbs the repeats), never loses it.
      await writeFile(
        full + CORPUS_INGESTED_SUFFIX,
        JSON.stringify({
          ingested_at: new Date().toISOString(),
          facts_inserted: r.inserted,
          facts_duplicate: r.duplicate,
        }) + '\n',
      );
      report.corpusIngested += 1;
    } catch (e) {
      if (e instanceof Error && e.name === 'AbortError') {
        skip('budget_exhausted:corpus', candidates.length - i);
        abortLoop = true;
      } else {
        skip('corpus_file_error');
        log(`[sweep] corpus ingest failed for ${name}: ${e instanceof Error ? e.message : String(e)}`);
      }
    } finally {
      // Success (its .ingested sidecar now stands in) and failure (retry next
      // sweep) both release the claim.
      await rm(claimPath, { force: true }).catch(() => {});
    }
    if (abortLoop) break;
  }
}

/**
 * Try to claim a corpus file for ingestion. O_EXCL (`wx`) create is the
 * atomic primitive; a live existing claim (< CORPUS_CLAIM_STALE_MS old)
 * means another sweep owns the file. Stale claims are removed and re-raced
 * (one contender wins the second `wx`). Never throws.
 */
async function acquireCorpusClaim(claimPath: string): Promise<boolean> {
  const body = JSON.stringify({ claimed_at: new Date().toISOString(), pid: process.pid }) + '\n';
  const tryCreate = () =>
    writeFile(claimPath, body, { flag: 'wx', mode: 0o600 }).then(() => true, () => false);

  if (await tryCreate()) return true;
  try {
    const st = await stat(claimPath);
    if (Date.now() - st.mtimeMs <= CORPUS_CLAIM_STALE_MS) return false; // live claim
  } catch {
    // Claim vanished between wx-create and stat — its owner just released.
    // Treat as contended; a later sweep picks the file up if still needed.
    return false;
  }
  await rm(claimPath, { force: true }).catch(() => {});
  return tryCreate();
}

// ── Serve-startup arming [ENG-5] ─────────────────────────────────────────

export interface StartupSweepOpts {
  /** Post-connect settle delay. Default STARTUP_SWEEP_DELAY_MS (3s). */
  delayMs?: number;
  /** Source to sweep. Default 'default'. */
  sourceId?: string;
  /** Env for the GBRAIN_SWEEP kill switch. Default process.env. */
  env?: Record<string, string | undefined>;
  /** Timer seams (tests). Defaults: global setTimeout/clearTimeout. */
  setTimeoutFn?: (fn: () => void, ms: number) => unknown;
  clearTimeoutFn?: (handle: unknown) => void;
  /** Sweep body override (tests). Default: runMaintenanceSweep. */
  sweep?: (engine: BrainEngine) => Promise<unknown>;
}

/**
 * Arm the one-shot startup sweep for a serve process. Returns a cancel
 * handle for shutdown, or null when the GBRAIN_SWEEP=0 kill switch is set.
 * The timer is unref'd (never holds the process open) and the sweep body
 * swallows every error — best-effort by construction, same posture as the
 * resolve-IPC block in src/mcp/server.ts.
 */
export function armStartupSweep(
  engine: BrainEngine,
  opts: StartupSweepOpts = {},
): { cancel: () => void } | null {
  const env = opts.env ?? process.env;
  if (env.GBRAIN_SWEEP === '0') return null;

  const setT = opts.setTimeoutFn ?? ((fn: () => void, ms: number) => setTimeout(fn, ms));
  const clearT = opts.clearTimeoutFn
    ?? ((h: unknown) => clearTimeout(h as ReturnType<typeof setTimeout>));
  const run = opts.sweep
    ?? ((e: BrainEngine) => runMaintenanceSweep(e, { sourceId: opts.sourceId }));

  const handle = setT(() => {
    Promise.resolve()
      .then(() => run(engine))
      .catch(() => { /* startup sweep is best-effort; never crash serve */ });
  }, opts.delayMs ?? STARTUP_SWEEP_DELAY_MS);
  (handle as { unref?: () => void } | null)?.unref?.();

  return {
    cancel: () => {
      try { clearT(handle); } catch { /* noop */ }
    },
  };
}
