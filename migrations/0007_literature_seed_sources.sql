-- 0007_literature_seed_sources.sql
-- Seed the literature sources catalog. Idempotent: ON CONFLICT (slug) DO NOTHING.
-- Adding a new blog later = a new migration that inserts one more row. Never update/delete here.
--
-- Journals and SSRN are seeded with active=false because their feed URLs still
-- need confirmation on a live run. The gap detector ignores inactive sources,
-- so this prevents false "never_run" alerts until the respective collector is
-- actually wired up and the URL is validated.

insert into literature.sources (slug, name, tier, source_type, base_url, feed_url, active, notes) values

-- ---------- Tier 1: structured, high-signal, API or Atom feed ----------
('arxiv_qfin',
 'arXiv — Quantitative Finance (q-fin)',
 1, 'api',
 'http://export.arxiv.org/api/query',
 null,
 true,
 'Categories: q-fin.TR, q-fin.PM, q-fin.ST, q-fin.CP, q-fin.RM. Daily. external_id = arXiv id (e.g. 2404.12345).'),

-- ---------- Tier 2: curated quant blogs (one collector, feed_urls from DB) ----------
('blog_hudson_thames',
 'Hudson & Thames — blog',
 2, 'rss',
 'https://hudsonthames.org/',
 'https://hudsonthames.org/feed/',
 true,
 'Every 6h. external_id = feed entry GUID.'),

('blog_robot_wealth',
 'Robot Wealth — blog',
 2, 'rss',
 'https://robotwealth.com/',
 'https://robotwealth.com/feed/',
 true,
 'Every 6h.'),

('blog_allquant',
 'AllQuant — blog',
 2, 'rss',
 'https://allquant.co/',
 'https://allquant.co/feed/',
 true,
 'Every 6h. Feed URL to verify during first run.'),

('blog_quantpedia',
 'Quantpedia — blog',
 2, 'rss',
 'https://quantpedia.com/blog/',
 'https://quantpedia.com/feed/',
 true,
 'Every 6h. Feed URL to verify during first run.'),

('blog_quantocracy',
 'Quantocracy — aggregator',
 2, 'rss',
 'https://quantocracy.com/',
 'https://quantocracy.com/feed/',
 true,
 'Every 6h. Aggregates many quant blogs; expect heavy dedup inside the blog collector.'),

-- ---------- Tier 3: journal RSS (seeded inactive until URLs are confirmed) ----------
('journal_jfe',
 'Journal of Financial Economics — RSS',
 3, 'rss',
 'https://www.sciencedirect.com/journal/journal-of-financial-economics',
 null,
 false,
 'Daily when enabled. Feed URL to confirm (ScienceDirect RSS path varies).'),

('journal_rfs',
 'Review of Financial Studies — RSS',
 3, 'rss',
 'https://academic.oup.com/rfs',
 null,
 false,
 'Daily when enabled. Feed URL to confirm.'),

('journal_jpm',
 'Journal of Portfolio Management — RSS',
 3, 'rss',
 'https://www.pm-research.com/content/jpm',
 null,
 false,
 'Daily when enabled. Feed URL to confirm.'),

('journal_jfm',
 'Journal of Financial Markets — RSS',
 3, 'rss',
 'https://www.sciencedirect.com/journal/journal-of-financial-markets',
 null,
 false,
 'Daily when enabled. Feed URL to confirm.'),

-- ---------- Tier 1b: SSRN FEN (last to build; messiest) ----------
('ssrn_fen',
 'SSRN — Financial Economics Network',
 1, 'rss',
 'https://www.ssrn.com/',
 null,
 false,
 'Daily when enabled. SSRN has no clean public API; specific FEN journal feeds to choose before build.')

on conflict (slug) do nothing;
