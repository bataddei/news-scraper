-- 0009_activate_sciencedirect_journals.sql
-- Confirmed feed URLs for the two Elsevier/ScienceDirect journals:
--   JFE: https://rss.sciencedirect.com/publication/science/0304405X
--   JFM: https://rss.sciencedirect.com/publication/science/13864181
-- Discovered via <link rel="alternate" type="application/rss+xml"> on each
-- journal's homepage. Feed returns RSS 2.0 with title + author list + abstract
-- + PII (Publisher Item Identifier, usable as external_id).
--
-- RFS (Oxford Academic) and JPM (PM-Research) are left inactive:
--   * Both return HTTP 403 behind Cloudflare/WAF even on homepage fetches
--     with a browser User-Agent. No cleanly reachable RSS endpoint.
--   * Re-enabling requires either a paid API, an academic library proxy,
--     or a browser-automation session to clear the challenge. Out of scope
--     for the current pipeline.
--
-- Idempotent: guarded by the old feed_url value.

update literature.sources
   set feed_url = 'https://rss.sciencedirect.com/publication/science/0304405X',
       active   = true,
       notes    = 'Daily. ScienceDirect RSS, 16 items per refresh. external_id = article PII from the link URL.'
 where slug = 'journal_jfe'
   and feed_url is null;

update literature.sources
   set feed_url = 'https://rss.sciencedirect.com/publication/science/13864181',
       active   = true,
       notes    = 'Daily. ScienceDirect RSS. external_id = article PII from the link URL.'
 where slug = 'journal_jfm'
   and feed_url is null;

-- Keep RFS and JPM inactive; just update the note so we remember why.
update literature.sources
   set notes = 'Blocked by Cloudflare bot-challenge on Oxford Academic (HTTP 403 on homepage + RSS). Re-enable requires library proxy or browser session.'
 where slug = 'journal_rfs'
   and active = false;

update literature.sources
   set notes = 'Blocked by WAF on PM-Research (HTTP 403 on homepage + probed RSS paths). Re-enable requires a paid API or institutional access.'
 where slug = 'journal_jpm'
   and active = false;
