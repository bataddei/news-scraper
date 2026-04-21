-- 0003_seed_sources.sql
-- Seed the sources catalog. Idempotent: ON CONFLICT (slug) DO NOTHING so re-running is safe.
-- Adding a new source later = a new migration that inserts more rows. Never update/delete rows here.

insert into news_archive.sources (slug, name, tier, source_type, base_url, active, notes) values

-- ---------- Tier 1: authoritative, timestamped, highest signal ----------
('fed_fomc_statements',
 'Federal Reserve — FOMC statements and minutes',
 1, 'scraper',
 'https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm',
 true,
 'FOMC decisions and minutes. Highest-impact macro source. Target cadence: every 4 hours.'),

('fed_speeches',
 'Federal Reserve — speeches',
 1, 'scraper',
 'https://www.federalreserve.gov/newsevents/speeches.htm',
 true,
 'Fed governor and regional president speeches. Target cadence: hourly.'),

('bls_releases',
 'Bureau of Labor Statistics — economic releases',
 1, 'scraper',
 'https://www.bls.gov/bls/newsrels.htm',
 true,
 'CPI, NFP, PPI, Employment Situation. Hourly on release days, daily otherwise.'),

('treasury_press',
 'US Treasury — press releases',
 1, 'rss',
 'https://home.treasury.gov/news/press-releases',
 true,
 'Treasury announcements. Target cadence: hourly.'),

('sec_edgar_mag7',
 'SEC EDGAR — Mag 7 filings',
 1, 'api',
 'https://www.sec.gov/cgi-bin/browse-edgar',
 true,
 'AAPL, MSFT, GOOGL, AMZN, META, NVDA, TSLA filings via EDGAR. Target cadence: every 15 minutes.'),

-- ---------- Tier 2: broad coverage, less authoritative, higher volume ----------
('gdelt_gkg',
 'GDELT Global Knowledge Graph',
 2, 'bulk',
 'https://data.gdeltproject.org/gdeltv2/',
 true,
 'Daily bulk GKG download. Filtered at ingest to Fed/macro/Mag7 themes. Backfill from 2015 on first run.'),

('reuters_top',
 'Reuters — top news RSS',
 2, 'rss',
 'https://www.reuters.com/',
 true,
 'Top news feed. Target cadence: hourly.'),

('reuters_business',
 'Reuters — business RSS',
 2, 'rss',
 'https://www.reuters.com/business/',
 true,
 'Business feed. Target cadence: hourly.'),

('reuters_markets',
 'Reuters — markets RSS',
 2, 'rss',
 'https://www.reuters.com/markets/',
 true,
 'Markets feed. Target cadence: hourly.'),

('ap_business',
 'Associated Press — business RSS',
 2, 'rss',
 'https://apnews.com/hub/business',
 true,
 'AP business feed. Target cadence: hourly.'),

('ap_markets',
 'Associated Press — markets/financial RSS',
 2, 'rss',
 'https://apnews.com/hub/financial-markets',
 true,
 'AP markets feed. Target cadence: hourly.'),

('wire_business_wire',
 'Business Wire press releases',
 2, 'rss',
 'https://www.businesswire.com/',
 true,
 'Company press releases. Filter to Mag 7 + macro-relevant tickers at ingest.'),

('wire_pr_newswire',
 'PR Newswire press releases',
 2, 'rss',
 'https://www.prnewswire.com/',
 true,
 'Company press releases. Filter to Mag 7 + macro-relevant tickers at ingest.'),

('wire_globenewswire',
 'GlobeNewswire press releases',
 2, 'rss',
 'https://www.globenewswire.com/',
 true,
 'Company press releases. Filter to Mag 7 + macro-relevant tickers at ingest.'),

('econ_calendar_ff',
 'ForexFactory economic calendar',
 2, 'scraper',
 'https://www.forexfactory.com/calendar',
 true,
 'Scheduled release metadata so we can tag articles to specific events later.')

on conflict (slug) do nothing;
