-- 0008_fix_allquant_feed_url.sql
-- AllQuant is Wix-hosted, not WordPress. The WordPress-convention `/feed/` URL
-- 404s; the actual feed is exposed via a `<link rel="alternate" type="application/rss+xml">`
-- on the homepage and lives at /blog-feed.xml. Correcting the seed so the blog
-- collector can read it. Idempotent: only updates rows with the old value.
update literature.sources
   set feed_url = 'https://www.allquant.co/blog-feed.xml'
 where slug = 'blog_allquant'
   and feed_url = 'https://allquant.co/feed/';
