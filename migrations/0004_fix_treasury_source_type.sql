-- Treasury doesn't expose a dedicated press-releases RSS feed; the collector
-- scrapes the Drupal listing HTML instead. The seed from 0003 used 'rss' as a
-- placeholder — this corrects it so the sources table accurately describes how
-- each source is collected (relied on by the Week 4 integrity report).
update news_archive.sources
set source_type = 'scraper'
where slug = 'treasury_press' and source_type = 'rss';
