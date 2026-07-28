from prometheus_client import Counter, Histogram

FETCHED = Counter("newsbot_articles_fetched_total", "Fetched articles", ["source"])
PROCESSED = Counter("newsbot_articles_processed_total", "Processed articles")
EVENTS = Counter("newsbot_events_created_total", "Created events")
STAGE_SECONDS = Histogram("newsbot_stage_seconds", "Pipeline stage duration", ["stage"])
