from prometheus_client import Counter, Gauge, Histogram

FETCHED = Counter("newsbot_articles_fetched_total", "Fetched articles", ["source"])
PROCESSED = Counter("newsbot_articles_processed_total", "Processed articles")
EVENTS = Counter("newsbot_events_created_total", "Created events")
SOURCE_ERRORS = Counter("newsbot_source_errors_total", "Source errors", ["source", "stage"])
EVENT_MERGES = Counter("newsbot_event_merges_total", "Articles merged into events")
RISK_BLOCKED = Counter("newsbot_risk_blocked_total", "Events blocked by risk checks")
PUBLISH_ERRORS = Counter(
    "newsbot_publication_errors_total", "Publication errors", ["platform", "kind"]
)
OUTBOX = Gauge("newsbot_outbox_jobs", "Outbox jobs", ["state"])
LLM_REQUESTS = Counter("newsbot_llm_requests_total", "LLM requests", ["result"])
LLM_TOKENS = Counter("newsbot_llm_tokens_total", "LLM token usage", ["direction"])
STAGE_SECONDS = Histogram("newsbot_stage_seconds", "Pipeline stage duration", ["stage"])
