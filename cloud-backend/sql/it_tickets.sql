CREATE TABLE IF NOT EXISTS it_tickets (
    id TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    summary TEXT NOT NULL,
    priority TEXT NOT NULL CHECK(priority IN ('low', 'medium', 'high')),
    status TEXT NOT NULL DEFAULT 'OPEN' CHECK(status IN ('OPEN', 'IN_PROGRESS', 'CLOSED')),
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_it_tickets_user_created
ON it_tickets(user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_it_tickets_status_priority
ON it_tickets(status, priority, created_at DESC);
