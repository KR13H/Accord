CREATE TABLE IF NOT EXISTS allocation_approvals (
    allocation_event_id INTEGER PRIMARY KEY,
    maker_admin_id INTEGER NOT NULL,
    checker_admin_id INTEGER NULL,
    status TEXT NOT NULL CHECK(status IN ('PENDING_APPROVAL', 'APPROVED', 'REJECTED')),
    decision_reason TEXT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    decision_at TEXT NULL,
    FOREIGN KEY(allocation_event_id) REFERENCES rera_allocation_events(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_allocation_approvals_status
    ON allocation_approvals(status, updated_at DESC);
