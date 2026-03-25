CREATE TABLE IF NOT EXISTS brokers (
    id TEXT PRIMARY KEY,
    legal_name TEXT NOT NULL,
    phone TEXT,
    email TEXT,
    pan TEXT,
    default_commission_rate TEXT NOT NULL DEFAULT '0.0200',
    status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK(status IN ('ACTIVE', 'INACTIVE')),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS broker_commissions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    broker_id TEXT NOT NULL,
    booking_id TEXT NOT NULL,
    commission_rate TEXT NOT NULL,
    amount TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING_ALLOCATION' CHECK(status IN ('PENDING_ALLOCATION', 'READY_TO_PAY', 'PAID')),
    entry_id INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(broker_id) REFERENCES brokers(id) ON DELETE RESTRICT,
    FOREIGN KEY(booking_id) REFERENCES sales_bookings(booking_id) ON DELETE RESTRICT,
    FOREIGN KEY(entry_id) REFERENCES journal_entries(id) ON DELETE SET NULL,
    UNIQUE(broker_id, booking_id)
);

CREATE INDEX IF NOT EXISTS idx_broker_commissions_status ON broker_commissions(status, updated_at);
CREATE INDEX IF NOT EXISTS idx_broker_commissions_booking ON broker_commissions(booking_id, status);
