CREATE TABLE IF NOT EXISTS sales_bookings (
    booking_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    customer_name TEXT,
    unit_code TEXT,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rera_allocation_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    booking_id TEXT NOT NULL,
    payment_reference TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK(event_type IN ('PAYMENT', 'REFUND')),
    receipt_amount TEXT NOT NULL,
    applied_rera_ratio TEXT NOT NULL,
    rera_amount TEXT NOT NULL,
    operations_amount TEXT NOT NULL,
    is_override INTEGER NOT NULL DEFAULT 0 CHECK(is_override IN (0, 1)),
    override_reason TEXT,
    actor_role TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'POSTED' CHECK(status IN ('POSTED', 'FAILED')),
    created_at TEXT NOT NULL,
    FOREIGN KEY(booking_id) REFERENCES sales_bookings(booking_id) ON DELETE RESTRICT,
    UNIQUE(booking_id, payment_reference, event_type)
);

CREATE TABLE IF NOT EXISTS rera_allocation_vouchers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    allocation_event_id INTEGER NOT NULL,
    voucher_kind TEXT NOT NULL CHECK(voucher_kind IN ('RERA_TRANSFER', 'OPERATIONS_TRANSFER')),
    from_account TEXT NOT NULL,
    to_account TEXT NOT NULL,
    amount TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(allocation_event_id) REFERENCES rera_allocation_events(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS rera_allocation_idempotency (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    idempotency_key TEXT NOT NULL UNIQUE,
    request_hash TEXT NOT NULL,
    allocation_event_id INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(allocation_event_id) REFERENCES rera_allocation_events(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_rera_allocation_events_booking
ON rera_allocation_events(booking_id);

CREATE INDEX IF NOT EXISTS idx_rera_allocation_vouchers_event
ON rera_allocation_vouchers(allocation_event_id);

CREATE INDEX IF NOT EXISTS idx_rera_allocation_idempotency_event
ON rera_allocation_idempotency(allocation_event_id);
