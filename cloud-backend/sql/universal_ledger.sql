CREATE TABLE IF NOT EXISTS sme_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    business_id TEXT NOT NULL DEFAULT 'SME-001',
    type TEXT NOT NULL CHECK (type IN ('INCOME', 'EXPENSE')),
    amount NUMERIC NOT NULL CHECK (amount > 0),
    category TEXT NOT NULL,
    payment_method TEXT NOT NULL CHECK (payment_method IN ('Cash', 'UPI')),
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sme_transactions_business_date
ON sme_transactions (business_id, created_at DESC);
