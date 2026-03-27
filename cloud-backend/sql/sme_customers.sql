CREATE TABLE IF NOT EXISTS sme_customers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    business_id TEXT NOT NULL DEFAULT 'SME-001',
    name TEXT NOT NULL,
    phone TEXT,
    outstanding_balance NUMERIC NOT NULL DEFAULT 0 CHECK (outstanding_balance >= 0)
);

CREATE INDEX IF NOT EXISTS idx_sme_customers_business
ON sme_customers (business_id, name);
