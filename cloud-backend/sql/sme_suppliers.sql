CREATE TABLE IF NOT EXISTS sme_suppliers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    business_id TEXT NOT NULL DEFAULT 'SME-001',
    name TEXT NOT NULL,
    phone TEXT,
    amount_owed NUMERIC NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_sme_suppliers_business_name
ON sme_suppliers (business_id, name);
