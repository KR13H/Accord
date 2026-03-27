CREATE TABLE IF NOT EXISTS exchange_rates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    currency_code TEXT NOT NULL,
    as_of_date TEXT NOT NULL,
    inr_rate TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'MANUAL',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(currency_code, as_of_date)
);

CREATE INDEX IF NOT EXISTS idx_exchange_rates_lookup
ON exchange_rates(currency_code, as_of_date DESC);
