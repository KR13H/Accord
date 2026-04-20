CREATE TABLE IF NOT EXISTS sme_inventory_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    business_id TEXT NOT NULL DEFAULT 'SME-001',
    item_name TEXT NOT NULL,
    localized_name TEXT,
    factory_serial TEXT,
    system_serial TEXT NOT NULL UNIQUE,
    is_system_generated BOOLEAN NOT NULL DEFAULT 0,
    current_stock NUMERIC NOT NULL DEFAULT 0,
    minimum_stock_level NUMERIC NOT NULL DEFAULT 0,
    unit_price NUMERIC NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_sme_inventory_business_sku
ON sme_inventory_items (business_id, system_serial);

CREATE INDEX IF NOT EXISTS idx_sme_inventory_system_generated
ON sme_inventory_items (business_id, is_system_generated);
