CREATE TABLE IF NOT EXISTS organizations (
    id TEXT PRIMARY KEY,
    legal_name TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS spvs (
    id TEXT PRIMARY KEY,
    parent_org_id TEXT NOT NULL,
    legal_name TEXT NOT NULL,
    code TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(parent_org_id) REFERENCES organizations(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS projects (
    id TEXT PRIMARY KEY,
    spv_id TEXT NOT NULL,
    project_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK(status IN ('ACTIVE', 'ON_HOLD', 'COMPLETED')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(spv_id) REFERENCES spvs(id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_spvs_parent_org
ON spvs(parent_org_id);

CREATE INDEX IF NOT EXISTS idx_projects_spv
ON projects(spv_id);
