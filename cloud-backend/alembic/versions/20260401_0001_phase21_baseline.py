"""phase21 baseline schema

Revision ID: 20260401_0001
Revises:
Create Date: 2026-04-01 00:00:00
"""
from __future__ import annotations

from pathlib import Path

from alembic import op

# revision identifiers, used by Alembic.
revision = "20260401_0001"
down_revision = None
branch_labels = None
depends_on = None


def _sql_path(name: str) -> Path:
    return Path(__file__).resolve().parents[2] / "sql" / name


def _execute_sql_file(name: str) -> None:
    path = _sql_path(name)
    if not path.exists():
        return
    sql = path.read_text(encoding="utf-8")
    for statement in sql.split(";"):
        chunk = statement.strip()
        if not chunk:
            continue
        op.execute(f"{chunk};")


def upgrade() -> None:
    # Baseline migration for additive Phase 21 artifacts plus portable foundation.
    _execute_sql_file("sme_inventory.sql")
    _execute_sql_file("sme_suppliers.sql")
    _execute_sql_file("approvals.sql")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS allocation_approvals")
    op.execute("DROP TABLE IF EXISTS sme_suppliers")
    op.execute("DROP TABLE IF EXISTS sme_inventory_items")
