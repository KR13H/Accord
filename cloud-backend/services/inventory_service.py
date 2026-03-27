from __future__ import annotations

from contextlib import closing
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Callable


MONEY_QUANT = Decimal("0.0001")


class InventoryService:
	"""High-precision inventory service with batch/expiry governance.

	Keeps quantity and valuation at four-decimal precision and validates GST slab
	alignment with the 2026-supported rates.
	"""

	def __init__(
		self,
		*,
		get_conn: Callable[[], Any],
		allowed_hsn_slabs: set[Decimal],
	) -> None:
		self.get_conn = get_conn
		self.allowed_hsn_slabs = {Decimal(str(value)).quantize(MONEY_QUANT) for value in allowed_hsn_slabs}

	def _money_4(self, value: Any) -> Decimal:
		return Decimal(str(value)).quantize(MONEY_QUANT)

	def upsert_batch(
		self,
		*,
		sku_code: str,
		sku_name: str,
		batch_code: str,
		hsn_code: str,
		gst_rate: Decimal,
		quantity: Decimal,
		unit_cost: Decimal,
		expiry_date: date | None,
		created_by: int,
	) -> dict[str, Any]:
		normalized_hsn = hsn_code.strip()
		normalized_rate = self._money_4(gst_rate)
		normalized_qty = self._money_4(quantity)
		normalized_unit_cost = self._money_4(unit_cost)

		if normalized_qty <= 0:
			raise ValueError("Batch quantity must be positive")
		if normalized_unit_cost < 0:
			raise ValueError("Unit cost cannot be negative")
		if normalized_rate not in self.allowed_hsn_slabs:
			raise ValueError(f"Unsupported HSN 2026 GST slab: {normalized_rate:.4f}")

		total_value = self._money_4(normalized_qty * normalized_unit_cost)
		now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"

		with closing(self.get_conn()) as conn:
			row = conn.execute(
				"""
				SELECT id
				FROM inventory_batches
				WHERE sku_code = ? AND batch_code = ?
				""",
				(sku_code.strip(), batch_code.strip()),
			).fetchone()

			if row is None:
				cursor = conn.execute(
					"""
					INSERT INTO inventory_batches(
						sku_code,
						sku_name,
						batch_code,
						hsn_code,
						gst_rate,
						quantity,
						unit_cost,
						total_value,
						expiry_date,
						status,
						created_at,
						updated_at,
						created_by
					)
					VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?, ?, ?)
					""",
					(
						sku_code.strip(),
						sku_name.strip(),
						batch_code.strip(),
						normalized_hsn,
						f"{normalized_rate:.4f}",
						f"{normalized_qty:.4f}",
						f"{normalized_unit_cost:.4f}",
						f"{total_value:.4f}",
						expiry_date.isoformat() if expiry_date else None,
						now_iso,
						now_iso,
						created_by,
					),
				)
				batch_id = int(cursor.lastrowid)
				action = "CREATED"
			else:
				batch_id = int(row["id"])
				conn.execute(
					"""
					UPDATE inventory_batches
					SET sku_name = ?,
						hsn_code = ?,
						gst_rate = ?,
						quantity = ?,
						unit_cost = ?,
						total_value = ?,
						expiry_date = ?,
						status = CASE
							WHEN expiry_date IS NOT NULL AND expiry_date < date('now') THEN 'EXPIRED'
							ELSE status
						END,
						updated_at = ?
					WHERE id = ?
					""",
					(
						sku_name.strip(),
						normalized_hsn,
						f"{normalized_rate:.4f}",
						f"{normalized_qty:.4f}",
						f"{normalized_unit_cost:.4f}",
						f"{total_value:.4f}",
						expiry_date.isoformat() if expiry_date else None,
						now_iso,
						batch_id,
					),
				)
				action = "UPDATED"

			conn.commit()

		return {
			"status": "ok",
			"action": action,
			"batch_id": batch_id,
			"sku_code": sku_code.strip(),
			"batch_code": batch_code.strip(),
			"hsn_code": normalized_hsn,
			"gst_rate": f"{normalized_rate:.4f}",
			"quantity": f"{normalized_qty:.4f}",
			"unit_cost": f"{normalized_unit_cost:.4f}",
			"total_value": f"{total_value:.4f}",
			"expiry_date": expiry_date.isoformat() if expiry_date else None,
		}

	def list_batches(self, *, include_expired: bool = True, limit: int = 200) -> list[dict[str, Any]]:
		where_clause = "" if include_expired else "WHERE status != 'EXPIRED'"
		safe_limit = max(1, min(2000, int(limit)))

		with closing(self.get_conn()) as conn:
			rows = conn.execute(
				f"""
				SELECT id, sku_code, sku_name, batch_code, hsn_code, gst_rate,
					   quantity, unit_cost, total_value, expiry_date, status,
					   created_at, updated_at, created_by
				FROM inventory_batches
				{where_clause}
				ORDER BY updated_at DESC, id DESC
				LIMIT ?
				""",
				(safe_limit,),
			).fetchall()

		return [
			{
				"id": int(row["id"]),
				"sku_code": str(row["sku_code"]),
				"sku_name": str(row["sku_name"]),
				"batch_code": str(row["batch_code"]),
				"hsn_code": str(row["hsn_code"]),
				"gst_rate": str(row["gst_rate"]),
				"quantity": str(row["quantity"]),
				"unit_cost": str(row["unit_cost"]),
				"total_value": str(row["total_value"]),
				"expiry_date": row["expiry_date"],
				"status": str(row["status"]),
				"created_at": str(row["created_at"]),
				"updated_at": str(row["updated_at"]),
				"created_by": int(row["created_by"]),
			}
			for row in rows
		]
