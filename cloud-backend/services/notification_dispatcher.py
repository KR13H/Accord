from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class NotificationService:
    def __init__(self, *, template_dir: Path | None = None) -> None:
        self.template_dir = template_dir or (Path(__file__).resolve().parents[1] / "templates")

    def _template_for_lang(self, lang: str) -> Path:
        code = (lang or "en").strip().lower()
        mapping = {
            "en": self.template_dir / "rent_reminder_en.json",
            "hi": self.template_dir / "rent_reminder_hi.json",
            "pa": self.template_dir / "rent_reminder_pa.json",
            "ur": self.template_dir / "rent_reminder_ur.json",
        }
        return mapping.get(code, mapping["en"])

    def dispatch_reminder(self, tenant_id: str, amount: str, due_date: str, lang: str) -> dict[str, Any]:
        template_path = self._template_for_lang(lang)
        if not template_path.exists():
            raise FileNotFoundError(f"Template missing: {template_path}")

        template = json.loads(template_path.read_text(encoding="utf-8"))
        message = str(template.get("message", "")).format(amount_due=amount, due_date=due_date, tenant_id=tenant_id)

        payload = {
            "channel": "WHATSAPP",
            "tenant_id": tenant_id,
            "language": lang,
            "template": template.get("template_id", template_path.stem),
            "message": message,
        }
        print(json.dumps(payload, ensure_ascii=False))
        return payload
