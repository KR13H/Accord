from __future__ import annotations

import json
from typing import Any

import httpx


class LocalAIParser:
    OLLAMA_URL = "http://localhost:11434/api/generate"

    def _prompt(self, raw_text: str) -> str:
        return (
            "You are a strict JSON extraction engine. Return only valid JSON with keys: "
            "vendor_name, gstin, hsn_code, base_amount, cgst, sgst, igst, total. "
            "No markdown, no prose. If unknown use null.\n"
            f"Invoice Text:\n{raw_text}"
        )

    def _parse_model_json(self, text: str) -> dict[str, Any]:
        raw = text.strip()
        start = raw.find("{")
        end = raw.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise ValueError("Model did not return a JSON object")
        payload = json.loads(raw[start : end + 1])
        required = {"vendor_name", "gstin", "hsn_code", "base_amount", "cgst", "sgst", "igst", "total"}
        missing = required.difference(payload.keys())
        if missing:
            raise ValueError(f"Missing keys in model output: {sorted(missing)}")
        return payload

    async def parse_invoice_text(self, raw_text: str) -> dict[str, Any]:
        temperatures = [0.0, 0.2, 0.4]
        errors: list[str] = []

        async with httpx.AsyncClient(timeout=45.0) as client:
            for temp in temperatures:
                response = await client.post(
                    self.OLLAMA_URL,
                    json={
                        "model": "llama3",
                        "prompt": self._prompt(raw_text),
                        "stream": False,
                        "options": {"temperature": temp},
                    },
                )
                response.raise_for_status()
                body = response.json()
                model_text = str(body.get("response") or "").strip()
                try:
                    return self._parse_model_json(model_text)
                except Exception as exc:  # noqa: BLE001
                    errors.append(str(exc))

        raise ValueError(f"Unable to parse invoice JSON from Ollama: {' | '.join(errors)}")
