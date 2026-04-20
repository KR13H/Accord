from __future__ import annotations

import os

import httpx


OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434").rstrip("/")
OLLAMA_GENERATE_URL = f"{OLLAMA_HOST}/api/generate"
TRANSLATION_MODEL = os.getenv("ACCORD_TRANSLATION_MODEL", "llama3")


async def translate_item_name(text: str, target_language: str = "hi") -> str:
    clean_text = (text or "").strip()
    if not clean_text:
        return ""

    if target_language.strip().lower() != "hi":
        return clean_text

    system_prompt = (
        "You are an English to Hindi retail translator. Translate the given hardware/kirana "
        "store item name to Hindi script (Devanagari). Return ONLY the translated string, "
        "no quotes or explanations."
    )

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.post(
                OLLAMA_GENERATE_URL,
                json={
                    "model": TRANSLATION_MODEL,
                    "stream": False,
                    "prompt": f"{system_prompt}\n\nItem: {clean_text}",
                    "options": {"temperature": 0.1},
                },
            )
        response.raise_for_status()
        payload = response.json()
        translated = str(payload.get("response", "")).strip().strip('"').strip("'")
        return translated or clean_text
    except Exception:  # noqa: BLE001
        return clean_text
