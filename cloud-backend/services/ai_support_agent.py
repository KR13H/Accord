from __future__ import annotations

import json
import re
from typing import Any

import httpx


SYSTEM_PROMPT = (
    "You are Accord's IT Support AI. Be concise. Solve basic software questions. "
    "IF the user asks for a human, reports a bug, OR if you cannot solve the issue, "
    "you MUST append this EXACT JSON string to the very end of your response: "
    '{"escalate": true, "summary": "<brief issue summary>", "priority": "<low/medium/high>"}'
)

_ESCALATION_REGEX = re.compile(r"\{[^{}]*\"escalate\"\s*:\s*true[^{}]*\}", re.IGNORECASE | re.DOTALL)
_FENCE_REGEX = re.compile(r"```(?:json)?\s*(\{[^{}]*\"escalate\"\s*:\s*true[^{}]*\})\s*```", re.IGNORECASE | re.DOTALL)


class AiSupportAgent:
    def __init__(self, *, model: str = "llama3", timeout: float = 15.0) -> None:
        self.model = model
        self.url = "http://localhost:11434/api/chat"
        self.timeout = timeout

    async def respond(self, *, message: str, history: list[dict[str, str]]) -> tuple[str, dict[str, Any] | None]:
        messages: list[dict[str, str]] = [{"role": "system", "content": SYSTEM_PROMPT}]
        messages.extend(history)
        messages.append({"role": "user", "content": message})

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                self.url,
                json={"model": self.model, "messages": messages, "stream": False},
            )
            response.raise_for_status()
            payload = response.json()

        raw = str(payload.get("message", {}).get("content") or "").strip()
        return self._extract_escalation(raw)

    def _extract_escalation(self, text: str) -> tuple[str, dict[str, Any] | None]:
        if not text:
            return ("I could not process that. Please try again.", None)

        match = _FENCE_REGEX.search(text) or _ESCALATION_REGEX.search(text)
        if match is None:
            return (text.strip(), None)

        escalation_raw = match.group(1) if match.lastindex else match.group(0)
        escalation: dict[str, Any] | None = None
        try:
            parsed = json.loads(escalation_raw)
            if isinstance(parsed, dict) and bool(parsed.get("escalate")):
                priority = str(parsed.get("priority") or "medium").strip().lower()
                if priority not in {"low", "medium", "high"}:
                    priority = "medium"
                escalation = {
                    "escalate": True,
                    "summary": str(parsed.get("summary") or "AI requested escalation").strip(),
                    "priority": priority,
                }
        except Exception:
            escalation = {
                "escalate": True,
                "summary": "AI indicated escalation but emitted malformed JSON.",
                "priority": "medium",
            }

        clean_text = (text[: match.start()] + text[match.end() :]).strip()
        clean_text = re.sub(r"```(?:json)?|```", "", clean_text).strip()
        return (clean_text or "Escalating this issue for human support.", escalation)
