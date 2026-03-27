from __future__ import annotations

import json
import re
from pathlib import Path
from urllib import error, request

ROOT = Path(__file__).resolve().parents[1]
SERVICE_FILES = [
    ROOT / "services" / "universal_accounting.py",
    ROOT / "services" / "sme_credit_service.py",
]
OUTPUT_FILE = ROOT / "tests" / "ai_generated" / "test_universal_accounting_ai.py"
OLLAMA_TAGS_URL = "http://localhost:11434/api/tags"
OLLAMA_GENERATE_URL = "http://localhost:11434/api/generate"


def read_sources() -> str:
    chunks: list[str] = []
    for path in SERVICE_FILES:
        chunks.append(f"# FILE: {path.name}\n")
        chunks.append(path.read_text(encoding="utf-8"))
        chunks.append("\n\n")
    return "".join(chunks)


def resolve_model_name(default: str = "llama3") -> str:
    try:
        req = request.Request(OLLAMA_TAGS_URL, method="GET")
        with request.urlopen(req, timeout=10) as res:  # noqa: S310
            payload = json.loads(res.read().decode("utf-8"))
        models = payload.get("models", [])
        if models:
            first_name = str(models[0].get("name", "")).strip()
            if first_name:
                return first_name
    except Exception:
        return default
    return default


def strip_code_fences(text: str) -> str:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```[a-zA-Z0-9_\-]*\n", "", cleaned)
        cleaned = re.sub(r"\n```$", "", cleaned)
    return cleaned.strip()


def extract_python_code(text: str) -> str:
    cleaned = strip_code_fences(text)

    fenced_match = re.search(r"```(?:python)?\n([\s\S]*?)\n```", text, flags=re.IGNORECASE)
    if fenced_match:
        candidate = fenced_match.group(1).strip()
        if candidate:
            return candidate

    lines = cleaned.splitlines()
    code_start = 0
    start_patterns = ("import ", "from ", "def ", "class ", "@pytest", "pytest")
    for idx, line in enumerate(lines):
        if line.strip().startswith(start_patterns):
            code_start = idx
            break
    candidate = "\n".join(lines[code_start:]).strip()

    # Remove common trailing commentary sections.
    candidate = re.split(r"\n\s*(?:This pytest suite|Note that|Summary:)", candidate, maxsplit=1)[0].strip()
    return candidate


def request_tests(model_name: str, source_blob: str) -> str:
    prompt = (
        "You are an expert Python QA engineer. Write a complete, highly aggressive pytest suite "
        "for the following FastAPI/SQLAlchemy service code. Include tests for negative numbers, "
        "missing IDs, extreme large floats, and SQL injection attempts. Output ONLY valid Python "
        "code, no markdown explanations.\n\n"
        f"{source_blob}"
    )
    body = {
        "model": model_name,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0.2},
    }
    req = request.Request(
        OLLAMA_GENERATE_URL,
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=120) as res:  # noqa: S310
            payload = json.loads(res.read().decode("utf-8"))
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"Ollama request failed: {exc.code} {detail}") from exc

    response = str(payload.get("response", "")).strip()
    if not response:
        raise RuntimeError("Ollama returned an empty response")
    extracted = extract_python_code(response)
    if not extracted:
        raise RuntimeError("Ollama response did not contain valid Python code")
    return extracted


def normalize_generated_tests(code: str) -> str:
    normalized = code
    normalized = re.sub(
        r"from\s+sme_credit_service\s+import",
        "from services.sme_credit_service import",
        normalized,
    )
    normalized = re.sub(
        r"from\s+universal_accounting\s+import",
        "from services.universal_accounting import",
        normalized,
    )

    header = (
        "from __future__ import annotations\n"
        "\n"
        "import sqlite3\n"
        "from datetime import date\n"
        "from decimal import Decimal\n"
        "from pathlib import Path\n"
        "import sys\n"
        "\n"
        "ROOT = Path(__file__).resolve().parents[2]\n"
        "if str(ROOT) not in sys.path:\n"
        "    sys.path.insert(0, str(ROOT))\n"
        "\n"
    )

    if "ROOT = Path(__file__).resolve().parents[2]" not in normalized:
        normalized = header + normalized.lstrip()
    return normalized


def main() -> None:
    model_name = resolve_model_name()
    source_blob = read_sources()
    generated = request_tests(model_name, source_blob)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(normalize_generated_tests(generated) + "\n", encoding="utf-8")

    print(f"Generated tests with model: {model_name}")
    print(f"Saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
