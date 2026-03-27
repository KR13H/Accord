from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path
import sys

import httpx

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import main


def test_sme_concurrency_500_posts_500_gets() -> None:
    main.init_db()

    async def run_load() -> None:
        transport = httpx.ASGITransport(app=main.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver", timeout=30.0) as client:
            stamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
            post_tasks = [
                client.post(
                    "/api/v1/sme/transactions",
                    json={
                        "business_id": "SME-001",
                        "type": "INCOME",
                        "amount": 10,
                        "category": f"Load-{stamp}",
                        "payment_method": "Cash",
                    },
                )
                for _ in range(500)
            ]
            summary_tasks = [client.get("/api/v1/sme/summary") for _ in range(500)]

            responses = await asyncio.gather(*post_tasks, *summary_tasks)

        post_responses = responses[:500]
        get_responses = responses[500:]

        post_failures = [resp.status_code for resp in post_responses if resp.status_code != 201]
        get_failures = [resp.status_code for resp in get_responses if resp.status_code != 200]

        assert not post_failures, f"POST failures: {post_failures[:10]}"
        assert not get_failures, f"GET failures: {get_failures[:10]}"

    asyncio.run(run_load())
