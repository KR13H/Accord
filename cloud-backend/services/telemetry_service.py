from __future__ import annotations

import functools
import time
import uuid
from datetime import datetime
from typing import Any, Callable


class TelemetryService:
    """Provides endpoint performance instrumentation and structured error envelopes.

    The service centralizes request latency logging and standardized error metadata.
    """

    def __init__(self, api_logger: Any, latency_warn_threshold_ms: float = 3000.0) -> None:
        """Initializes telemetry service settings.

        Args:
            api_logger: Logger used for performance and error logging.
            latency_warn_threshold_ms: Threshold after which latency warnings are emitted.

        Hardware Impact:
            Low CPU overhead; suitable for always-on middleware instrumentation on Apple M3.
        Logic Invariants:
            Every tracked operation emits a duration in milliseconds.
        Legal Context:
            Supports auditability by attaching deterministic request telemetry traces.
        """
        self.api_logger = api_logger
        self.latency_warn_threshold_ms = latency_warn_threshold_ms

    def log_latency(self, *, method: str, path: str, elapsed_ms: float, started_at: str, request_id: str) -> None:
        """Logs latency information for an API call.

        Args:
            method: HTTP method.
            path: Request path.
            elapsed_ms: Execution duration in milliseconds.
            started_at: UTC ISO timestamp for request start.
            request_id: Request correlation identifier.

        Hardware Impact:
            Negligible I/O-bound logging only.
        Logic Invariants:
            Latency warning is emitted only if threshold is exceeded.
        Legal Context:
            Preserves timestamped observability records useful for compliance evidence.
        """
        if elapsed_ms > self.latency_warn_threshold_ms:
            self.api_logger.warning(
                "Saturation warning request_id=%s method=%s path=%s latency_ms=%.1f started_at=%s",
                request_id,
                method,
                path,
                elapsed_ms,
                started_at,
            )

    def performance_monitor(self, endpoint_name: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Creates a decorator that records endpoint execution time.

        Args:
            endpoint_name: Stable endpoint identifier used in logs.

        Returns:
            A decorator for async endpoint callables.

        Hardware Impact:
            Adds one monotonic timestamp pair per endpoint call.
        Logic Invariants:
            Duration is always logged even if endpoint raises an exception.
        Legal Context:
            Captures deterministic runtime traces for audit investigation timelines.
        """

        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            @functools.wraps(func)
            async def wrapper(*args: Any, **kwargs: Any) -> Any:
                started = time.perf_counter()
                try:
                    return await func(*args, **kwargs)
                finally:
                    elapsed_ms = (time.perf_counter() - started) * 1000.0
                    self.api_logger.info("PERF endpoint=%s latency_ms=%.2f", endpoint_name, elapsed_ms)

            return wrapper

        return decorator

    def build_error_envelope(
        self,
        *,
        detail: str,
        status_code: int,
        path: str,
        error_type: str,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """Builds a structured JSON error envelope.

        Args:
            detail: Human-readable error detail.
            status_code: HTTP status code.
            path: Request path.
            error_type: Stable error category.
            request_id: Optional request correlation identifier.

        Returns:
            Error envelope payload with unique AuditID.

        Hardware Impact:
            Constant-time UUID generation and dictionary allocation.
        Logic Invariants:
            Always emits an AuditID and UTC timestamp.
        Legal Context:
            Provides immutable incident identifiers for audit and dispute workflows.
        """
        return {
            "status": "error",
            "detail": detail,
            "error": {
                "audit_id": uuid.uuid4().hex[:12],
                "request_id": request_id,
                "type": error_type,
                "status_code": status_code,
                "path": path,
                "detail": detail,
                "timestamp": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            },
        }
