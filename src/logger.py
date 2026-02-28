"""
SHIELD AI — Structured JSON Logger
=====================================

Configures the Python logging stack with a JSON formatter so every log record
is emitted as a single-line JSON object.  A ``PIPELINE_RUN_ID`` (UUID4) is
generated once at import time and injected into every record via a
``logging.Filter``, providing a correlation key across microservice boundaries
and log aggregators.

Usage
-----
    # In main entry point (call once before pw.run()):
    from src.logger import configure_logging
    configure_logging()

    # In every module (never configure, only obtain):
    import logging
    log = logging.getLogger(__name__)

    log.info("pipeline started", extra={"cetp_dir": cetp_dir})
    log.warning("unknown_sensitivity", extra={"discharge_point_id": pid})
    log.debug("z_score computed", extra={"sensor_id": sid, "z_score": z})

JSON record schema
------------------
Every record includes:
    timestamp     ISO 8601 string (UTC)                e.g. "2026-02-28T06:14:29.012Z"
    level         Logging level name                   e.g. "INFO"
    module        __name__ of the calling module       e.g. "src.zscore"
    message       The formatted log message
    run_id        Pipeline-run UUID (constant per run)
    + any extra   kwargs passed via extra={}

Design constraints
------------------
- Zero external dependencies: stdlib logging, json, uuid, datetime only.
- JSON output is single-line (json.separators=(',', ':')).
- Formatter is applied to the root logger handler.
- No logging inside tight computation loops — only state-transition events.
"""

from __future__ import annotations

import datetime
import json
import logging
import uuid


# ---------------------------------------------------------------------------
# Module-level PIPELINE_RUN_ID — generated ONCE at import time.
# ---------------------------------------------------------------------------

PIPELINE_RUN_ID: str = str(uuid.uuid4())

# Reserved LogRecord attributes that are NOT forwarded as extra fields.
# This avoids "double-serialising" built-in fields into a noisy 'extra' blob.
_STDLIB_LOG_ATTRS: frozenset[str] = frozenset({
    "args", "created", "exc_info", "exc_text", "filename", "funcName",
    "id", "levelname", "levelno", "lineno", "message", "module", "msecs",
    "msg", "name", "pathname", "process", "processName", "relativeCreated",
    "run_id", "stack_info", "taskName", "thread", "threadName",
})


# ---------------------------------------------------------------------------
# JSON formatter
# ---------------------------------------------------------------------------

class _JsonFormatter(logging.Formatter):
    """Format each LogRecord as a single-line JSON object.

    The record is serialised as:
        {
          "timestamp": "<ISO 8601 UTC>",
          "level":     "<LEVEL>",
          "module":    "<name>",
          "message":   "<message>",
          "run_id":    "<uuid>",
          ... extra kwargs ...
        }

    Extra fields are any attributes added via extra={} that are not part of
    the standard LogRecord namespace.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Serialise a LogRecord to a single-line JSON string."""
        # Resolve message (applies % formatting if args present)
        record.message = record.getMessage()

        # ISO 8601 UTC timestamp
        ts = datetime.datetime.fromtimestamp(
            record.created, tz=datetime.timezone.utc
        ).strftime("%Y-%m-%dT%H:%M:%S.") + f"{record.msecs:03.0f}Z"

        payload: dict = {
            "timestamp": ts,
            "level":     record.levelname,
            "module":    record.name,
            "message":   record.message,
            "run_id":    getattr(record, "run_id", PIPELINE_RUN_ID),
        }

        # Append any user-supplied extra fields, skipping stdlib attrs
        for key, val in record.__dict__.items():
            if key not in _STDLIB_LOG_ATTRS and not key.startswith("_"):
                payload[key] = val

        # Inline exception traceback if present (single line via repr)
        if record.exc_info and record.exc_info[0] is not None:
            payload["exc"] = self.formatException(record.exc_info).replace("\n", " | ")

        return json.dumps(payload, default=str, separators=(",", ":"))


# ---------------------------------------------------------------------------
# Run-ID filter — injects PIPELINE_RUN_ID into every record
# ---------------------------------------------------------------------------

class _RunIdFilter(logging.Filter):
    """Inject the PIPELINE_RUN_ID into every LogRecord that passes through."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Always allow the record; attach run_id as a side effect."""
        record.run_id = PIPELINE_RUN_ID
        return True


# ---------------------------------------------------------------------------
# Public configuration function
# ---------------------------------------------------------------------------

def configure_logging(level: str = "INFO") -> None:
    """Configure the root logger with the JSON formatter and RunId filter.

    Must be called once — in the pipeline entry point — before any log records
    are emitted.  Subsequent calls are idempotent (existing handlers are
    replaced to avoid duplicate output).

    Args:
        level: Logging level string (DEBUG, INFO, WARNING, ERROR, CRITICAL).
               Defaults to "INFO".  Typically passed as config.CONFIG.log_level.
    """
    root = logging.getLogger()

    # Remove existing handlers to prevent duplicate / plain-text output
    root.handlers.clear()

    handler = logging.StreamHandler()
    handler.setFormatter(_JsonFormatter())
    handler.addFilter(_RunIdFilter())

    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Immediately confirm logging is live (useful for startup verification)
    logging.getLogger(__name__).info(
        "JSON logging configured",
        extra={"log_level": level, "run_id": PIPELINE_RUN_ID},
    )
