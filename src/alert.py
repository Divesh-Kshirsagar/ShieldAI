"""
SHIELD AI — Phase 1: Evidence Log & Alert Dispatch (The Evidence Log)
=====================================================================

Consumes the shock_events table from tripwire.py, runs temporal backtrack
attribution via backtrack.attribute_event(), then:
    1. Appends every attribution record to an un-falsifiable JSONL log.
    2. Optionally fires a webhook POST (configurable via SHIELD_WEBHOOK_URL env var).

Phase 3 extensions (stub hooks included):
    3. Generates a PDF summary report via fpdf2.
    4. Dispatches an email alert via smtplib.

The JSONL log is the audit trail. Once written, records are never modified.
Each line is a complete, self-contained JSON object with all evidence fields.

Usage
-----
    from src.alert import attach_alert_sink
    factory_index = build_factory_index()
    attach_alert_sink(shock_events, factory_index)  # registers pw.io.subscribe
"""

import json
import os
import smtplib
from datetime import datetime, timezone
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

import httpx
import pathway as pw

from src.backtrack import attribute_event, build_factory_index
from src.config import CONFIG as _cfg

_ALERT_LOG_PATH:     str = _cfg.alert_log_path
_SHIELD_WEBHOOK_URL: str = _cfg.shield_webhook_url


# ---------------------------------------------------------------------------
# JSONL evidence sink
# ---------------------------------------------------------------------------

def _make_evidence_callback(
    factory_index,
) -> Any:
    """Return a pw.io.subscribe callback that runs backtrack attribution on each alert.

    Args:
        factory_index: Pre-loaded pandas DataFrame from backtrack.build_factory_index().

    Returns:
        Callable matching the pw.io.subscribe signature.
    """

    def _callback(key: pw.Pointer, row: dict, time: int, is_addition: bool) -> None:
        """Write one evidence record to the JSONL log.

        Args:
            key:         Pathway row key (unused).
            row:         Dict of column name → value for this shock event row.
            time:        Pathway internal event timestamp (ms), unused here.
            is_addition: True when a new row is added; False on retraction (skipped).
        """
        # NOTE: Retractions can occur when upstream data is corrected.
        # We only log additions to keep the audit trail append-only.
        if not is_addition:
            return

        cetp_time = row.get("time", "")
        cetp_cod  = row.get("cod_value") or row.get("cetp_inlet_cod")
        breach    = row.get("breach_mag")
        level     = row.get("alert_level", "MEDIUM")

        # Run temporal backtrack attribution (pandas lookup)
        attribution = attribute_event(cetp_time, factory_index)

        record = {
            "logged_at":          datetime.now(tz=timezone.utc).isoformat(),
            "cetp_event_time":    cetp_time,
            "cetp_cod":           cetp_cod,
            "breach_mag":         breach,
            "alert_level":        level,
            "backtrack_time":     attribution["backtrack_time"],
            "attributed_factory": attribution["attributed_factory"],
            "factory_cod":        attribution["factory_cod"],
            "factory_bod":        attribution["factory_bod"],
            "factory_tss":        attribution["factory_tss"],
        }

        Path(_ALERT_LOG_PATH).parent.mkdir(parents=True, exist_ok=True)
        with open(_ALERT_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

        print(
            f"[ALERT] {record['cetp_event_time']} | "
            f"Factory: {record['attributed_factory']} | "
            f"COD: {record['cetp_cod']} mg/L | "
            f"Level: {record['alert_level']}"
        )

        if _SHIELD_WEBHOOK_URL:
            _fire_webhook(record)

    return _callback


# ---------------------------------------------------------------------------
# Pathway sink registration
# ---------------------------------------------------------------------------

def attach_alert_sink(
    shock_events: pw.Table,
    factory_index=None,
) -> None:
    """Register the JSONL writer as a Pathway subscribe sink on shock_events.

    Builds the factory index if not supplied, then attaches the callback.

    Args:
        shock_events:   Output of tripwire.detect_anomalies().
        factory_index:  Pre-loaded factory DataFrame. Built automatically if None.
    """
    if factory_index is None:
        factory_index = build_factory_index()

    pw.io.subscribe(shock_events, _make_evidence_callback(factory_index))


# ---------------------------------------------------------------------------
# Webhook dispatch
# ---------------------------------------------------------------------------

def _fire_webhook(record: dict) -> None:
    """POST the evidence record to the configured webhook URL (best-effort)."""
    try:
        response = httpx.post(_SHIELD_WEBHOOK_URL, json=record, timeout=5.0)
        response.raise_for_status()
        print(f"[WEBHOOK] Delivered — HTTP {response.status_code}")
    except Exception as exc:  # noqa: BLE001
        print(f"[WEBHOOK] Delivery failed: {exc}")


# ---------------------------------------------------------------------------
# Email alert (Phase 3 stub)
# ---------------------------------------------------------------------------

def send_email_alert(record: dict) -> None:
    """Send an HTML email alert for a single evidence record (Phase 3 stub).

    Configure via env vars: SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, ALERT_EMAIL_TO.
    """
    smtp_host = os.getenv("SMTP_HOST", "")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER", "")
    smtp_pass = os.getenv("SMTP_PASS", "")
    to_addr   = os.getenv("ALERT_EMAIL_TO", "")

    if not all([smtp_host, smtp_user, smtp_pass, to_addr]):
        return  # SMTP not configured

    body = f"""
    <h2>⚠️ SHIELD AI — Shock Load Alert</h2>
    <table>
      <tr><td><b>CETP Event Time</b></td><td>{record['cetp_event_time']}</td></tr>
      <tr><td><b>CETP COD</b></td><td>{record['cetp_cod']} mg/L</td></tr>
      <tr><td><b>Alert Level</b></td><td>{record['alert_level']}</td></tr>
      <tr><td><b>Attributed Factory</b></td><td><strong>{record['attributed_factory']}</strong></td></tr>
      <tr><td><b>Factory COD @ T-15min</b></td><td>{record['factory_cod']} mg/L</td></tr>
    </table>
    <p>Evidence logged: {_ALERT_LOG_PATH}</p>
    """

    msg = MIMEText(body, "html")
    msg["Subject"] = f"[SHIELD AI] {record['alert_level']} Alert — {record['attributed_factory']}"
    msg["From"]    = smtp_user
    msg["To"]      = to_addr

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
    except Exception as exc:  # noqa: BLE001
        print(f"[EMAIL] Send failed: {exc}")


# ---------------------------------------------------------------------------
# PDF report (Phase 3 stub)
# ---------------------------------------------------------------------------

def generate_pdf_report(records: list[dict], out_path: str) -> str:
    """Generate a PDF summary of all evidence records (Phase 3 stub).

    Args:
        records:  List of evidence dicts (read from evidence_log.jsonl).
        out_path: File path for the output PDF.

    Returns:
        Absolute path to the generated PDF.
    """
    from fpdf import FPDF  # deferred — fpdf2 optional in Phase 1

    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 16)
    pdf.cell(0, 10, "SHIELD AI — Evidence Report", ln=True)
    pdf.set_font("Helvetica", size=10)
    pdf.cell(0, 6, f"Generated: {datetime.now(tz=timezone.utc).isoformat()}", ln=True)
    pdf.ln(4)

    for i, rec in enumerate(records, 1):
        pdf.set_font("Helvetica", "B", 11)
        pdf.cell(0, 7, f"Event {i}: {rec.get('cetp_event_time', 'N/A')}", ln=True)
        pdf.set_font("Helvetica", size=10)
        for field, label in [
            ("attributed_factory", "Attributed Factory"),
            ("cetp_cod",           "CETP COD (mg/L)"),
            ("breach_mag",         "Breach Magnitude"),
            ("alert_level",        "Alert Level"),
            ("factory_cod",        "Factory COD @ T-15min"),
        ]:
            pdf.cell(0, 6, f"  {label}: {rec.get(field, 'N/A')}", ln=True)
        pdf.ln(2)

    pdf.output(out_path)
    return os.path.abspath(out_path)
