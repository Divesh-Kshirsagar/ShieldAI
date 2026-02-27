"""
SHIELD AI — Phase 1: Evidence Log & Alert Dispatch (The Evidence Log)
=====================================================================

Consumes the evidence_table from backtrack.py and:
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
    attach_alert_sink(evidence_table)   # registers Pathway sink; call pw.run() after
"""

import json
import os
import smtplib
from datetime import datetime, timezone
from email.mime.text import MIMEText
from pathlib import Path

import httpx
import pathway as pw

from src.constants import ALERT_LOG_PATH, SHIELD_WEBHOOK_URL


# ---------------------------------------------------------------------------
# JSONL evidence sink
# ---------------------------------------------------------------------------

def _write_evidence_row(key: pw.Pointer, row: dict, time: int, is_addition: bool) -> None:
    """Pathway subscribe callback — writes one evidence record to the JSONL log.

    Args:
        key:        Pathway row key (unused but required by subscribe signature).
        row:        Dict of column name → value for this evidence row.
        time:       Pathway internal event timestamp (ms, unused here).
        is_addition: True when a new row is added; False on retraction (we skip).
    """
    # NOTE: Retractions can occur in Pathway when upstream data is corrected.
    # We only log additions to keep the audit trail append-only and un-falsifiable.
    if not is_addition:
        return

    Path(ALERT_LOG_PATH).parent.mkdir(parents=True, exist_ok=True)

    record = {
        "logged_at":          datetime.now(tz=timezone.utc).isoformat(),
        "cetp_event_time":    row.get("cetp_event_time"),
        "cetp_cod":           row.get("cetp_cod"),
        "breach_mag":         row.get("breach_mag"),
        "alert_level":        row.get("alert_level"),
        "backtrack_time":     row.get("backtrack_time"),
        "attributed_factory": row.get("attributed_factory"),
        "factory_cod":        row.get("factory_cod"),
        "factory_bod":        row.get("factory_bod"),
        "factory_tss":        row.get("factory_tss"),
    }

    with open(ALERT_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")

    print(
        f"[ALERT] {record['cetp_event_time']} | "
        f"Factory: {record['attributed_factory']} | "
        f"COD: {record['cetp_cod']} mg/L | "
        f"Level: {record['alert_level']}"
    )

    # Fire webhook if configured
    if SHIELD_WEBHOOK_URL:
        _fire_webhook(record)


# ---------------------------------------------------------------------------
# Pathway sink registration
# ---------------------------------------------------------------------------

def attach_alert_sink(evidence_table: pw.Table) -> None:
    """Register the JSONL writer as a Pathway subscribe sink on evidence_table.

    Call this before pw.run() so Pathway knows to invoke _write_evidence_row
    for every new row produced by backtrack.attribute_factory().

    Args:
        evidence_table: Output of backtrack.attribute_factory().
    """
    pw.io.subscribe(evidence_table, _write_evidence_row)


# ---------------------------------------------------------------------------
# Webhook dispatch (Phase 1 stub)
# ---------------------------------------------------------------------------

def _fire_webhook(record: dict) -> None:
    """POST the evidence record to the configured webhook URL.

    NOTE: This is a best-effort fire-and-forget call. Failures are logged
    to stdout but do not raise — the JSONL sink is the primary audit trail.

    Args:
        record: Evidence dict to POST as JSON body.
    """
    try:
        response = httpx.post(
            SHIELD_WEBHOOK_URL,
            json=record,
            timeout=5.0,
        )
        response.raise_for_status()
        print(f"[WEBHOOK] Delivered to {SHIELD_WEBHOOK_URL} — HTTP {response.status_code}")
    except Exception as exc:  # noqa: BLE001
        print(f"[WEBHOOK] Delivery failed: {exc}")


# ---------------------------------------------------------------------------
# Email alert (Phase 3 stub — wired but not called in Phase 1)
# ---------------------------------------------------------------------------

def send_email_alert(record: dict) -> None:
    """Send an HTML email alert for a single evidence record.

    NOTE: This is a Phase 3 feature stub. Configure via env vars:
        SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, ALERT_EMAIL_TO

    Args:
        record: Evidence dict (same shape written to JSONL).
    """
    smtp_host = os.getenv("SMTP_HOST", "")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER", "")
    smtp_pass = os.getenv("SMTP_PASS", "")
    to_addr   = os.getenv("ALERT_EMAIL_TO", "")

    if not all([smtp_host, smtp_user, smtp_pass, to_addr]):
        # NOTE: Email sending is disabled until SMTP env vars are configured.
        return

    body = f"""
    <h2>⚠️ SHIELD AI — Shock Load Alert</h2>
    <table>
      <tr><td><b>CETP Event Time</b></td><td>{record['cetp_event_time']}</td></tr>
      <tr><td><b>CETP COD</b></td><td>{record['cetp_cod']} mg/L</td></tr>
      <tr><td><b>Breach Magnitude</b></td><td>{record['breach_mag']:.2f} mg/L above baseline</td></tr>
      <tr><td><b>Alert Level</b></td><td>{record['alert_level']}</td></tr>
      <tr><td><b>Attributed Factory</b></td><td><strong>{record['attributed_factory']}</strong></td></tr>
      <tr><td><b>Factory COD at T-15min</b></td><td>{record['factory_cod']} mg/L</td></tr>
    </table>
    <p>Evidence logged to: {ALERT_LOG_PATH}</p>
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
        print(f"[EMAIL] Alert sent to {to_addr}")
    except Exception as exc:  # noqa: BLE001
        print(f"[EMAIL] Send failed: {exc}")


# ---------------------------------------------------------------------------
# PDF report (Phase 3 stub)
# ---------------------------------------------------------------------------

def generate_pdf_report(records: list[dict], out_path: str) -> str:
    """Generate a PDF summary of all evidence records.

    NOTE: Phase 3 feature stub. Requires fpdf2 (already in pyproject.toml).
    Called by the Streamlit dashboard's "Download Report" button.

    Args:
        records:  List of evidence dicts (read from evidence_log.jsonl).
        out_path: File path for the output PDF.

    Returns:
        Absolute path to the generated PDF.
    """
    from fpdf import FPDF  # deferred import — fpdf2 is optional in Phase 1

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
