"""
SHIELD AI — Phase 2: MPCB Central Server Open API v2.3 Stub
============================================================

PURPOSE
-------
This module acts as SHIELD AI's inward-facing compliance bridge.

In the real MPCB ecosystem every analyser station must transmit data to the
Central Server over this exact REST protocol (v2.3). By mirroring the spec
precisely — same routes, same request/response JSON shapes, same auth header
fields — api.py ensures that when the prototype is ready for a live deployment,
the only change needed is swapping the stub responses for real database writes
and forwarding logic.

During the demo it also lets regulators or auditors inspect the exact data
format the system would receive from live industrial sensors.

Auth (stubbed)
--------------
Every request must include a JSON body or header with:
    site_id, software_version_id, time_stamp_data

These are validated for presence but NOT cryptographically checked in v1.
In production: decrypt the digest using the Site Private Key and verify
the timestamp is within 15 minutes of server time.

Run
---
    uvicorn src.api:app --reload
    # Swagger docs: http://localhost:8000/docs
"""

from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

app = FastAPI(
    title="SHIELD AI — MPCB Central Server API v2.3",
    description=(
        "Prototype stub of the MPCB Open API v2.3 specification. "
        "Endpoints mirror the real spec exactly for structural compliance. "
        "No live data is transmitted — this is a demo implementation."
    ),
    version="2.3.0",
)


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

class _AuthFields(BaseModel):
    """Minimum authentication fields required in every request body."""
    site_id:             str = Field(..., description="Unique site ID issued by MPCB")
    software_version_id: str = Field(..., description="Version registered on Central Server")
    time_stamp_data:     str = Field(..., description="Timestamp of encryption (ISO 8601)")


def _validate_auth(body: dict[str, Any]) -> None:
    """Check that required auth fields are present.

    NOTE: In production this would decrypt the digest with the Site Private Key
    and verify time_stamp_data is within 15 minutes of server time.
    For the prototype we only assert field presence.

    Raises:
        HTTPException 401 if any auth field is missing.
    """
    required = {"site_id", "software_version_id", "time_stamp_data"}
    missing  = required - body.keys()
    if missing:
        raise HTTPException(
            status_code=401,
            detail=f"Missing auth fields: {', '.join(sorted(missing))}",
        )


def _server_time() -> str:
    """Return current UTC time as an ISO 8601 string."""
    return datetime.now(tz=timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# 1. Data Upload — /realtimeUpload  &  /delayedUpload
# ---------------------------------------------------------------------------

@app.post(
    "/realtimeUpload",
    summary="Upload real-time sensor data (max 2 min delay)",
    tags=["Data Upload"],
)
async def realtime_upload(request: Request) -> JSONResponse:
    """Receive 1-min sensor data from the analyser station.

    Expects: multipart/form-data containing a zipped file with:
        - Data File    (encrypted, ISO-7168 / simplified delimited format)
        - Metadata File (format specification)

    In the prototype: accepts the request and returns a stubbed success response.
    The ConfigurationDownloadFlag etc. are always False in the stub.

    NOTE: In production this endpoint would:
        1. Unzip and decrypt the package using the Site Private Key.
        2. Parse the data file and write readings to the time-series database.
        3. Forward validated readings to the Pathway stream via a socket/queue.
    """
    # NOTE: Multipart parsing stubbed — in production use request.form() + zipfile.
    return JSONResponse({
        "status":                      "Success",
        "serverConfigLastUpdatedTime": _server_time(),
        "ConfigurationDownloadFlag":   "False",
        "ConfigurationUpdateFlag":     "False",
        "RemoteCalibrationUpdateFlag": "False",
        "DiagnosticUpdateFlag":        "False",
        "statusMessage":               "file uploaded successfully.",
    })


@app.post(
    "/delayedUpload",
    summary="Upload delayed sensor data (>15 min communication gap)",
    tags=["Data Upload"],
)
async def delayed_upload(request: Request) -> JSONResponse:
    """Receive delayed sensor data due to communication failure.

    Identical handling to /realtimeUpload in the prototype.
    In production: writes to a delayed-data reconciliation queue.
    """
    return JSONResponse({
        "status":        "Success",
        "statusMessage": "file uploaded successfully.",
    })


# ---------------------------------------------------------------------------
# 2. Configuration Download — /getConfig
# ---------------------------------------------------------------------------

class _GetConfigRequest(_AuthFields):
    siteId:      str = Field(..., alias="siteId")
    monitoringid: str


@app.post(
    "/getConfig",
    summary="Download station configuration to sync with analyser",
    tags=["Configuration"],
)
async def get_config(payload: _GetConfigRequest) -> JSONResponse:
    """Serve the station configuration when ConfigurationDownloadFlag is True.

    Returns a stubbed configuration matching the v2.3 spec shape.
    """
    _validate_auth(payload.dict(by_alias=True))
    return JSONResponse({
        "status":                    "Success",
        "serverConfigLastUpdatedTime": _server_time(),
        "SiteDetails": {
            "siteName":               "Demo CETP Site",
            "siteLabel":              "PRIYA_CETP",
            "siteConfigLastUpdatedTime": _server_time(),
            "siteId":                 payload.siteId,
            "customparameters":       {},
        },
        "CollectorDetails": [{
            "CollectorType":          "MODBUS",
            "CollectorName":          "COD_Analyser",
            "ConfiguredChannels":     "4",
            "PollingStep":            "10",
            "ChecksumStatusBit":      "0",
            "Address":                "192.168.1.10",
            "HeartBeat":              "60",
            "DataFormatBits":         "00",
            "Port":                   "502",
            "CommunicationTimeOut":   "30",
            "customparameters":       {},
        }],
        "AcquisitionSystemDetails": {
            "AcquisitionVersion": "1.0.0",
            "AcquisitionSystem":  "SHIELD AI Demo",
        },
    })


# ---------------------------------------------------------------------------
# 3. Upload Config — /uploadConfig
# ---------------------------------------------------------------------------

@app.post(
    "/uploadConfig",
    summary="Upload analyser configuration to server (ConfigFetch)",
    tags=["Configuration"],
)
async def upload_config(request: Request) -> JSONResponse:
    """Accept analyser config when ConfigurationUpdateFlag is True."""
    body = await request.json()
    _validate_auth(body)
    return JSONResponse({
        "status":             "Success",
        "configUpdateStatus": "Received Site configuration successfully",
    })


# ---------------------------------------------------------------------------
# 4. Config Acknowledgement — /completedConfig
# ---------------------------------------------------------------------------

class _CompletedConfigRequest(_AuthFields):
    siteId:       str
    monitoringid: str
    ConfigUpdated: str  # "True"


@app.post(
    "/completedConfig",
    summary="Acknowledge successful configuration sync",
    tags=["Configuration"],
)
async def completed_config(payload: _CompletedConfigRequest) -> JSONResponse:
    """Confirm that the client has applied the downloaded configuration."""
    return JSONResponse({
        "status":              "Success",
        "calibrationUpdateStatus": "Server and Site Configuration Synchronized",
    })


# ---------------------------------------------------------------------------
# 5. Remote Calibration — /getcalibrationconfig
# ---------------------------------------------------------------------------

class _CalibrationRequest(_AuthFields):
    siteId:          str
    monitoringid:    str
    CalibrationType: str  # "Scheduled" or "Immediate"


@app.post(
    "/getcalibrationconfig",
    summary="Download remote calibration schedule",
    tags=["Calibration"],
)
async def get_calibration_config(payload: _CalibrationRequest) -> JSONResponse:
    """Serve calibration schedule when RemoteCalibrationUpdateFlag is True."""
    _validate_auth(payload.dict())
    return JSONResponse({
        "status": "Success",
        "calibration": {
            "calibratorName":    "Demo_Calibrator",
            "siteName":          "Demo CETP Site",
            "monitoringType":    "EFFLUENT",
            "frequency":         "Weekly",
            "parameterId":       "COD",
            "parameterName":     "COD",
            "execute_Immediate": "False",
            "siteId":            payload.siteId,
            "serverCalibrationLastUpdatedTime": _server_time(),
            "sequence": [{
                "function":     "span_check",
                "duration_secs": "300",
                "gas":          "N/A",
                "value":        "0",
                "delay":        "0",
                "sequenceName": "Standard Span",
                "duration":     "5",
                "type":         "Scheduled",
                "unit":         "mg/L",
            }],
        },
    })


# ---------------------------------------------------------------------------
# 6. Calibration Acknowledgement — /updatecalibrationconfig
# ---------------------------------------------------------------------------

@app.post(
    "/updatecalibrationconfig",
    summary="Acknowledge calibration schedule sync",
    tags=["Calibration"],
)
async def update_calibration_config(payload: _CalibrationRequest) -> JSONResponse:
    """Confirm calibration schedule was downloaded and applied locally."""
    return JSONResponse({
        "status":                  "Success",
        "calibrationUpdateStatus": "Server and Site Calibration Synchronized",
    })


# ---------------------------------------------------------------------------
# 7. Diagnostic Upload — /uploadDiagnosticInfo
# ---------------------------------------------------------------------------

@app.post(
    "/uploadDiagnosticInfo",
    summary="Upload analyser diagnostic information",
    tags=["Diagnostics"],
)
async def upload_diagnostic_info(request: Request) -> JSONResponse:
    """Accept internal analyser diagnostics when DiagnosticUpdateFlag is True."""
    body = await request.json()
    _validate_auth(body)
    return JSONResponse({
        "status":                "Success",
        "diagnosticUpdateStatus": "Received Site diagnostics successfully",
    })
