# MPCB Central Server Software Open API v2.3 Documentation

### General API Requirements

* 
**Base URL:** `http://<ipaddress:port>/MPCBServer` 


* 
**Protocol:** HTTP-based REST Service 


* 
**Authentication:** All requests must be authenticated. Unauthenticated requests are discarded.


* 
**Data Frequencies:** Analyser sampling should occur every 10 seconds. Data transmission to the server must occur at a 1-minute frequency.


* 
**Data Components:** Transmissions must include raw data, linearized data, data quality codes, and captured timestamps.


* 
**Site Restriction:** Data must be transmitted directly from the site location; other locations will be rejected.



### Authentication Headers

Every API request header must include:

* 
**Timestamp:** Configurable, but must not be older than 15 minutes from the current timestamp.


* 
**Authorization:** An encrypted digest generated just before transmission using the Site Private Key. The digest must contain:


* 
`site_id`: Unique authentication ID provided by MPCB.


* 
`software_version_id`: Version set and registered by the Central Server (not dependent on client software version).


* 
`time_stamp_data`: Timestamp of encryption.





---

### 1. Data Upload (Real Time & Delayed)

Uploads site data to the server. If upload is successful, the client software must read subsequent instruction flags in the response.

* **Routes:**
* 
`/realtimeUpload` (For data captured during the last poll frequency / max 2 min delay) 


* 
`/delayedUpload` (For data delayed beyond 15 minutes due to communication failure) 




* 
**Method:** POST 


* 
**Request Format:** `multipart/form-data` containing a zipped file.


* **Payload Requirements:**
* The zip file must contain two files: a **Data File** and a **Metadata File**.


* The Data File must be encrypted using the Site Private Key and follow ISO-7168 format, simplified delimited, or fixed-width.


* The Metadata File specifies the file specification/format.


* The header must contain the encryption digest.





**Success Response JSON:**

```json
{
  "status": "Success",
  "serverConfigLastUpdatedTime": "<time>",
  "ConfigurationDownloadFlag": "<True/False>",
  "ConfigurationUpdateFlag": "<True/False>",
  "RemoteCalibrationUpdateFlag": "<True/False>",
  "DiagnosticUpdateFlag": "<True/False>",
  "statusMessage": "file uploaded successfully."
}

```



**Failure Response JSON:**

```json
{
  "status": "Failed",
  "statusMessage": "No files were uploaded."
}

```



---

### 2. Configuration Download

Triggered when the `ConfigurationDownloadFlag` is set to "True" in the Data Upload response. Downloads the entire station configuration to sync with the analyser.

* 
**Route:** `/getConfig` 


* 
**Method:** POST 



**Request JSON:**

```json
{
  "siteId": "<site-id>",
  "monitoringid": "<monitor-id>"
}

```



**Response JSON (Success):**

```json
{
  "status": "Success",
  "serverConfigLastUpdatedTime": "<ServerConfigUpdatedLastTime>",
  "SiteDetails": {
    "siteName": "<SiteName>",
    "siteLabel": "<SiteLabel>",
    "siteConfigLastUpdatedTime": "<SiteConfigUpdatedLastTime>",
    "siteId": "<site id>",
    "customparameters": {}
  },
  "CollectorDetails": [{
    "CollectorType": "<>",
    "CollectorName": "<>",
    "ConfiguredChannels": "<>",
    "PollingStep": "<polling step>",
    "ChecksumStatusBit": "<checksum bit>",
    "Address": "<address>",
    "HeartBeat": "<heartbeat>",
    "DataFormatBits": "00",
    "Port": "<port>",
    "CommunicationTimeOut": "<communication bit>",
    "customparameters": {}
  }],
  "configJson": {
    "monitoringType": { "required": "True", "padding": "-", "start_pos": 55, "end_pos": 64, "type": "string", "alignment": "left" },
    "monitoringId": { "required": "True", "padding": "-", "start_pos": 65, "end_pos": 84, "type": "string", "alignment": "left" },
    "QualityCode": { "required": "True", "padding": "*", "start_pos": 42, "end_pos": 43, "type": "string", "alignment": "left" },
    "SensorTime": { "required": "True", "padding": "-", "start_pos": 44, "end_pos": 54, "type": "string", "alignment": "left" },
    "parameterId": { "required": "True", "padding": "-", "start_pos": 85, "end_pos": 100, "type": "string", "alignment": "left" },
    "parameterName": { "required": "True", "padding": "", "start_pos": 11, "end_pos": 25, "type": "string", "alignment": "left" },
    "Reading": { "required": "True", "padding": "*", "start_pos": 26, "end_pos": 41, "type": "string", "alignment": "left" },
    "id": { "required": "True", "padding": "-", "start_pos": 1, "end_pos": 8, "type": "string", "alignment": "left" },
    "sensorChannel": { "required": "True", "padding": "", "start_pos": 9, "end_pos": 10, "type": "string", "alignment": "left" },
    "analyzerId": { "required": "True", "padding": "", "start_pos": 101, "end_pos": 115, "type": "string", "alignment": "left" }
  },
  "AcquisitionSystemDetails": {
    "AcquisitionVersion": "<Version Number>",
    "AcquisitionSystem": "<Acquisition System Name>"
  },
  "SensorA": {
    "collectorType": "<Monitoring Type>",
    "monitoringType": "<Monitoring Type>",
    "monitoringId": "<Monitoring Id>",
    "ChannelNo": "0",
    "GaugeMinimum": "",
    "CoefficientA": "",
    "parameterId": "<parameter id>",
    "GaugeMaximum": "",
    "MeasurementUnit": "<measurement unit>",
    "compPort": "",
    "parameterName": "<parameter name>",
    "CoefficientB": "",
    "analyzerId": "<analyzer id>",
    "customparameters": {}
  }
}

```



---

### 3. Fetch Configuration From Client

Triggered when `ConfigurationUpdateFlag` is set to "True". The client uploads its current analyser configuration to the server.

* 
**Route:** `/uploadConfig` 


* 
**Method:** POST 



**Request JSON:**
*Matches the exact schema of the `/getConfig` response, but wrapped with a Command key.*

```json
{
  "Command": "ConfigFetch",
  "serverConfigLastUpdatedTime": "<ServerConfigUpdatedLastTime>",
  "SiteDetails": {
    "siteName": "<SiteName>",
    "siteLabel": "<SiteLabel>",
    "siteConfigLastUpdatedTime": "<SiteConfigUpdatedLastTime>",
    "siteId": "<site id>",
    "monitoringId": "<monitoring id>",
    "customparameters": {}
  },
  "CollectorDetails": [ { "..." } ],
  "configJson": { "..." },
  "AcquisitionSystemDetails": { "..." },
  "SensorA": { "..." }
}

```



**Response JSON:**

```json
{
  "status": "Success",
  "configUpdateStatus": "Received Site configuration successfully"
}

```



---

### 4. Configuration Update Acknowledgement

Used by the client to confirm successful receipt and application of configurations from `/getConfig` . If not sent, the server will continuously set the download flag to "True".

* 
**Route:** `/completedConfig` 


* 
**Method:** POST 



**Request JSON:**

```json
{
  "siteId": "<site-id>",
  "monitoringid": "<monitor-id>",
  "ConfigUpdated": "True"
}

```



**Response JSON:**

```json
{
  "status": "Success",
  "calibrationUpdateStatus": "Server and Site Configuration Synchronized"
}

```



---

### 5. Remote Calibration Service

Triggered when `RemoteCalibrationUpdateFlag` is set to "True". Downloads the necessary schedule, parameters, and sequence required for local calibrators.

* 
**Route:** `/getcalibrationconfig` 


* 
**Method:** POST 



**Request JSON:**

```json
{
  "siteId": "<site-id>",
  "monitoringid": "<monitor-id>",
  "CalibrationType": "<Scheduled or Immediate>"
}

```



**Response JSON:**


*(Note: Custom parameters can be added via the "customparameters" tag if required by analyser makers)* 

```json
{
  "status": "Success",
  "calibration": {
    "calibratorName": "<calibrator-name>",
    "sequence": [{
      "function": "<function name>",
      "duration_secs": "<duration in seconds>",
      "gas": "<gas>",
      "value": "0",
      "delay": "<delay in minutes>",
      "sequenceName": "<sequence name>",
      "duration": "<duration in minute>",
      "type": "<type of calibration>",
      "unit": "<unit of gas>"
    }],
    "siteName": "<site name>",
    "monitoringType": "<monitoring type>",
    "frequency": "<frequency>",
    "analyzerId": "<analyser id>",
    "parameterId": "<parameter id>",
    "remoteCalibrationId": "<remote calibration id>",
    "parameterName": "SO2",
    "cycleUnit": "1",
    "total_duration": "<total duration>",
    "frequencyDay": "<day>",
    "siteId": "<site id>",
    "startTime": { "date": "<date>", "time": "<time>" },
    "execute_Immediate": "True",
    "day": "<day>",
    "cycle": "<cycle>",
    "frequencyTime": "<frequency time>",
    "calibratorId": "<calibration id>",
    "monitoringUnit": "<monitoring unit>",
    "value": "",
    "channelNumber": "<channel number>",
    "analyzerType": "<analyser type>",
    "endTime": { "date": "<date>", "time": "<time>" },
    "remoteCalibrationName": "<remote calibration name>",
    "analyzerName": "<analyser name>",
    "serverCalibrationLastUpdatedTime": "<serverCalibrationLastUpdatedTime>",
    "siteCalibrationLastUpdatedTime": "<siteCalibrationLastUpdatedTime>",
    "lastCalibratedOn": "<lastCalibratedon>"
  }
}

```



---

### 6. Calibration Update Acknowledgement

Confirms the calibration sequence was successfully downloaded and scheduled locally.

* 
**Route:** `/updatecalibrationconfig` 


* 
**Method:** POST 



**Request JSON:**

```json
{
  "siteId": "<site-id>",
  "monitoringid": "<monitor-id>",
  "CalibrationType": "<Scheduled or Immediate>"
}

```



**Response JSON:**

```json
{
  "status": "Success",
  "calibrationUpdateStatus": "Server and Site Calibration Synchronized"
}

```



---

### 7. Diagnostic Information Upload

Triggered when `DiagnosticUpdateFlag` is set to "True". Uploads internal state and diagnostic information of the analyser.

* 
**Route:** `/uploadDiagnosticInfo` 


* 
**Method:** POST 



**Request JSON:**

```json
{
  "Command": "DiagnosticFetch",
  "SiteDetails": {
    "siteName": "<SiteName>",
    "siteLabel": "<SiteLabel>",
    "siteConfigLastUpdatedTime": "<SiteConfigUpdatedLastTime>",
    "siteId": "<site id>",
    "monitoringId": "<monitoring id>",
    "customparameters": {}
  },
  "CollectorDetails": [{
    "CollectorType": "<>",
    "CollectorName": "<>",
    "ConfiguredChannels": "<>",
    "PollingStep": "<polling step>",
    "ChecksumStatusBit": "<checksum bit>",
    "Address": "<address>",
    "HeartBeat": "<heartbeat>",
    "DataFormatBits": "00",
    "Port": "<port>",
    "CommunicationTimeOut": "<communication bit>",
    "customparameters": {}
  }],
  "diagnosticJson": [{
    "analyserId": "<analyser-id>",
    "parameterName": "",
    "diagnostics": [{
      "key": "<key>",
      "value": "<value>",
      "category": "<category>"
    }]
  }]
}

```



**Response JSON:**

```json
{
  "status": "Success",
  "diagnosticUpdateStatus": "Received Site diagnostics successfully"
}

```



