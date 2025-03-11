package net.snowflake.client.core;

import java.security.cert.CertificateException;
import net.minidev.json.JSONObject;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;

public class OCSPTelemetryData {
  private String certId;
  private String sfcPeerHost;
  private String ocspUrl;
  private String ocspReq;
  private Boolean cacheEnabled;
  private Boolean cacheHit;
  private OCSPMode ocspMode;

  public OCSPTelemetryData() {
    this.ocspMode = OCSPMode.FAIL_OPEN;
    this.cacheEnabled = true;
  }

  public void setCertId(String certId) {
    this.certId = certId;
  }

  public void setSfcPeerHost(String sfcPeerHost) {
    this.sfcPeerHost = sfcPeerHost;
  }

  public void setOcspUrl(String ocspUrl) {
    this.ocspUrl = ocspUrl;
  }

  public void setOcspReq(String ocspReq) {
    this.ocspReq = ocspReq;
  }

  public void setCacheEnabled(Boolean cacheEnabled) {
    this.cacheEnabled = cacheEnabled;
    if (!cacheEnabled) {
      this.cacheHit = false;
    }
  }

  public void setCacheHit(Boolean cacheHit) {
    if (!this.cacheEnabled) {
      this.cacheHit = false;
    } else {
      this.cacheHit = cacheHit;
    }
  }

  public void setOCSPMode(OCSPMode ocspMode) {
    this.ocspMode = ocspMode;
  }

  public String generateTelemetry(String eventType, CertificateException ex) {
    JSONObject value = new JSONObject();
    String valueStr;
    value.put("eventType", eventType);
    value.put("sfcPeerHost", this.sfcPeerHost);
    value.put("certId", this.certId);
    value.put("ocspResponderURL", this.ocspUrl);
    value.put("ocspReqBase64", this.ocspReq);
    value.put("ocspMode", this.ocspMode.name());
    value.put("cacheEnabled", this.cacheEnabled);
    value.put("cacheHit", this.cacheHit);
    valueStr = value.toString(); // Avoid adding exception stacktrace to user logs.
    TelemetryService.getInstance().logOCSPExceptionTelemetryEvent(eventType, value, ex);
    return valueStr;
  }
}
