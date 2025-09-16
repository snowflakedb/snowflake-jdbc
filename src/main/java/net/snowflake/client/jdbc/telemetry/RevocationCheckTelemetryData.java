package net.snowflake.client.jdbc.telemetry;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
public class RevocationCheckTelemetryData {
  private String crlUrl;
  private long timeParsingCrl;
  private long timeDownloadingCrl;
  private long crlBytes;
  private int numberOfRevokedCertificates;

  public void setTimeParsingCrl(long timeParsingCrl) {
    this.timeParsingCrl = timeParsingCrl;
  }

  public void setTimeDownloadingCrl(long timeDownloadingCrl) {
    this.timeDownloadingCrl = timeDownloadingCrl;
  }

  public void setCrlUrl(String crlUrl) {
    this.crlUrl = crlUrl;
  }

  public void setCrlBytes(long crlBytes) {
    this.crlBytes = crlBytes;
  }

  public void setNumberOfRevokedCertificates(int numberOfRevokedCertificates) {
    this.numberOfRevokedCertificates = numberOfRevokedCertificates;
  }

  public TelemetryData buildTelemetry() {
    return TelemetryUtil.buildCrlData(
        crlUrl, crlBytes, numberOfRevokedCertificates, timeDownloadingCrl, timeParsingCrl);
  }
}
