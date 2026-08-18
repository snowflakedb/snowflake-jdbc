package net.snowflake.client.internal.jdbc.diagnostic;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import javax.net.ssl.SNIHostName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

public class CertificateDiagnosticCheckTest {

  /**
   * Hosts that already satisfy the Letter-Digit-Hyphen rule must be probed exactly as the allowlist
   * file lists them.
   */
  @ParameterizedTest
  @ValueSource(
      strings = {
        "snowhouse.snowflakecomputing.com",
        "sfc-ds2-customer-stage.s3.us-west-2.amazonaws.com",
        "o.ss2.us",
        "100.21.137.183"
      })
  public void shouldLeaveValidHostsUnchanged(String host) {
    assertEquals(host, CertificateDiagnosticCheck.toSniCompatibleHost(host));
  }

  /**
   * Snowflake account names may contain underscores, which the JDK cannot send as a TLS server
   * name. The hyphenated variant, which Snowflake also serves, must be used instead so that the
   * handshake carries SNI.
   */
  @ParameterizedTest
  @CsvSource({
    "account_name.snowflakecomputing.com,account-name.snowflakecomputing.com",
    "org-account_name.snowflakecomputing.com,org-account-name.snowflakecomputing.com",
    "snowsql_repo.snowflakecomputing.com,snowsql-repo.snowflakecomputing.com",
    "duo_security.duosecurity.com,duo-security.duosecurity.com",
    // regionless format used when duplicate account names exist across regions
    "sfcogsops-snowhouse_aws_us_west_2.snowflakecomputing.com,"
        + "sfcogsops-snowhouse-aws-us-west-2.snowflakecomputing.com"
  })
  public void shouldHyphenateHostsWithUnderscores(String host, String expected) {
    assertEquals(expected, CertificateDiagnosticCheck.toSniCompatibleHost(host));
  }

  /** Whatever is returned for an underscored host must actually be sendable as SNI. */
  @ParameterizedTest
  @ValueSource(
      strings = {
        "account_name.snowflakecomputing.com",
        "out_of_band_telemetry.snowflakecomputing.com",
        "sfcogsops-snowhouse_aws_us_west_2.snowflakecomputing.com"
      })
  public void shouldReturnHostAcceptedBySNIHostName(String host) {
    assertThrows(IllegalArgumentException.class, () -> new SNIHostName(host));
    String sniHost = CertificateDiagnosticCheck.toSniCompatibleHost(host);
    assertDoesNotThrow(() -> new SNIHostName(sniHost));
  }

  /**
   * When hyphenation cannot produce a valid server name the original host is returned unchanged, so
   * the check still probes the endpoint the allowlist asked for rather than an invented one.
   */
  @ParameterizedTest
  @ValueSource(
      strings = {
        "bad host_name.snowflakecomputing.com", // space is not fixable by hyphenation
        "_leading.snowflakecomputing.com", // would become an illegal leading hyphen
        "a..b.snowflakecomputing.com"
      })
  public void shouldReturnOriginalHostWhenNoValidVariantExists(String host) {
    assertEquals(host, CertificateDiagnosticCheck.toSniCompatibleHost(host));
  }

  @Test
  public void shouldReturnEmptyHostUnchanged() {
    assertEquals("", CertificateDiagnosticCheck.toSniCompatibleHost(""));
  }
}
