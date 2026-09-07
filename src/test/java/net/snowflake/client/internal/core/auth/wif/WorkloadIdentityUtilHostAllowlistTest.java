package net.snowflake.client.internal.core.auth.wif;

import static net.snowflake.client.internal.core.auth.wif.WorkloadIdentityUtil.isSnowflakeHostForWorkloadIdentity;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import net.snowflake.client.internal.core.SFException;
import net.snowflake.client.internal.core.SFLoginInput;
import net.snowflake.client.internal.core.SessionUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for the WORKLOAD_IDENTITY host allowlist guard (finding #5): WORKLOAD_IDENTITY fetches an
 * ambient cloud credential and must only ever hand it to a recognized Snowflake host, regardless of
 * what the connection configuration (account/host/URL) specifies.
 */
class WorkloadIdentityUtilHostAllowlistTest {

  @ParameterizedTest
  @ValueSource(
      strings = {
        "myorg-acct.snowflakecomputing.com",
        "myorg-acct.privatelink.snowflakecomputing.com",
        "acct.us-east-1.snowflakecomputing.com",
        "acct.snowflakecomputing.cn",
        "acct.snowflakecomputing.mil",
        "acct.some-region.privatelink.snowflakecomputing.mil",
        "snowflakecomputing.com",
        "ACCT.SnowflakeComputing.COM",
        "acct.snowflakecomputing.com.",
        // Vector 23: FQDN form with a trailing dot AND an explicit port - the port must be
        // stripped first, otherwise the trailing dot survives and the host is rejected.
        "acct.snowflakecomputing.com.:443"
      })
  void shouldAcceptSnowflakeHosts(String host) {
    assertTrue(isSnowflakeHostForWorkloadIdentity(host), "expected host to be accepted: " + host);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "evilsnowflakecomputing.com",
        "acct.snowflakecomputing.com.other-vendor.example",
        "evil.snowflakecomputing.other-vendor.example",
        "acct.snowflakecomputing.zip",
        "other-vendor.example",
        "snowflakecomputing.com.evil.io",
        "127.0.0.1",
        "xsnowflakecomputing.mil",
        "acct.snowflakecomputing.co"
      })
  void shouldRejectNonSnowflakeHosts(String host) {
    assertFalse(isSnowflakeHostForWorkloadIdentity(host), "expected host to be rejected: " + host);
  }

  @Test
  void shouldRejectEmptyHost() {
    assertFalse(isSnowflakeHostForWorkloadIdentity(""));
  }

  @Test
  void shouldRejectWiremockHostWithEnvVarUnset() {
    assertFalse(WorkloadIdentityUtil.isSnowflakeHostForWorkloadIdentity("wiremock.local", null));
  }

  @Test
  void shouldAcceptWiremockHostWithMatchingEnvVarSet() {
    assertTrue(
        WorkloadIdentityUtil.isSnowflakeHostForWorkloadIdentity(
            "wiremock.local", "wiremock.local"));
  }

  @Test
  void shouldStillRejectUnrelatedHostWithUnrelatedEnvVarSet() {
    assertFalse(
        WorkloadIdentityUtil.isSnowflakeHostForWorkloadIdentity(
            "other-vendor.example", "wiremock.local"));
  }

  @Test
  void shouldAcceptAdditionalSuffixWithMultipleCommaSeparatedEntries() {
    assertTrue(
        WorkloadIdentityUtil.isSnowflakeHostForWorkloadIdentity(
            "host.wiremock.local", " Other-Vendor.Example , wiremock.local. "));
    assertTrue(
        WorkloadIdentityUtil.isSnowflakeHostForWorkloadIdentity(
            "sub.other-vendor.example", " Other-Vendor.Example , wiremock.local. "));
  }

  /**
   * Proves the ambient cloud credential is never fetched/minted for a rejected host: the private
   * SessionUtil#getWorkloadIdentityAttestation(SFLoginInput) method - the only call site that
   * constructs the WIF attestation providers and asks them for a credential - must fail closed with
   * SFException before any provider/creator ever touches the network, for a host outside the
   * allowlist.
   */
  @Test
  void shouldNotCreateAttestationForRejectedHost() throws Exception {
    SFLoginInput loginInput = new SFLoginInput();
    loginInput.setServerUrl("https://other-vendor.example/session/v1/login-request");

    Method getWorkloadIdentityAttestation =
        SessionUtil.class.getDeclaredMethod("getWorkloadIdentityAttestation", SFLoginInput.class);
    getWorkloadIdentityAttestation.setAccessible(true);

    InvocationTargetException invocationTargetException =
        Assertions.assertThrows(
            InvocationTargetException.class,
            () -> getWorkloadIdentityAttestation.invoke(null, loginInput));
    Assertions.assertInstanceOf(SFException.class, invocationTargetException.getCause());
    String message = invocationTargetException.getCause().getMessage();
    Assertions.assertTrue(
        message.contains("other-vendor.example"),
        "expected error message to name the rejected host, got: " + message);
  }
}
