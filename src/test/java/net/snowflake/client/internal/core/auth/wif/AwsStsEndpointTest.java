package net.snowflake.client.internal.core.auth.wif;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import net.snowflake.client.internal.core.SFException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class AwsStsEndpointTest {

  @ParameterizedTest
  @CsvSource({
    "us-east-1, sts.us-east-1.amazonaws.com, https://sts.us-east-1.amazonaws.com",
    "cn-north-1, sts.cn-north-1.amazonaws.com.cn, https://sts.cn-north-1.amazonaws.com.cn",
    "us-gov-west-1, sts.us-gov-west-1.amazonaws.com, https://sts.us-gov-west-1.amazonaws.com",
    "us-iso-east-1, sts.us-iso-east-1.c2s.ic.gov, https://sts.us-iso-east-1.c2s.ic.gov",
    "us-isob-east-1, sts.us-isob-east-1.sc2s.sgov.gov, https://sts.us-isob-east-1.sc2s.sgov.gov",
    "eu-isoe-west-1, sts.eu-isoe-west-1.cloud.adc-e.uk, https://sts.eu-isoe-west-1.cloud.adc-e.uk",
    "us-isof-south-1, sts.us-isof-south-1.csp.hci.ic.gov, https://sts.us-isof-south-1.csp.hci.ic.gov",
    "eusc-de-east-1, sts.eusc-de-east-1.amazonaws.eu, https://sts.eusc-de-east-1.amazonaws.eu"
  })
  public void shouldResolveRegionalDefault(
      String region, String expectedAuthority, String expectedBaseUrl) throws SFException {
    AwsStsEndpoint endpoint = AwsStsEndpoint.resolve(null, region);
    assertEquals(expectedAuthority, endpoint.getAuthority());
    assertEquals(expectedBaseUrl, endpoint.getBaseUrl());
    assertFalse(endpoint.isOverridden());
  }

  @ParameterizedTest
  @CsvSource({
    "sts.custom.example.com, sts.custom.example.com, https://sts.custom.example.com",
    "sts.custom.example.com:8443, sts.custom.example.com:8443, https://sts.custom.example.com:8443",
    "https://sts.custom.example.com, sts.custom.example.com, https://sts.custom.example.com",
    "https://sts.custom.example.com///, sts.custom.example.com, https://sts.custom.example.com",
    "https://sts.custom.example.com/, sts.custom.example.com, https://sts.custom.example.com",
    "http://sts.custom.example.com, sts.custom.example.com, http://sts.custom.example.com",
    "'  sts.custom.example.com  ', sts.custom.example.com, https://sts.custom.example.com"
  })
  public void shouldParseWorkloadIdentityHost(
      String host, String expectedAuthority, String expectedBaseUrl) throws SFException {
    AwsStsEndpoint endpoint = AwsStsEndpoint.resolve(host, "us-custom-1");
    assertEquals(expectedAuthority, endpoint.getAuthority());
    assertEquals(expectedBaseUrl, endpoint.getBaseUrl());
    assertTrue(endpoint.isOverridden());
  }

  @Test
  public void shouldRejectInvalidScheme() {
    SFException thrown =
        assertThrows(
            SFException.class,
            () -> AwsStsEndpoint.parseWorkloadIdentityHost("ftp://sts.custom.example.com"));
    assertTrue(thrown.getMessage().contains("must use https or http"));
  }

  @Test
  public void shouldRejectQuery() {
    SFException thrown =
        assertThrows(
            SFException.class,
            () ->
                AwsStsEndpoint.parseWorkloadIdentityHost(
                    "https://sts.custom.example.com?Action=Foo"));
    assertTrue(thrown.getMessage().contains("must not contain user info, a query or a fragment"));
  }

  @Test
  public void shouldRejectMissingHostname() {
    SFException thrown =
        assertThrows(
            SFException.class, () -> AwsStsEndpoint.parseWorkloadIdentityHost("https:///sts"));
    assertTrue(thrown.getMessage().contains("does not contain a hostname"));
  }

  @Test
  public void shouldRejectFragment() {
    SFException thrown =
        assertThrows(
            SFException.class,
            () -> AwsStsEndpoint.parseWorkloadIdentityHost("https://sts.custom.example.com#frag"));
    assertTrue(thrown.getMessage().contains("must not contain user info, a query or a fragment"));
  }

  @Test
  public void shouldRejectUserInfo() {
    SFException thrown =
        assertThrows(
            SFException.class,
            () ->
                AwsStsEndpoint.parseWorkloadIdentityHost(
                    "https://user:pass@sts.custom.example.com")); // pragma: allowlist secret
    assertTrue(thrown.getMessage().contains("must not contain user info, a query or a fragment"));
  }

  @Test
  public void shouldRejectEmptyHost() {
    SFException thrown =
        assertThrows(SFException.class, () -> AwsStsEndpoint.parseWorkloadIdentityHost("   "));
    assertTrue(thrown.getMessage().contains("workloadIdentityHost is empty"));
  }

  @Test
  public void shouldRejectNonRootPath() {
    SFException thrown =
        assertThrows(
            SFException.class,
            () -> AwsStsEndpoint.parseWorkloadIdentityHost("https://sts.custom.example.com/foo"));
    assertTrue(thrown.getMessage().contains("must not contain a path"));
  }
}
