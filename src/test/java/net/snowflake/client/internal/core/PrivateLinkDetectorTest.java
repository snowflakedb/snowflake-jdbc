package net.snowflake.client.internal.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class PrivateLinkDetectorTest {
  static class DataProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
      return Stream.of(
          Arguments.of("snowhouse.snowflakecomputing.com", false),
          Arguments.of("snowhouse.privatelink.snowflakecomputing.com", true),
          Arguments.of("snowhouse.PRIVATELINK.snowflakecomputing.com", true),
          Arguments.of("snowhouse.snowflakecomputing.cn", false),
          Arguments.of("snowhouse.privatelink.snowflakecomputing.cn", true),
          Arguments.of("snowhouse.PRIVATELINK.snowflakecomputing.cn", true),
          Arguments.of("snowhouse.snowflakecomputing.xyz", false),
          Arguments.of("snowhouse.privatelink.snowflakecomputing.xyz", true),
          Arguments.of("snowhouse.PRIVATELINK.snowflakecomputing.xyz", true));
    }
  }

  @ParameterizedTest
  @ArgumentsSource(DataProvider.class)
  public void shouldDetectPrivateLinkHost(String host, boolean expectedToBePrivateLink) {
    assertEquals(
        expectedToBePrivateLink,
        PrivateLinkDetector.isPrivateLink(host),
        String.format("Expecting %s to be private link: %s", host, expectedToBePrivateLink));
  }

  @Test
  public void shouldRejectPrivateLinkHostWithTrailingAttackerDomain() {
    assertFalse(
        PrivateLinkDetector.isPrivateLink("evil.privatelink.snowflakecomputing.attacker.com"),
        "Hostname with extra labels after the TLD must not be treated as PrivateLink");
  }

  @Test
  public void shouldRejectPrivateLinkSubstringInNonSnowflakeDomain() {
    assertFalse(
        PrivateLinkDetector.isPrivateLink("x.privatelink.snowflakecomputing.com.attacker.com"),
        "Snowflake domain embedded as a subdomain of attacker must not match");
  }

  @Test
  public void shouldHandleNullHost() {
    assertFalse(PrivateLinkDetector.isPrivateLink(null));
  }

  @Test
  public void shouldDetectPrivateLinkFromFullUrl() {
    assertTrue(
        PrivateLinkDetector.isPrivateLink("https://test.privatelink.snowflakecomputing.com"));
    assertTrue(
        PrivateLinkDetector.isPrivateLink("https://test.privatelink.snowflakecomputing.com:443/"));
    assertFalse(PrivateLinkDetector.isPrivateLink("https://test.snowflakecomputing.com"));
    assertFalse(
        PrivateLinkDetector.isPrivateLink(
            "https://evil.privatelink.snowflakecomputing.attacker.com"));
  }

  static class IsSnowflakeHostDataProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
      return Stream.of(
          Arguments.of("account.us-east-1.snowflakecomputing.com", true),
          Arguments.of("account.snowflakecomputing.com", true),
          Arguments.of("account.snowflakecomputing.cn", true),
          Arguments.of("account.privatelink.snowflakecomputing.com", true),
          Arguments.of("account.global.snowflakecomputing.com", true),
          Arguments.of("ACCOUNT.SNOWFLAKECOMPUTING.COM", true),
          Arguments.of("evil.privatelink.snowflakecomputing.attacker.com", false),
          Arguments.of("snowflakecomputing.com.attacker.com", false),
          Arguments.of("account.snowflakecomputing.com.evil.com", false),
          Arguments.of("attacker.com", false),
          Arguments.of("snowflakecomputing.com", false),
          Arguments.of(null, false));
    }
  }

  @ParameterizedTest
  @ArgumentsSource(IsSnowflakeHostDataProvider.class)
  public void shouldValidateSnowflakeHost(String host, boolean expectedToBeSnowflake) {
    assertEquals(
        expectedToBeSnowflake,
        PrivateLinkDetector.isSnowflakeHost(host),
        String.format("Expecting %s to be Snowflake host: %s", host, expectedToBeSnowflake));
  }

  @Test
  public void shouldRejectHostWithOnlySnowflakeComputingDomain() {
    assertFalse(
        PrivateLinkDetector.isSnowflakeHost("snowflakecomputing.com"),
        "Bare snowflakecomputing.com without account label must not be valid");
  }

  @Test
  public void shouldAcceptMultiLabelSnowflakeHost() {
    assertTrue(
        PrivateLinkDetector.isSnowflakeHost("abc.us-west-2.aws.snowflakecomputing.com"),
        "Multi-label subdomain must be accepted");
  }
}
