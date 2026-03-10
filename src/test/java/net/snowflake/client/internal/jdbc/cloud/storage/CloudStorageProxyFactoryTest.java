package net.snowflake.client.internal.jdbc.cloud.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.internal.core.HttpClientSettingsKey;
import net.snowflake.client.internal.core.HttpProtocol;
import net.snowflake.client.internal.core.OCSPMode;
import net.snowflake.client.internal.core.SFSessionProperty;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CloudStorageProxyFactoryTest {
  static Stream<Arguments> prepareNonProxyHostsTestCases() {
    return Stream.of(
        Arguments.of("example.com", new HashSet<>(Arrays.asList("\\Qexample.com\\E"))),
        Arguments.of(
            "example.com|test.org | localhost",
            new HashSet<>(Arrays.asList("\\Qexample.com\\E", "\\Qtest.org\\E", "\\Qlocalhost\\E"))),
        Arguments.of("*.example.com", new HashSet<>(Arrays.asList("\\Q\\E.*\\Q.example.com\\E"))),
        Arguments.of(
            "example.com|*.test.org|localhost|*.internal.*",
            new HashSet<>(
                Arrays.asList(
                    "\\Qexample.com\\E",
                    "\\Q\\E.*\\Q.test.org\\E",
                    "\\Qlocalhost\\E",
                    "\\Q\\E.*\\Q.internal.\\E.*\\Q\\E"))));
  }

  @ParameterizedTest
  @MethodSource("prepareNonProxyHostsTestCases")
  void testPrepareNonProxyHosts(String input, Set<String> expected) {
    Set<String> result = CloudStorageProxyFactory.prepareNonProxyHostsForS3(input);
    assertEquals(expected, result);
  }

  // ── extractFromKey ─────────────────────────────────────────────────────────

  @Test
  void extractFromKey_returnsNullWhenKeyIsNull() {
    assertNull(CloudStorageProxyFactory.extractFromKey(null));
  }

  @Test
  void extractFromKey_returnsNullWhenProxyNotUsed() {
    HttpClientSettingsKey key = new HttpClientSettingsKey(OCSPMode.FAIL_OPEN);
    assertNull(CloudStorageProxyFactory.extractFromKey(key));
  }

  @Test
  void extractFromKey_returnsSettingsWithAllFields() {
    HttpClientSettingsKey key =
        new HttpClientSettingsKey(
            OCSPMode.FAIL_OPEN,
            "proxy.example.com",
            8080,
            "*.internal.com",
            "proxyuser",
            "proxypass",
            "https",
            null,
            false);

    ProxySettings s = CloudStorageProxyFactory.extractFromKey(key);

    assertNotNull(s);
    assertEquals("proxy.example.com", s.getHost());
    assertEquals(8080, s.getPort());
    assertEquals(HttpProtocol.HTTPS, s.getProtocol());
    assertEquals("proxyuser", s.getUser());
    assertEquals("proxypass", s.getPassword());
    assertEquals("*.internal.com", s.getNonProxyHosts());
  }

  @Test
  void extractFromKey_returnsSettingsWithHttpProtocol() {
    HttpClientSettingsKey key =
        new HttpClientSettingsKey(
            OCSPMode.FAIL_OPEN, "proxy.host", 3128, null, null, null, "http", null, false);

    ProxySettings s = CloudStorageProxyFactory.extractFromKey(key);

    assertNotNull(s);
    assertEquals(HttpProtocol.HTTP, s.getProtocol());
  }

  @Test
  void extractFromKey_hasCredentialsReturnsFalseWhenNoCredentials() {
    HttpClientSettingsKey key =
        new HttpClientSettingsKey(
            OCSPMode.FAIL_OPEN, "proxy.host", 3128, null, null, null, "http", null, false);

    ProxySettings s = CloudStorageProxyFactory.extractFromKey(key);

    assertNotNull(s);
    // empty user/password strings from the key should map to no credentials
    assertEquals(false, s.hasCredentials());
  }

  // ── extractFromProperties ──────────────────────────────────────────────────

  @Test
  void extractFromProperties_returnsNullWhenPropertiesNull() throws SnowflakeSQLException {
    assertNull(CloudStorageProxyFactory.extractFromProperties(null));
  }

  @Test
  void extractFromProperties_returnsNullWhenPropertiesEmpty() throws SnowflakeSQLException {
    assertNull(CloudStorageProxyFactory.extractFromProperties(new Properties()));
  }

  @Test
  void extractFromProperties_returnsNullWhenUseProxyAbsent() throws SnowflakeSQLException {
    Properties props = new Properties();
    props.setProperty(SFSessionProperty.PROXY_HOST.getPropertyKey(), "proxy.host");
    assertNull(CloudStorageProxyFactory.extractFromProperties(props));
  }

  @Test
  void extractFromProperties_returnsNullWhenUseProxyFalse() throws SnowflakeSQLException {
    Properties props = new Properties();
    props.setProperty(SFSessionProperty.USE_PROXY.getPropertyKey(), "false");
    props.setProperty(SFSessionProperty.PROXY_HOST.getPropertyKey(), "proxy.host");
    props.setProperty(SFSessionProperty.PROXY_PORT.getPropertyKey(), "8080");
    assertNull(CloudStorageProxyFactory.extractFromProperties(props));
  }

  @Test
  void extractFromProperties_returnsSettingsWithAllFields() throws SnowflakeSQLException {
    Properties props = new Properties();
    props.setProperty(SFSessionProperty.USE_PROXY.getPropertyKey(), "true");
    props.setProperty(SFSessionProperty.PROXY_HOST.getPropertyKey(), "proxy.example.com");
    props.setProperty(SFSessionProperty.PROXY_PORT.getPropertyKey(), "8080");
    props.setProperty(SFSessionProperty.PROXY_USER.getPropertyKey(), "proxyuser");
    props.setProperty(SFSessionProperty.PROXY_PASSWORD.getPropertyKey(), "proxypass");
    props.setProperty(SFSessionProperty.NON_PROXY_HOSTS.getPropertyKey(), "*.internal.com");
    props.setProperty(SFSessionProperty.PROXY_PROTOCOL.getPropertyKey(), "https");

    ProxySettings s = CloudStorageProxyFactory.extractFromProperties(props);

    assertNotNull(s);
    assertEquals("proxy.example.com", s.getHost());
    assertEquals(8080, s.getPort());
    assertEquals(HttpProtocol.HTTPS, s.getProtocol());
    assertEquals("proxyuser", s.getUser());
    assertEquals("proxypass", s.getPassword());
    assertEquals("*.internal.com", s.getNonProxyHosts());
  }

  @Test
  void extractFromProperties_defaultsToHttpProtocolWhenProtocolAbsent()
      throws SnowflakeSQLException {
    Properties props = new Properties();
    props.setProperty(SFSessionProperty.USE_PROXY.getPropertyKey(), "true");
    props.setProperty(SFSessionProperty.PROXY_HOST.getPropertyKey(), "proxy.host");
    props.setProperty(SFSessionProperty.PROXY_PORT.getPropertyKey(), "3128");

    ProxySettings s = CloudStorageProxyFactory.extractFromProperties(props);

    assertNotNull(s);
    assertEquals(HttpProtocol.HTTP, s.getProtocol());
  }

  @Test
  void extractFromProperties_defaultsToHttpProtocolWhenProtocolIsHttp()
      throws SnowflakeSQLException {
    Properties props = new Properties();
    props.setProperty(SFSessionProperty.USE_PROXY.getPropertyKey(), "true");
    props.setProperty(SFSessionProperty.PROXY_HOST.getPropertyKey(), "proxy.host");
    props.setProperty(SFSessionProperty.PROXY_PORT.getPropertyKey(), "3128");
    props.setProperty(SFSessionProperty.PROXY_PROTOCOL.getPropertyKey(), "http");

    ProxySettings s = CloudStorageProxyFactory.extractFromProperties(props);

    assertNotNull(s);
    assertEquals(HttpProtocol.HTTP, s.getProtocol());
  }

  @Test
  void extractFromProperties_throwsOnInvalidPort() {
    Properties props = new Properties();
    props.setProperty(SFSessionProperty.USE_PROXY.getPropertyKey(), "true");
    props.setProperty(SFSessionProperty.PROXY_HOST.getPropertyKey(), "proxy.host");
    props.setProperty(SFSessionProperty.PROXY_PORT.getPropertyKey(), "not-a-number");

    assertThrows(
        SnowflakeSQLException.class, () -> CloudStorageProxyFactory.extractFromProperties(props));
  }

  @Test
  void extractFromProperties_throwsWhenPortAbsent() {
    Properties props = new Properties();
    props.setProperty(SFSessionProperty.USE_PROXY.getPropertyKey(), "true");
    props.setProperty(SFSessionProperty.PROXY_HOST.getPropertyKey(), "proxy.host");

    assertThrows(
        SnowflakeSQLException.class, () -> CloudStorageProxyFactory.extractFromProperties(props));
  }
}
