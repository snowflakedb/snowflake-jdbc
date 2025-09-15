package net.snowflake.client.core;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.AnyOf.anyOf;

import java.io.File;
import java.io.IOException;
import java.security.Security;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.Properties;
import java.util.stream.Stream;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.crl.CRLCacheConfig;
import net.snowflake.client.core.crl.CertRevocationCheckMode;
import net.snowflake.client.jdbc.BaseJDBCTest;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag(TestTags.CORE)
public class SFCrlTrustManagerLatestIT extends BaseJDBCTest {
  private static final SFLogger logger = SFLoggerFactory.getLogger(SFTrustManagerIT.class);
  @TempDir static File tmpFolder;

  private static class HostProvider implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
      return Stream.of(
          Arguments.of("storage.googleapis.com"),
          Arguments.of("ocspssd.us-east-1.snowflakecomputing.com/ocsp/fetch"),
          Arguments.of("sfcsupport.snowflakecomputing.com"),
          Arguments.of("sfcsupport.us-east-1.snowflakecomputing.com"),
          Arguments.of("sfcsupport.eu-central-1.snowflakecomputing.com"),
          Arguments.of("sfc-dev1-regression.s3.amazonaws.com"),
          Arguments.of("sfc-ds2-customer-stage.s3.amazonaws.com"),
          Arguments.of("snowflake.okta.com"),
          Arguments.of("sfcdev2.blob.core.windows.net"));
    }
  }

  @ParameterizedTest
  @ArgumentsSource(HostProvider.class)
  public void testCrl(String host) throws Throwable {
    System.setProperty(CRLCacheConfig.CRL_RESPONSE_CACHE_DIR, tmpFolder.getAbsolutePath());
    HttpClientSettingsKey httpClientSettings =
        new HttpClientSettingsKey(OCSPMode.DISABLE_OCSP_CHECKS);
    httpClientSettings.setRevocationCheckMode(CertRevocationCheckMode.ENABLED);
    HttpClient client = HttpUtil.buildHttpClient(httpClientSettings, null, false);
    accessHost(host, client);
  }

  private static void accessHost(String host, HttpClient client) throws IOException {
    HttpGet httpRequest = new HttpGet(String.format("https://%s:443/", host));
    HttpResponse response = client.execute(httpRequest);

    await()
        .atMost(Duration.ofSeconds(10))
        .until(() -> response.getStatusLine().getStatusCode(), not(equalTo(-1)));

    assertThat(
        String.format("response code for %s", host),
        response.getStatusLine().getStatusCode(),
        anyOf(equalTo(200), equalTo(400), equalTo(403), equalTo(404), equalTo(513)));
  }

  @Test
  void shouldNotFailWhenSimpleTrustManagerIsUsed() throws Exception {
    Security.insertProviderAt(new TestSecurityProvider(), 1);
    HttpUtil.reset();
    Properties props = new Properties();
    props.setProperty("insecureMode", "true");
    props.setProperty("disableOCSPChecks", "true");
    props.setProperty("CERT_REVOCATION_CHECK_MODE", "ENABLED");
    try (Connection connection = getConnection(props)) {
      Statement statement = connection.createStatement();
      statement.execute("SELECT 1");
    } finally {
      Security.removeProvider(TestSecurityProvider.class.getSimpleName());
      HttpUtil.reset();
    }
  }
}
