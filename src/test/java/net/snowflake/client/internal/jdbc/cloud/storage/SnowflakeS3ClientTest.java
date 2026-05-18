package net.snowflake.client.internal.jdbc.cloud.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.internal.core.SFSession;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.http.nio.netty.SdkEventLoopGroup;
import software.amazon.awssdk.services.s3.S3AsyncClient;

/**
 * Unit tests for {@link SnowflakeS3Client} endpoint selection ({@code stageInfo.endPoint} and
 * regional URL mode). Uses {@link S3AsyncClient#serviceClientConfiguration()}{@code
 * .endpointOverride()} to read the effective override.
 */
public class SnowflakeS3ClientTest {

  @Test
  public void shouldDetermineDomainForRegion() {
    assertEquals("amazonaws.com", SnowflakeS3Client.getDomainSuffixForRegionalUrl("us-east-1"));
    assertEquals(
        "amazonaws.com.cn", SnowflakeS3Client.getDomainSuffixForRegionalUrl("cn-northwest-1"));
    assertEquals(
        "amazonaws.com.cn", SnowflakeS3Client.getDomainSuffixForRegionalUrl("CN-NORTHWEST-1"));
  }

  /** Documents why bare hostnames must be normalized before {@code endpointOverride(URI)}. */
  @Test
  public void shouldDocumentBareAwsHostnameHasNullUriScheme() {
    assertNull(URI.create("s3.us-west-2.amazonaws.com").getScheme());
  }

  @Test
  public void shouldPrependHttpsWhenStageEndpointHasNoScheme() throws Exception {
    SnowflakeS3Client client =
        newClient(
            "eu-west-1", "s3.eu-west-1.amazonaws.com", false, /* isClientSideEncrypted */ false);
    try {
      URI override = endpointOverride(client).get();
      assertEquals("https", override.getScheme());
      assertEquals("s3.eu-west-1.amazonaws.com", override.getHost());
    } finally {
      client.shutdown();
    }
  }

  @Test
  public void shouldKeepHttpsStageEndpointUnchanged() throws Exception {
    String url = "https://s3-fips.us-gov-west-1.amazonaws.com";
    SnowflakeS3Client client =
        newClient("us-gov-west-1", url, false, /* isClientSideEncrypted */ false);
    try {
      URI override = endpointOverride(client).get();
      assertEquals(URI.create(url), override);
    } finally {
      client.shutdown();
    }
  }

  /**
   * RFC 3986: scheme is case-insensitive; must not prepend a second {@code https://} (e.g. to
   * {@code HTTPS://...}).
   */
  @Test
  public void shouldAcceptUppercaseHttpsSchemeInStageEndpoint() throws Exception {
    String url = "HTTPS://s3-fips.us-gov-west-1.amazonaws.com";
    SnowflakeS3Client client =
        newClient("us-gov-west-1", url, false, /* isClientSideEncrypted */ false);
    try {
      URI override = endpointOverride(client).get();
      assertTrue(override.getScheme().equalsIgnoreCase("https"));
      assertEquals("s3-fips.us-gov-west-1.amazonaws.com", override.getHost());
    } finally {
      client.shutdown();
    }
  }

  @Test
  public void shouldKeepHttpStageEndpointUnchanged() throws Exception {
    String url = "http://minio.local:9000";
    SnowflakeS3Client client =
        newClient("us-east-1", url, false, /* isClientSideEncrypted */ false);
    try {
      URI override = endpointOverride(client).get();
      assertEquals("http", override.getScheme());
      assertEquals("minio.local", override.getHost());
      assertEquals(9000, override.getPort());
    } finally {
      client.shutdown();
    }
  }

  @Test
  public void shouldSetRegionalS3EndpointForCommercialRegion() throws Exception {
    SnowflakeS3Client client =
        newClient(
            "us-west-2",
            /* stageEndPoint */ null,
            /* useS3RegionalUrl */ true,
            /* isClientSideEncrypted */ false);
    try {
      URI override = endpointOverride(client).get();
      assertEquals(URI.create("https://s3.us-west-2.amazonaws.com"), override);
    } finally {
      client.shutdown();
    }
  }

  @Test
  public void shouldSetRegionalS3EndpointForChinaRegion() throws Exception {
    SnowflakeS3Client client =
        newClient(
            "cn-northwest-1",
            /* stageEndPoint */ null,
            /* useS3RegionalUrl */ true,
            /* isClientSideEncrypted */ false);
    try {
      URI override = endpointOverride(client).get();
      assertEquals(URI.create("https://s3.cn-northwest-1.amazonaws.com.cn"), override);
    } finally {
      client.shutdown();
    }
  }

  @Test
  public void shouldOmitEndpointOverrideWhenNotConfigured() throws Exception {
    SnowflakeS3Client client =
        newClient(
            "eu-central-1",
            /* stageEndPoint */ null,
            /* useS3RegionalUrl */ false,
            /* isClientSideEncrypted */ false);
    try {
      assertFalse(endpointOverride(client).isPresent());
    } finally {
      client.shutdown();
    }
  }

  /** Regression: scheme-less {@code stageEndPoint} must not NPE inside AWS SDK v2 client build. */
  @Test
  public void shouldBuildClientWithoutNpeWhenStageEndpointIsSchemelessHost() throws Exception {
    SnowflakeS3Client client = null;
    try {
      client =
          newClient(
              "us-west-2", "s3.us-west-2.amazonaws.com", false, /* isClientSideEncrypted */ false);
      assertNotNull(client);
      assertTrue(endpointOverride(client).isPresent());
      assertNotNull(endpointOverride(client).get().getScheme());
    } finally {
      if (client != null) {
        client.shutdown();
      }
    }
  }

  @Test
  public void shouldCloseInheritedClientOnRenew() throws Exception {
    TestSession session = new TestSession();
    SnowflakeS3Client client = newClientWithSession(session);
    try {
      Field amazonClientField = SnowflakeS3Client.class.getDeclaredField("amazonClient");
      amazonClientField.setAccessible(true);
      S3AsyncClient original = (S3AsyncClient) amazonClientField.get(client);
      S3AsyncClient spied = spy(original);
      amazonClientField.set(client, spied);

      client.renew(awsCreds());

      verify(spied).close();
      assertNotSame(spied, amazonClientField.get(client), "renew should install a new S3 client");
    } finally {
      client.shutdown();
      session.releaseEventLoopGroup();
    }
  }

  /**
   * Pins the AWS SDK shape that {@link SnowflakeS3Client.S3EventLoopGroupHolder#close()} relies on
   * via reflection (the relocation asymmetry blocks a direct call). If a future SDK upgrade renames
   * {@code SdkEventLoopGroup#eventLoopGroup()} or changes the {@code shutdownGracefully(long, long,
   * TimeUnit)} signature, the production catch-all logs a warning and silently leaks Netty threads;
   * this test fails CI loudly instead.
   */
  @Test
  public void shouldResolveSdkEventLoopGroupShutdownReflectively() throws Exception {
    SdkEventLoopGroup group = SdkEventLoopGroup.builder().build();
    try {
      Method accessor = group.getClass().getMethod("eventLoopGroup");
      Object eventLoopGroup = accessor.invoke(group);
      assertNotNull(eventLoopGroup);
      Method shutdown =
          eventLoopGroup
              .getClass()
              .getMethod("shutdownGracefully", long.class, long.class, TimeUnit.class);
      assertNotNull(shutdown.invoke(eventLoopGroup, 0L, 1L, TimeUnit.SECONDS));
    } finally {
      try {
        Object eventLoopGroup = group.getClass().getMethod("eventLoopGroup").invoke(group);
        eventLoopGroup
            .getClass()
            .getMethod("shutdownGracefully", long.class, long.class, TimeUnit.class)
            .invoke(eventLoopGroup, 0L, 1L, TimeUnit.SECONDS);
      } catch (ReflectiveOperationException ignored) {
        // best-effort cleanup; the assertions above are the real signal
      }
    }
  }

  private static SnowflakeS3Client newClient(
      String stageRegion,
      String stageEndPoint,
      boolean useS3RegionalUrl,
      boolean isClientSideEncrypted)
      throws SnowflakeSQLException {
    SnowflakeS3Client.ClientConfiguration config =
        new SnowflakeS3Client.ClientConfiguration(2, 3, 5_000, 5_000);
    return new SnowflakeS3Client(
        awsCreds(),
        config,
        /* encMat */ null,
        /* proxyProperties */ null,
        stageRegion,
        stageEndPoint,
        isClientSideEncrypted,
        /* session */ null,
        useS3RegionalUrl);
  }

  /**
   * Exposes the protected close hook so the test can signal shutdown of the shared Netty group
   * (asynchronous — shutdownGracefully is fire-and-forget with a 1 s timeout).
   */
  private static class TestSession extends SFSession {
    void releaseEventLoopGroup() {
      closeS3EventLoopGroup();
    }
  }

  private static SnowflakeS3Client newClientWithSession(SFSession session)
      throws SnowflakeSQLException {
    SnowflakeS3Client.ClientConfiguration config =
        new SnowflakeS3Client.ClientConfiguration(2, 3, 5_000, 5_000);
    return new SnowflakeS3Client(
        awsCreds(),
        config,
        /* encMat */ null,
        /* proxyProperties */ null,
        "us-west-2",
        /* stageEndPoint */ null,
        /* isClientSideEncrypted */ false,
        session,
        /* useS3RegionalUrl */ false);
  }

  private static Map<String, String> awsCreds() {
    Map<String, String> creds = new HashMap<>();
    creds.put("AWS_KEY_ID", "AKIA_TEST");
    creds.put("AWS_SECRET_KEY", "testSecretAccessKey");
    return creds;
  }

  private static Optional<URI> endpointOverride(SnowflakeS3Client client) throws Exception {
    Field f = SnowflakeS3Client.class.getDeclaredField("amazonClient");
    f.setAccessible(true);
    S3AsyncClient aws = (S3AsyncClient) f.get(client);
    return aws.serviceClientConfiguration().endpointOverride();
  }
}
