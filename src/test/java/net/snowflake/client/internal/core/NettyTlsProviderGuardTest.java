package net.snowflake.client.internal.core;

import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;

/**
 * Guards the mechanism by which {@code jdk.tls.client.protocols} reaches PUT/GET stage transfers.
 *
 * <p>The cloud storage SDKs expose no TLS version API (notably {@code
 * NettyNioAsyncHttpClient.Builder}), so the system property is the only way to constrain stage
 * transfers. It works because Netty falls back to {@code SslProvider.JDK}, which delegates to JSSE
 * and therefore honours the property. If {@code netty-tcnative} ever lands on the classpath, Netty
 * switches to the native OpenSSL provider, which does not read JDK system properties -- TLS
 * enforcement on stage transfers would silently stop working with no other test failing.
 *
 * <p>{@code netty-tcnative-boringssl-static} is currently excluded from every dependency that would
 * otherwise pull it in. This test fails if that changes, so the regression is caught here rather
 * than in a compliance audit.
 */
public class NettyTlsProviderGuardTest {

  private static final String TCNATIVE_CLASS = "io.netty.internal.tcnative.SSL";

  @Test
  public void shouldNotBundleNettyTcnative() {
    try {
      Class.forName(TCNATIVE_CLASS, false, getClass().getClassLoader());
      fail(
          "netty-tcnative is on the classpath. Netty will prefer the native OpenSSL provider, which"
              + " ignores jdk.tls.client.protocols, so TLS version enforcement on PUT/GET stage"
              + " transfers would silently stop working. Either restore the dependency exclusions"
              + " or configure the TLS protocols on the storage SDK clients explicitly.");
    } catch (ClassNotFoundException expected) {
      // no native provider: Netty uses SslProvider.JDK and honours jdk.tls.client.protocols
    }
  }
}
