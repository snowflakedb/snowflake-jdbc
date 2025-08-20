package net.snowflake.client.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.NoSuchAlgorithmException;
import javax.net.ssl.SSLContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class SFSSLConnectionSocketFactoryTest {
  @BeforeEach
  public void setUp() {
    SFSSLConnectionSocketFactory.setMinTlsVersion("TLSv1.2");
    SFSSLConnectionSocketFactory.setMaxTlsVersion("TLSv1.3");
  }

  @AfterEach
  public void tearDown() {
    SFSSLConnectionSocketFactory.setMinTlsVersion("TLSv1.2");
    SFSSLConnectionSocketFactory.setMaxTlsVersion("TLSv1.3");
  }

  @Test
  public void testDefaultTlsVersions() throws Exception {
    String[] supportedVersions = getSupportedTlsVersions();

    if (isTls13Available()) {
      assertEquals(2, supportedVersions.length);
      assertEquals("TLSv1.2", supportedVersions[0]);
      assertEquals("TLSv1.3", supportedVersions[1]);
    } else {
      assertEquals(1, supportedVersions.length);
      assertEquals("TLSv1.2", supportedVersions[0]);
    }
  }

  @ParameterizedTest
  @CsvSource({
    "TLSv1.2,TLSv1.3,TLSv1.2 TLSv1.3",
    "TLSv1.2,TLSv1.2,TLSv1.2",
    "TLSv1.3,TLSv1.3,TLSv1.3"
  })
  public void testTlsConstraints(String min, String max, String expected) throws Exception {
    if (isTls13Available()) {
      SFSSLConnectionSocketFactory.setMinTlsVersion(min);
      SFSSLConnectionSocketFactory.setMaxTlsVersion(max);

      String versions = String.join(" ", getSupportedTlsVersions());
      assertEquals(expected, versions);
    }
  }

  @Test
  public void testMinGreaterThanMax() {
    SFSSLConnectionSocketFactory.setMinTlsVersion("TLSv1.3");
    SFSSLConnectionSocketFactory.setMaxTlsVersion("TLSv1.2");

    InvocationTargetException thrown =
        assertThrows(InvocationTargetException.class, this::getSupportedTlsVersions);
    assertInstanceOf(IllegalArgumentException.class, thrown.getCause());
  }

  private String[] getSupportedTlsVersions() throws Exception {
    Method method = SFSSLConnectionSocketFactory.class.getDeclaredMethod("getSupportedTlsVersions");
    method.setAccessible(true);
    return (String[]) method.invoke(null);
  }

  private boolean isTls13Available() {
    try {
      SSLContext.getInstance("TLSv1.3");
      return true;
    } catch (NoSuchAlgorithmException e) {
      return false;
    }
  }
}
