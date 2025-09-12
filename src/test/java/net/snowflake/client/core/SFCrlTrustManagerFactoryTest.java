package net.snowflake.client.core;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.security.cert.X509Certificate;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;
import net.snowflake.client.core.crl.CertRevocationCheckMode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

public class SFCrlTrustManagerFactoryTest {
  private HttpClientSettingsKey testKey;
  private MockedStatic<TrustManagerFactory> mockedTrustManagerFactory;

  @BeforeEach
  void setUp() {
    testKey = new HttpClientSettingsKey(OCSPMode.DISABLE_OCSP_CHECKS);
    testKey.setRevocationCheckMode(CertRevocationCheckMode.ENABLED);
  }

  @AfterEach
  void tearDown() {
    if (mockedTrustManagerFactory != null) {
      mockedTrustManagerFactory.close();
    }
  }

  @ParameterizedTest
  @ValueSource(classes = {X509TrustManager.class, X509ExtendedTrustManager.class})
  void testCreateProperCrlTrustManagerBasedOnJvmProvided(Class<? extends X509TrustManager> clazz)
      throws Exception {
    mockTrustManagerFactoryToReturn(clazz);

    X509TrustManager result = SFCrlTrustManagerFactory.createCrlTrustManager(testKey);

    assertInstanceOf(clazz, result);
  }

  private <T extends X509TrustManager> void mockTrustManagerFactoryToReturn(Class<T> mockClass) {
    mockedTrustManagerFactory = mockStatic(TrustManagerFactory.class);
    TrustManagerFactory mockFactory = mock(TrustManagerFactory.class);

    mockedTrustManagerFactory
        .when(() -> TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm()))
        .thenReturn(mockFactory);

    T trustManagerMock = mock(mockClass);
    when(trustManagerMock.getAcceptedIssuers()).thenReturn(new X509Certificate[0]);
    when(mockFactory.getTrustManagers()).thenReturn(new TrustManager[] {trustManagerMock});
  }
}
