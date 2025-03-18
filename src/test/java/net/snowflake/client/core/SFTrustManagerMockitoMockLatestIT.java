package net.snowflake.client.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import net.snowflake.client.TestUtil;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.jdbc.SnowflakeUtil;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

@Tag(TestTags.CORE)
public class SFTrustManagerMockitoMockLatestIT {

  @TempDir private File tmpFolder;

  /*
   * Test SF_OCSP_RESPONSE_CACHE_DIR environment variable changes the
   * location of the OCSP cache directory.
   */
  @Test
  @Disabled("static initialization block of SFTrustManager class doesn't run sometimes")
  public void testUnitOCSPWithCustomCacheDirectory() throws IOException {
    try (MockedStatic<TrustManagerFactory> mockedTrustManagerFactory =
            mockStatic(TrustManagerFactory.class);
        MockedStatic<SnowflakeUtil> mockedSnowflakeUtil = mockStatic(SnowflakeUtil.class)) {

      File cacheFolder = new File(tmpFolder, "cache");
      cacheFolder.mkdirs();
      mockedSnowflakeUtil
          .when(() -> TestUtil.systemGetEnv("SF_OCSP_RESPONSE_CACHE_DIR"))
          .thenReturn(cacheFolder.getCanonicalPath());

      TrustManagerFactory tested = mock(TrustManagerFactory.class);
      when(tested.getTrustManagers()).thenReturn(new TrustManager[] {});

      mockedTrustManagerFactory
          .when(() -> TrustManagerFactory.getInstance("SunX509"))
          .thenReturn(tested);

      new SFTrustManager(
          new HttpClientSettingsKey(OCSPMode.FAIL_CLOSED), null); // cache file location

      // The goal is to check if the cache file location is changed to the specified
      // directory, so it doesn't need to do OCSP check in this test.
      assertThat(
          "The cache file doesn't exist.",
          new File(cacheFolder, SFTrustManager.CACHE_FILE_NAME).exists());
    }
  }
}
