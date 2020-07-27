/*
 * Copyright (c) 2018-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

import java.io.File;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.category.TestCategoryCore;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@Ignore("powermock incompat with JDK>=9, see https://github.com/powermock/powermock/issues/901")
@RunWith(PowerMockRunner.class)
@Category(TestCategoryCore.class)
public class SFTrustManagerPowerMockIT {
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  @PrepareForTest({
    net.snowflake.client.core.SFTrustManager.class,
    net.snowflake.client.core.FileCacheManager.class,
    TrustManagerFactory.class
  })

  /*
   * Test SF_OCSP_RESPONSE_CACHE_DIR environment variable changes the
   * location of the OCSP cache directory.
   */
  public void testUnitOCSPWithCustomCacheDirectory() throws Throwable {
    mockStatic(System.class);
    mockStatic(TrustManagerFactory.class);

    // mock System.getenv
    File cacheFolder = tmpFolder.newFolder();
    when(System.getenv("SF_OCSP_RESPONSE_CACHE_DIR")).thenReturn(cacheFolder.getCanonicalPath());

    // mock trust manager and return empty list
    TrustManagerFactory tested = mock(TrustManagerFactory.class);

    when(TrustManagerFactory.getInstance("SunX509")).thenReturn(tested);
    when(tested.getTrustManagers()).thenReturn(new TrustManager[] {});

    new SFTrustManager(OCSPMode.FAIL_CLOSED, null); // cache file location

    // The goal is check if the cache file location is changed to the specified
    // directory, so it doesn't need to do OCSP check in this test.
    assertThat(
        "The cache file doesn't exist.",
        new File(cacheFolder, SFTrustManager.CACHE_FILE_NAME).exists());
  }
}
