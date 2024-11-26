/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc.cloud.storage;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class StageInfoGcsCustomEndpointTest {
  private final String region;
  private final boolean useRegionalUrl;
  private final String endPoint;
  private final Optional<String> expectedHost;

  public StageInfoGcsCustomEndpointTest(
      String region, boolean useRegionalUrl, String endPoint, Optional<String> expectedHost) {
    this.region = region;
    this.useRegionalUrl = useRegionalUrl;
    this.endPoint = endPoint;
    this.expectedHost = expectedHost;
  }

  @Test
  public void shouldReturnEmptyGCSRegionalUrlWhenNotMeCentral1AndNotUseRegionalUrl() {
    StageInfo stageInfo =
        StageInfo.createStageInfo("GCS", "bla", new HashMap<>(), region, endPoint, "account", true);
    stageInfo.setUseRegionalUrl(useRegionalUrl);
    assertEquals(expectedHost, stageInfo.gcsCustomEndpoint());
  }

  @Parameterized.Parameters()
  public static Object[][] data() {
    return new Object[][] {
      {"US-CENTRAL1", false, null, Optional.empty()},
      {"US-CENTRAL1", false, "", Optional.empty()},
      {"US-CENTRAL1", false, "null", Optional.empty()},
      {"US-CENTRAL1", false, "    ", Optional.empty()},
      {"US-CENTRAL1", false, "example.com", Optional.of("example.com")},
      {"ME-CENTRAL2", false, null, Optional.of("storage.me-central2.rep.googleapis.com")},
      {"ME-CENTRAL2", true, null, Optional.of("storage.me-central2.rep.googleapis.com")},
      {"ME-CENTRAL2", true, "", Optional.of("storage.me-central2.rep.googleapis.com")},
      {"ME-CENTRAL2", true, "  ", Optional.of("storage.me-central2.rep.googleapis.com")},
      {"ME-CENTRAL2", true, "example.com", Optional.of("example.com")},
      {"US-CENTRAL1", true, null, Optional.of("storage.us-central1.rep.googleapis.com")},
      {"US-CENTRAL1", true, "", Optional.of("storage.us-central1.rep.googleapis.com")},
      {"US-CENTRAL1", true, " ", Optional.of("storage.us-central1.rep.googleapis.com")},
      {"US-CENTRAL1", true, "null", Optional.of("storage.us-central1.rep.googleapis.com")},
      {"US-CENTRAL1", true, "example.com", Optional.of("example.com")},
    };
  }
}
