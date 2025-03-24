package net.snowflake.client.jdbc.cloud.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import net.snowflake.client.providers.SnowflakeArgumentsProvider;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class StageInfoGcsCustomEndpointTest {

  private static class DataProvider extends SnowflakeArgumentsProvider {
    @Override
    protected List<Arguments> rawArguments(ExtensionContext context) {
      return Arrays.asList(
          Arguments.of("US-CENTRAL1", false, null, Optional.empty()),
          Arguments.of("US-CENTRAL1", false, "", Optional.empty()),
          Arguments.of("US-CENTRAL1", false, "null", Optional.empty()),
          Arguments.of("US-CENTRAL1", false, "    ", Optional.empty()),
          Arguments.of("US-CENTRAL1", false, "example.com", Optional.of("example.com")),
          Arguments.of(
              "ME-CENTRAL2", false, null, Optional.of("storage.me-central2.rep.googleapis.com")),
          Arguments.of(
              "ME-CENTRAL2", true, null, Optional.of("storage.me-central2.rep.googleapis.com")),
          Arguments.of(
              "ME-CENTRAL2", true, "", Optional.of("storage.me-central2.rep.googleapis.com")),
          Arguments.of(
              "ME-CENTRAL2", true, "  ", Optional.of("storage.me-central2.rep.googleapis.com")),
          Arguments.of("ME-CENTRAL2", true, "example.com", Optional.of("example.com")),
          Arguments.of(
              "US-CENTRAL1", true, null, Optional.of("storage.us-central1.rep.googleapis.com")),
          Arguments.of(
              "US-CENTRAL1", true, "", Optional.of("storage.us-central1.rep.googleapis.com")),
          Arguments.of(
              "US-CENTRAL1", true, " ", Optional.of("storage.us-central1.rep.googleapis.com")),
          Arguments.of(
              "US-CENTRAL1", true, "null", Optional.of("storage.us-central1.rep.googleapis.com")),
          Arguments.of("US-CENTRAL1", true, "example.com", Optional.of("example.com")));
    }
  }

  @ParameterizedTest
  @ArgumentsSource(DataProvider.class)
  public void shouldReturnEmptyGCSRegionalUrlWhenNotMeCentral1AndNotUseRegionalUrl(
      String region, boolean useRegionalUrl, String endPoint, Optional<String> expectedHost) {
    StageInfo stageInfo =
        StageInfo.createStageInfo("GCS", "bla", new HashMap<>(), region, endPoint, "account", true);
    stageInfo.setUseRegionalUrl(useRegionalUrl);
    assertEquals(expectedHost, stageInfo.gcsCustomEndpoint());
  }
}
