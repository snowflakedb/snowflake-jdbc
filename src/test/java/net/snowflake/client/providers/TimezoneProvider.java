package net.snowflake.client.providers;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;

public class TimezoneProvider extends SnowflakeArgumentsProvider {
  private int length;

  private static List<Arguments> timeZones =
      Arrays.asList(
          Arguments.of("UTC"),
          Arguments.of("America/Los_Angeles"),
          Arguments.of("America/New_York"),
          Arguments.of("Pacific/Honolulu"),
          Arguments.of("Asia/Singapore"),
          Arguments.of("CET"),
          Arguments.of("GMT+0200"));

  public TimezoneProvider(int length) {
    this.length = length;
  }

  public TimezoneProvider() {
    this.length = timeZones.size();
  }

  @Override
  protected List<Arguments> rawArguments(ExtensionContext context) {
    return timeZones.subList(0, length);
  }
}
