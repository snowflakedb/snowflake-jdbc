package net.snowflake.client.providers;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;

public class SimpleResultFormatProvider extends SnowflakeArgumentsProvider {
  private static List<Arguments> arguments =
      Arrays.asList(Arguments.of("JSON"), Arguments.of("ARROW"));

  public static void setSupportedFormats(List<Arguments> supportedFormats) {
    arguments = supportedFormats;
  }

  public static void resetSupportedFormats() {
    setSupportedFormats(Arrays.asList(Arguments.of("JSON"), Arguments.of("ARROW")));
  }

  @Override
  protected List<Arguments> rawArguments(ExtensionContext context) {
    return arguments;
  }
}
