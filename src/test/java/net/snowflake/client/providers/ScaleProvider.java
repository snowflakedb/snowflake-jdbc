package net.snowflake.client.providers;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;

public class ScaleProvider extends SnowflakeArgumentsProvider {
  @Override
  protected List<Arguments> rawArguments(ExtensionContext context) {
    ArrayList<Arguments> scales = new ArrayList<>();
    for (int scale = 0; scale < 10; scale++) {
      scales.add(Arguments.of(scale));
    }
    return scales;
  }
}
