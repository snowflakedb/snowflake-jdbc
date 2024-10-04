package net.snowflake.client.providers;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;

public class BooleanProvider extends SnowflakeArgumentsProvider {
  @Override
  protected List<Arguments> rawArguments(ExtensionContext context) {
    return Arrays.asList(Arguments.of(true), Arguments.of(false));
  }
}
