package net.snowflake.client.providers;

import java.util.Arrays;
import java.util.List;
import net.snowflake.client.jdbc.ResultSetFormatType;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;

public class ResultFormatProvider extends SnowflakeArgumentsProvider {
  @Override
  protected List<Arguments> rawArguments(ExtensionContext context) {
    return Arrays.asList(
        Arguments.of(ResultSetFormatType.JSON),
        Arguments.of(ResultSetFormatType.ARROW_WITH_JSON_STRUCTURED_TYPES),
        Arguments.of(ResultSetFormatType.NATIVE_ARROW));
  }
}
