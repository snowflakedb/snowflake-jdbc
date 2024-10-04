package net.snowflake.client.providers;

import java.util.stream.Stream;
import net.snowflake.client.jdbc.ResultSetFormatType;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

public class FormatProvider implements ArgumentsProvider {
  @Override
  public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
    return Stream.of(
        Arguments.of(ResultSetFormatType.JSON),
        Arguments.of(ResultSetFormatType.ARROW_WITH_JSON_STRUCTURED_TYPES),
        Arguments.of(ResultSetFormatType.NATIVE_ARROW));
  }
}
