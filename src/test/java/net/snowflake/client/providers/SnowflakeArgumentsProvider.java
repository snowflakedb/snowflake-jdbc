package net.snowflake.client.providers;

import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

public abstract class SnowflakeArgumentsProvider implements ArgumentsProvider {
  protected abstract List<Arguments> rawArguments(ExtensionContext context);

  @Override
  public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
    return rawArguments(context).stream();
  }
}
