package net.snowflake.client.providers;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;

public class ProvidersUtil {
  private ProvidersUtil() {}

  private static List<Arguments> cartesianProduct(
      ExtensionContext context, List<Arguments> a, SnowflakeArgumentsProvider b) {
    List<Arguments> argsB = b.rawArguments(context);
    List<Arguments> result = new ArrayList<>();
    for (Arguments args : a) {
      for (Arguments args2 : argsB) {
        result.add(Arguments.of(ArrayUtils.addAll(args.get(), args2.get())));
      }
    }
    return result;
  }

  public static List<Arguments> cartesianProduct(
      ExtensionContext context,
      SnowflakeArgumentsProvider provider,
      SnowflakeArgumentsProvider... providers) {
    List<Arguments> args = provider.rawArguments(context);
    for (SnowflakeArgumentsProvider argProvider : providers) {
      args = cartesianProduct(context, args, argProvider);
    }
    return args;
  }
}
