package net.snowflake.client.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class PrivateLinkDetectorTest {
  static class DataProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
      return Stream.of(
          Arguments.of("snowhouse.snowflakecomputing.com", false),
          Arguments.of("snowhouse.privatelink.snowflakecomputing.com", true),
          Arguments.of("snowhouse.PRIVATELINK.snowflakecomputing.com", true),
          Arguments.of("snowhouse.snowflakecomputing.cn", false),
          Arguments.of("snowhouse.privatelink.snowflakecomputing.cn", true),
          Arguments.of("snowhouse.PRIVATELINK.snowflakecomputing.cn", true),
          Arguments.of("snowhouse.snowflakecomputing.xyz", false),
          Arguments.of("snowhouse.privatelink.snowflakecomputing.xyz", true),
          Arguments.of("snowhouse.PRIVATELINK.snowflakecomputing.xyz", true));
    }
  }

  @ParameterizedTest
  @ArgumentsSource(DataProvider.class)
  public void shouldDetectPrivateLinkHost(String host, boolean expectedToBePrivateLink) {
    assertEquals(
        expectedToBePrivateLink,
        PrivateLinkDetector.isPrivateLink(host),
        String.format("Expecting %s to be private link: %s", host, expectedToBePrivateLink));
  }
}
