package net.snowflake.client.internal.jdbc.cloud.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class S3HttpUtilTest {
  static Stream<Arguments> prepareNonProxyHostsTestCases() {
    return Stream.of(
        Arguments.of("example.com", new HashSet<>(Arrays.asList("example\\.com"))),
        Arguments.of(
            "example.com|test.org | localhost",
            new HashSet<>(Arrays.asList("example\\.com", "test\\.org", "localhost"))),
        Arguments.of("*.example.com", new HashSet<>(Arrays.asList(".*?\\.example\\.com"))),
        Arguments.of(
            "example.com|*.test.org|localhost|*.internal.*",
            new HashSet<>(
                Arrays.asList(
                    "example\\.com", ".*?\\.test\\.org", "localhost", ".*?\\.internal\\..*?"))));
  }

  @ParameterizedTest
  @MethodSource("prepareNonProxyHostsTestCases")
  void testPrepareNonProxyHosts(String input, Set<String> expected) {
    Set<String> result = S3HttpUtil.prepareNonProxyHosts(input);
    assertEquals(expected, result);
  }
}
