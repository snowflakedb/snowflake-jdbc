package net.snowflake.client.internal.jdbc.cloud.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
        Arguments.of("example.com", new HashSet<>(Arrays.asList("\\Qexample.com\\E"))),
        Arguments.of(
            "example.com|test.org | localhost",
            new HashSet<>(Arrays.asList("\\Qexample.com\\E", "\\Qtest.org\\E", "\\Qlocalhost\\E"))),
        Arguments.of("*.example.com", new HashSet<>(Arrays.asList("\\Q\\E.*\\Q.example.com\\E"))),
        Arguments.of(
            "example.com|*.test.org|localhost|*.internal.*",
            new HashSet<>(
                Arrays.asList(
                    "\\Qexample.com\\E",
                    "\\Q\\E.*\\Q.test.org\\E",
                    "\\Qlocalhost\\E",
                    "\\Q\\E.*\\Q.internal.\\E.*\\Q\\E"))));
  }

  @ParameterizedTest
  @MethodSource("prepareNonProxyHostsTestCases")
  void testPrepareNonProxyHosts(String input, Set<String> expected) {
    Set<String> result = S3HttpUtil.prepareNonProxyHosts(input);
    assertEquals(expected, result);
  }
}
