package net.snowflake.client;

import com.google.common.annotations.VisibleForTesting;
import net.snowflake.client.jdbc.SnowflakeDriver;
import org.junit.Assume;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class TestSinceConditionRule implements MethodRule {
  private final Version currentVersion;

  public TestSinceConditionRule() {
    currentVersion = Version.parse(SnowflakeDriver.implementVersion);
  }

  public TestSinceConditionRule(Version currentVersion) {
    this.currentVersion = currentVersion;
  }

  @Override
  public Statement apply(Statement statement, FrameworkMethod frameworkMethod, Object o) {
    SfTest sfTest = Optional.ofNullable(frameworkMethod.getAnnotation(SfTest.class))
        .orElseGet(() -> frameworkMethod.getDeclaringClass().getAnnotation(SfTest.class));
    if (sfTest == null) {
      return statement;
    }
    Version testSinceVersion = Version.parse(sfTest.sinceVersion());
    // when we introduce a new feature we have to separate two cases
    // running during old driver test - feature is available since version X
    // running during development - feature is available since version X-1
    return decideIfShouldBeRun(statement, new IgnoreStatement(sfTest), testSinceVersion, isOldDriverTest());
  }

  @VisibleForTesting
  Statement decideIfShouldBeRun(Statement normalRun, Statement ignoreRun, Version testSinceVersion, boolean oldDriverTest) {
    if (oldDriverTest && currentVersion.compareTo(testSinceVersion) > 0) {
      return normalRun;
    }
    if (!oldDriverTest && currentVersion.compareTo(testSinceVersion) >= 0) {
      return normalRun;
    }
    return ignoreRun;
  }

  private boolean isOldDriverTest() {
    return Boolean.TRUE.toString().equals(System.getenv("is_old_driver"));
  }

  private class IgnoreStatement extends Statement {
    private SfTest sfTest;

    private IgnoreStatement(SfTest sfTest) {
      this.sfTest = sfTest;
    }

    @Override
    public void evaluate() {
      Assume.assumeTrue(
          "Test ignored, as available since "
              + sfTest.sinceVersion()
              + ", current version "
              + currentVersion,
          false);
    }
  }

  static class Version implements Comparable<Version> {
    private final int major;
    private final int minor;
    private final int patch;

    Version(int major, int minor, int patch) {
      this.major = major;
      this.minor = minor;
      this.patch = patch;
    }

    static Version parse(String str) {
      if (str == null) {
        return null;
      }
      List<Integer> split =
          Arrays.stream(str.split("\\.")).map(Integer::valueOf).collect(Collectors.toList());
      if (split.size() != 3) {
        throw new ExceptionInInitializerError(str + " should be in a form of x.y.z");
      }
      return new Version(split.get(0), split.get(1), split.get(2));
    }

    @Override
    public int compareTo(Version o) {
      if (major != o.major) {
        return major - o.major;
      } else if (minor != o.minor) {
        return minor - o.minor;
      } else {
        return patch - o.patch;
      }
    }

    @Override
    public String toString() {
      return String.format("%d.%d.%d", major, minor, patch);
    }
  }
}
