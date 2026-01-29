package net.snowflake.client.internal.core.minicore;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.snowflake.client.internal.core.Constants;
import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;

/** Parses Linux distribution details from /etc/os-release for telemetry purposes. */
public class OsReleaseDetails {

  private static final SFLogger logger = SFLoggerFactory.getLogger(OsReleaseDetails.class);

  private static final String DEFAULT_OS_RELEASE_PATH = "/etc/os-release";
  private static final Pattern KEY_VALUE_PATTERN = Pattern.compile("^([A-Z0-9_]+)=(.*)$");

  private static final Set<String> ALLOWED_KEYS =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  "NAME",
                  "PRETTY_NAME",
                  "ID",
                  "IMAGE_ID",
                  "IMAGE_VERSION",
                  "BUILD_ID",
                  "VERSION",
                  "VERSION_ID")));

  private static Path testOverridePath = null;

  public static void setTestOverridePath(Path path) {
    testOverridePath = path;
  }

  static Map<String, String> load() {
    if (testOverridePath != null) {
      return loadFromPath(testOverridePath);
    }

    // Only collect on Linux in production
    if (Constants.getOS() != Constants.OS.LINUX) {
      logger.trace("OS details collection skipped: not running on Linux");
      return Collections.emptyMap();
    }

    return loadFromPath(Paths.get(DEFAULT_OS_RELEASE_PATH));
  }

  static Map<String, String> loadFromPath(Path path) {
    try {
      byte[] bytes = Files.readAllBytes(path);
      String content = new String(bytes, StandardCharsets.UTF_8);
      return parse(content);
    } catch (IOException e) {
      logger.trace("Failed to read OS details from {}: {}", path, e.getMessage());
      return Collections.emptyMap();
    } catch (Exception e) {
      logger.trace("Unexpected error reading OS details from {}: {}", path, e.getMessage());
      return Collections.emptyMap();
    }
  }

  static Map<String, String> parse(String content) {
    Map<String, String> details = new HashMap<>();
    if (content == null || content.isEmpty()) {
      return details;
    }

    for (String line : content.split("\n")) {
      if (line.isEmpty() || line.startsWith("#")) {
        continue;
      }

      Matcher matcher = KEY_VALUE_PATTERN.matcher(line);
      if (matcher.matches()) {
        String key = matcher.group(1);
        if (ALLOWED_KEYS.contains(key)) {
          String value = trimQuotes(matcher.group(2));
          details.put(key, value);
        }
      }
    }

    return Collections.unmodifiableMap(details);
  }

  /** Remove surrounding double quotes from value if present. */
  private static String trimQuotes(String value) {
    if (value != null && value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
      return value.substring(1, value.length() - 1);
    }
    return value;
  }
}
