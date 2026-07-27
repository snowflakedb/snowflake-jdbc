package net.snowflake.client.internal.util;

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

  private static Map<String, String> cachedOsDetails = null;

  public static synchronized Map<String, String> load() {
    if (cachedOsDetails != null) {
      return cachedOsDetails;
    }

    if (Constants.getOS() != Constants.OS.LINUX) {
      logger.trace("OS details collection skipped: not running on Linux");
      cachedOsDetails = Collections.emptyMap();
      return cachedOsDetails;
    }

    cachedOsDetails = loadFromPath(Paths.get(DEFAULT_OS_RELEASE_PATH));
    return cachedOsDetails;
  }

  static Map<String, String> loadFromPath(Path path) {
    try {
      byte[] bytes = Files.readAllBytes(path);
      String content = new String(bytes, StandardCharsets.UTF_8);
      return parse(content);
    } catch (IOException e) {
      logger.debug("Failed to read OS details from {}: {}", path, e.getMessage());
      return Collections.emptyMap();
    } catch (Exception e) {
      logger.debug("Unexpected error reading OS details from {}: {}", path, e.getMessage());
      return Collections.emptyMap();
    }
  }

  static Map<String, String> parse(String content) {
    Map<String, String> details = new HashMap<>();
    if (content == null || content.isEmpty()) {
      return details;
    }

    for (String line : content.split("\n")) {
      line = line.trim();
      if (line.isEmpty() || line.startsWith("#")) {
        continue;
      }

      Matcher matcher = KEY_VALUE_PATTERN.matcher(line);
      if (matcher.matches()) {
        String key = matcher.group(1).trim();
        if (ALLOWED_KEYS.contains(key)) {
          String value = parseValue(matcher.group(2));
          details.put(key, value);
        }
      }
    }

    return Collections.unmodifiableMap(details);
  }

  private static String parseValue(String value) {
    if (value == null) {
      return null;
    }

    value = value.trim();
    if (value.isEmpty()) {
      return value;
    }

    char firstChar = value.charAt(0);

    // Handle quoted values (single or double quotes)
    if (firstChar == '"' || firstChar == '\'') {
      int endQuote = value.indexOf(firstChar, 1);
      return endQuote > 0 ? value.substring(1, endQuote) : value.substring(1).trim();
    }

    // Unquoted value - strip inline comment if present
    int commentIndex = value.indexOf('#');
    return commentIndex > 0 ? value.substring(0, commentIndex).trim() : value;
  }
}
