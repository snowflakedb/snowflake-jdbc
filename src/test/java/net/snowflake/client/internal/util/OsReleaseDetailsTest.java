package net.snowflake.client.internal.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;
import org.junit.jupiter.api.Test;

public class OsReleaseDetailsTest {

  @Test
  public void testParseArchLinuxOsRelease() {
    String content =
        "NAME=\"Arch Linux\"\n"
            + "PRETTY_NAME=\"Arch Linux\"\n"
            + "ID=arch\n"
            + "BUILD_ID=rolling\n"
            + "VERSION_ID=20251019.0.436919\n"
            + "ANSI_COLOR=\"38;2;23;147;209\"\n"
            + "HOME_URL=\"https://archlinux.org/\"\n"
            + "DOCUMENTATION_URL=\"https://wiki.archlinux.org/\"\n"
            + "SUPPORT_URL=\"https://bbs.archlinux.org/\"\n"
            + "BUG_REPORT_URL=\"https://gitlab.archlinux.org/groups/archlinux/-/issues\"\n"
            + "PRIVACY_POLICY_URL=\"https://terms.archlinux.org/docs/privacy-policy/\"\n"
            + "LOGO=archlinux-logo\n";

    Map<String, String> result = OsReleaseDetails.parse(content);

    assertEquals("Arch Linux", result.get("NAME"));
    assertEquals("Arch Linux", result.get("PRETTY_NAME"));
    assertEquals("arch", result.get("ID"));
    assertEquals("rolling", result.get("BUILD_ID"));
    assertEquals("20251019.0.436919", result.get("VERSION_ID"));
    assertEquals(5, result.size());
  }

  @Test
  public void testParseUbuntuOsRelease() {
    String content =
        "NAME=\"Ubuntu\"\n"
            + "VERSION=\"22.04.3 LTS (Jammy Jellyfish)\"\n"
            + "ID=ubuntu\n"
            + "ID_LIKE=debian\n"
            + "PRETTY_NAME=\"Ubuntu 22.04.3 LTS\"\n"
            + "VERSION_ID=\"22.04\"\n"
            + "VERSION_CODENAME=jammy\n"
            + "HOME_URL=\"https://www.ubuntu.com/\"\n";

    Map<String, String> result = OsReleaseDetails.parse(content);

    assertEquals("Ubuntu", result.get("NAME"));
    assertEquals("22.04.3 LTS (Jammy Jellyfish)", result.get("VERSION"));
    assertEquals("ubuntu", result.get("ID"));
    assertEquals("Ubuntu 22.04.3 LTS", result.get("PRETTY_NAME"));
    assertEquals("22.04", result.get("VERSION_ID"));
    assertEquals(5, result.size());
  }

  @Test
  public void testParseAlpineLinuxOsRelease() {
    String content =
        "NAME=\"Alpine Linux\"\n"
            + "ID=alpine\n"
            + "VERSION_ID=3.18.4\n"
            + "PRETTY_NAME=\"Alpine Linux v3.18\"\n"
            + "HOME_URL=\"https://alpinelinux.org/\"\n";

    Map<String, String> result = OsReleaseDetails.parse(content);

    assertEquals("Alpine Linux", result.get("NAME"));
    assertEquals("alpine", result.get("ID"));
    assertEquals("3.18.4", result.get("VERSION_ID"));
    assertEquals("Alpine Linux v3.18", result.get("PRETTY_NAME"));
    assertEquals(4, result.size());
  }

  @Test
  public void testParseWithImageFields() {
    // Some container images have IMAGE_ID and IMAGE_VERSION
    String content =
        "NAME=\"Container OS\"\n"
            + "ID=container\n"
            + "IMAGE_ID=\"myimage\"\n"
            + "IMAGE_VERSION=\"1.2.3\"\n";

    Map<String, String> result = OsReleaseDetails.parse(content);

    assertEquals("Container OS", result.get("NAME"));
    assertEquals("container", result.get("ID"));
    assertEquals("myimage", result.get("IMAGE_ID"));
    assertEquals("1.2.3", result.get("IMAGE_VERSION"));
    assertEquals(4, result.size());
  }

  @Test
  public void testParseWithEmptyLines() {
    String content = "NAME=\"Test Linux\"\n" + "\n" + "\n" + "ID=test\n";

    Map<String, String> result = OsReleaseDetails.parse(content);

    assertEquals("Test Linux", result.get("NAME"));
    assertEquals("test", result.get("ID"));
    assertEquals(2, result.size());
  }

  @Test
  public void testParseUnquotedValues() {
    String content = "ID=arch\n" + "BUILD_ID=rolling\n" + "VERSION_ID=20251019.0.436919\n";

    Map<String, String> result = OsReleaseDetails.parse(content);

    assertEquals("arch", result.get("ID"));
    assertEquals("rolling", result.get("BUILD_ID"));
    assertEquals("20251019.0.436919", result.get("VERSION_ID"));
  }

  @Test
  public void testParseQuotedValues() {
    String content =
        "NAME=\"Ubuntu\"\n" + "VERSION=\"22.04.3 LTS\"\n" + "PRETTY_NAME=\"Ubuntu 22.04.3 LTS\"\n";

    Map<String, String> result = OsReleaseDetails.parse(content);

    assertEquals("Ubuntu", result.get("NAME"));
    assertEquals("22.04.3 LTS", result.get("VERSION"));
    assertEquals("Ubuntu 22.04.3 LTS", result.get("PRETTY_NAME"));
  }

  @Test
  public void testParseEmptyValue() {
    // Some fields may have empty values
    String content = "NAME=\"\"\n" + "ID=test\n";

    Map<String, String> result = OsReleaseDetails.parse(content);

    // Empty quoted value should be captured
    assertEquals("", result.get("NAME"));
    assertEquals("test", result.get("ID"));
  }

  @Test
  public void testParseSingleQuotedValues() {
    String content = "NAME='Ubuntu'\n" + "VERSION='22.04.3 LTS'\n" + "ID='ubuntu'\n";

    Map<String, String> result = OsReleaseDetails.parse(content);

    assertEquals("Ubuntu", result.get("NAME"));
    assertEquals("22.04.3 LTS", result.get("VERSION"));
    assertEquals("ubuntu", result.get("ID"));
  }

  @Test
  public void testParseValueWithSpacesAroundEquals() {
    // Some implementations might have spaces around the equals sign
    String content = "NAME= \"Ubuntu\"\n" + "ID= ubuntu\n" + "VERSION= '22.04'\n";

    Map<String, String> result = OsReleaseDetails.parse(content);

    assertEquals("Ubuntu", result.get("NAME"));
    assertEquals("ubuntu", result.get("ID"));
    assertEquals("22.04", result.get("VERSION"));
  }

  @Test
  public void testParseValueWithInlineComment() {
    // Unquoted values can have inline comments
    String content = "ID=ubuntu # this is a comment\n" + "VERSION_ID=22.04 # another comment\n";

    Map<String, String> result = OsReleaseDetails.parse(content);

    assertEquals("ubuntu", result.get("ID"));
    assertEquals("22.04", result.get("VERSION_ID"));
  }

  @Test
  public void testParseQuotedValueWithHashSymbol() {
    // Hash inside quotes should not be treated as comment
    String content = "NAME=\"Ubuntu #1\"\n" + "PRETTY_NAME='Test #2 Distro'\n";

    Map<String, String> result = OsReleaseDetails.parse(content);

    assertEquals("Ubuntu #1", result.get("NAME"));
    assertEquals("Test #2 Distro", result.get("PRETTY_NAME"));
  }

  @Test
  public void testParseQuotedValueWithInlineComment() {
    // Single-quoted value followed by inline comment
    String content =
        "NAME='Ubuntu' # the OS name\n"
            + "ID=\"ubuntu\" # lowercase id\n"
            + "VERSION='22.04 LTS' # long term support\n";

    Map<String, String> result = OsReleaseDetails.parse(content);

    assertEquals("Ubuntu", result.get("NAME"));
    assertEquals("ubuntu", result.get("ID"));
    assertEquals("22.04 LTS", result.get("VERSION"));
  }

  @Test
  public void testLoadFromFile() throws Exception {
    Path osReleaseFile =
        Paths.get(Objects.requireNonNull(getClass().getResource("/os-release-test")).toURI());

    Map<String, String> result = OsReleaseDetails.loadFromPath(osReleaseFile);

    assertEquals("Test Linux", result.get("NAME"));
    assertEquals("Test Linux 1.0 LTS", result.get("PRETTY_NAME"));
    assertEquals("testlinux", result.get("ID"));
    assertEquals("testlinux-cloud", result.get("IMAGE_ID"));
    assertEquals("1.0.0-cloud", result.get("IMAGE_VERSION"));
    assertEquals("20260130", result.get("BUILD_ID"));
    assertEquals("1.0 LTS (Test Release)", result.get("VERSION"));
    assertEquals("1.0.0", result.get("VERSION_ID"));
    assertEquals(8, result.size());
  }
}
