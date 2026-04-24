package net.snowflake.client.internal.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import net.snowflake.client.annotations.RunOnLinux;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class LibcDetailsTest {

  @AfterEach
  public void tearDown() {
    LibcDetails.resetCacheForTesting();
  }

  // -------------------- parseLddContent: glibc --------------------

  @Test
  public void testParseLddContentGlibc() {
    String content =
        "ldd (Ubuntu GLIBC 2.31-0ubuntu9.16) 2.31\n"
            + "Copyright (C) 2020 Free Software Foundation, Inc.\n"
            + "This is free software; see the source for copying conditions.  There is NO\n"
            + "warranty; not even for MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.\n"
            + "Written by Roland McGrath and Ulrich Drepper.\n"
            + "GNU C Library (Ubuntu GLIBC 2.31-0ubuntu9.16) stable release version 2.31.\n";

    LibcInfo result = LibcDetails.parseLddContent(content);

    assertNotNull(result);
    assertEquals(LibcDetails.GLIBC, result.getFamily());
    assertEquals("2.31", result.getVersion());
  }

  @Test
  public void testParseLddContentGlibcRhel() {
    String content =
        "GNU C Library (GNU libc) stable release version 2.17, by Roland McGrath et al.\n";

    LibcInfo result = LibcDetails.parseLddContent(content);

    assertNotNull(result);
    assertEquals(LibcDetails.GLIBC, result.getFamily());
    assertEquals("2.17", result.getVersion());
  }

  // -------------------- parseLddContent: musl --------------------

  @Test
  public void testParseLddContentMuslWithVersion() {
    String content = "musl libc (x86_64)\nVersion 1.2.3\n";

    LibcInfo result = LibcDetails.parseLddContent(content);

    assertNotNull(result);
    assertEquals(LibcDetails.MUSL, result.getFamily());
    assertEquals("1.2.3", result.getVersion());
  }

  @Test
  public void testParseLddContentMuslWithoutVersion() {
    String content = "musl libc (x86_64)\n";

    LibcInfo result = LibcDetails.parseLddContent(content);

    assertNotNull(result);
    assertEquals(LibcDetails.MUSL, result.getFamily());
    assertNull(result.getVersion());
  }

  @Test
  public void testParseLddContentMuslWithExtendedVersion() {
    String content = "musl libc (aarch64)\nVersion 1.2.5_git20240924\n";

    LibcInfo result = LibcDetails.parseLddContent(content);

    assertNotNull(result);
    assertEquals(LibcDetails.MUSL, result.getFamily());
    // The regex grabs the numeric/dot prefix only.
    assertEquals("1.2.5", result.getVersion());
  }

  // -------------------- parseLddContent: unknown / empty --------------------

  @Test
  public void testParseLddContentEmpty() {
    assertNull(LibcDetails.parseLddContent(""));
    assertNull(LibcDetails.parseLddContent(null));
  }

  @Test
  public void testParseLddContentUnknown() {
    assertNull(LibcDetails.parseLddContent("some unrelated content here"));
  }

  @Test
  public void testParseLddContentDoesNotMatchSubstringFalsePositives() {
    // "musl" must be matched as a whole word, not as a substring of e.g. "muslib" or "muscle".
    assertNull(LibcDetails.parseLddContent("muslib version 1.0\n"));
    assertNull(LibcDetails.parseLddContent("muscle library v1\n"));
    assertNull(LibcDetails.parseLddContent("emusl 0.1\n"));
  }

  // -------------------- detectFromFilesystem --------------------

  @Test
  public void testDetectFromFilesystemReadsAndParses(@TempDir Path tempDir) throws IOException {
    Path lddFile = tempDir.resolve("ldd");
    Files.write(
        lddFile,
        "GNU C Library (GNU libc) stable release version 2.34.\n".getBytes(StandardCharsets.UTF_8));

    LibcInfo result = LibcDetails.detectFromFilesystem(lddFile);

    assertNotNull(result);
    assertEquals(LibcDetails.GLIBC, result.getFamily());
    assertEquals("2.34", result.getVersion());
  }

  @Test
  public void testDetectFromFilesystemMissingFileReturnsNull(@TempDir Path tempDir) {
    Path missing = tempDir.resolve("does-not-exist");
    assertNull(LibcDetails.detectFromFilesystem(missing));
  }

  @Test
  public void testParseCommandOutputGlibcFromGetconf() {
    String output = "glibc 2.17\nldd (GNU libc) 2.17\n";

    LibcInfo result = LibcDetails.parseCommandOutput(output);

    assertNotNull(result);
    assertEquals(LibcDetails.GLIBC, result.getFamily());
    assertEquals("2.17", result.getVersion());
  }

  @Test
  public void testParseCommandOutputMuslFromLdd() {
    String output =
        "getconf: UNKNOWN variable GNU_LIBC_VERSION\n" + "musl libc (x86_64)\n" + "Version 1.2.2\n";

    LibcInfo result = LibcDetails.parseCommandOutput(output);

    assertNotNull(result);
    assertEquals(LibcDetails.MUSL, result.getFamily());
    assertEquals("1.2.2", result.getVersion());
  }

  @Test
  public void testParseCommandOutputMuslWithoutVersion() {
    String output = "getconf: UNKNOWN variable GNU_LIBC_VERSION\nmusl libc (x86_64)\n";

    LibcInfo result = LibcDetails.parseCommandOutput(output);

    assertNotNull(result);
    assertEquals(LibcDetails.MUSL, result.getFamily());
    assertNull(result.getVersion());
  }

  @Test
  public void testParseCommandOutputUnknown() {
    assertNull(LibcDetails.parseCommandOutput("some unrelated\noutput here\n"));
    assertNull(LibcDetails.parseCommandOutput(""));
    assertNull(LibcDetails.parseCommandOutput(null));
  }

  @Test
  @RunOnLinux
  public void testLoadOnLinuxReturnsKnownFamily() {
    LibcInfo result = LibcDetails.load();
    assertNotNull(result);
    // On Linux we expect to detect at least the family. Version may still be null if neither
    // /usr/bin/ldd is present nor getconf/ldd are on PATH, so we don't assert on version.
    String family = result.getFamily();
    assertTrue(
        LibcDetails.GLIBC.equals(family) || LibcDetails.MUSL.equals(family),
        "On Linux, family must be glibc or musl, got: " + family);
  }
}
