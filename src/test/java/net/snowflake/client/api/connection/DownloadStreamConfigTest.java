package net.snowflake.client.api.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class DownloadStreamConfigTest {

  @Test
  public void testBuilderWithAllFields() {
    DownloadStreamConfig config =
        DownloadStreamConfig.builder()
            .setStageName("@my_stage")
            .setSourceFileName("data/file.csv")
            .setDecompress(true)
            .build();

    assertEquals("@my_stage", config.getStageName());
    assertEquals("data/file.csv", config.getSourceFileName());
    assertTrue(config.isDecompress());
  }

  @Test
  public void testBuilderWithMinimalFields() {
    DownloadStreamConfig config =
        DownloadStreamConfig.builder()
            .setStageName("@my_stage")
            .setSourceFileName("file.csv")
            .build();

    assertEquals("@my_stage", config.getStageName());
    assertEquals("file.csv", config.getSourceFileName());
    assertFalse(config.isDecompress()); // default is false
  }

  @Test
  public void testBuilderTrimsWhitespace() {
    DownloadStreamConfig config =
        DownloadStreamConfig.builder()
            .setStageName("  @my_stage  ")
            .setSourceFileName("  data/file.csv  ")
            .build();

    assertEquals("@my_stage", config.getStageName());
    assertEquals("data/file.csv", config.getSourceFileName());
  }

  @Test
  public void testBuilderWithUserStage() {
    DownloadStreamConfig config =
        DownloadStreamConfig.builder().setStageName("@~").setSourceFileName("file.csv").build();

    assertEquals("@~", config.getStageName());
  }

  @Test
  public void testBuilderWithTableStage() {
    DownloadStreamConfig config =
        DownloadStreamConfig.builder()
            .setStageName("@%my_table")
            .setSourceFileName("file.csv")
            .build();

    assertEquals("@%my_table", config.getStageName());
  }

  @Test
  public void testBuilderWithNestedPath() {
    DownloadStreamConfig config =
        DownloadStreamConfig.builder()
            .setStageName("@my_stage")
            .setSourceFileName("2024/01/data.csv.gz")
            .setDecompress(true)
            .build();

    assertEquals("2024/01/data.csv.gz", config.getSourceFileName());
  }

  @Test
  public void testBuildThrowsExceptionWhenStageNameNull() {
    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () -> DownloadStreamConfig.builder().setSourceFileName("file.csv").build());

    assertEquals("stageName is required", exception.getMessage());
  }

  @Test
  public void testBuildThrowsExceptionWhenStageNameEmpty() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DownloadStreamConfig.builder().setStageName("   "));

    assertEquals("stageName cannot be null or empty", exception.getMessage());
  }

  @Test
  public void testBuildThrowsExceptionWhenSourceFileNameNull() {
    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () -> DownloadStreamConfig.builder().setStageName("@my_stage").build());

    assertEquals("sourceFileName is required", exception.getMessage());
  }

  @Test
  public void testBuildThrowsExceptionWhenSourceFileNameEmpty() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DownloadStreamConfig.builder().setSourceFileName(""));

    assertEquals("sourceFileName cannot be null or empty", exception.getMessage());
  }

  @Test
  public void testSetStageNameNull() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DownloadStreamConfig.builder().setStageName(null));

    assertEquals("stageName cannot be null or empty", exception.getMessage());
  }

  @Test
  public void testSetSourceFileNameNull() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DownloadStreamConfig.builder().setSourceFileName(null));

    assertEquals("sourceFileName cannot be null or empty", exception.getMessage());
  }

  @Test
  public void testToString() {
    DownloadStreamConfig config =
        DownloadStreamConfig.builder()
            .setStageName("@my_stage")
            .setSourceFileName("file.csv")
            .setDecompress(true)
            .build();

    String toString = config.toString();
    assertTrue(toString.contains("stageName='@my_stage'"));
    assertTrue(toString.contains("sourceFileName='file.csv'"));
    assertTrue(toString.contains("decompress=true"));
  }

  @Test
  public void testBuilderMethodChaining() {
    // Verify all setter methods return builder for method chaining
    DownloadStreamConfig.Builder builder = DownloadStreamConfig.builder();

    assertSame(builder, builder.setStageName("@stage"));
    assertSame(builder, builder.setSourceFileName("file.csv"));
    assertSame(builder, builder.setDecompress(true));
  }
}
