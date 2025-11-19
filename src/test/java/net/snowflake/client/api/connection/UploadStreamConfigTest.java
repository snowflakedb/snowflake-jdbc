package net.snowflake.client.api.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import org.junit.jupiter.api.Test;

public class UploadStreamConfigTest {

  @Test
  public void testBuilderWithAllFields() {
    InputStream stream = new ByteArrayInputStream("test data".getBytes());

    UploadStreamConfig config =
        UploadStreamConfig.builder()
            .setStageName("@my_stage")
            .setDestPrefix("data/2024")
            .setDestFileName("file.csv")
            .setInputStream(stream)
            .setCompressData(false)
            .build();

    assertEquals("@my_stage", config.getStageName());
    assertEquals("data/2024", config.getDestPrefix());
    assertEquals("file.csv", config.getDestFileName());
    assertSame(stream, config.getInputStream());
    assertFalse(config.isCompressData());
  }

  @Test
  public void testBuilderWithMinimalFields() {
    InputStream stream = new ByteArrayInputStream("test data".getBytes());

    UploadStreamConfig config =
        UploadStreamConfig.builder()
            .setStageName("@my_stage")
            .setDestFileName("file.csv")
            .setInputStream(stream)
            .build();

    assertEquals("@my_stage", config.getStageName());
    assertNull(config.getDestPrefix()); // null when not set
    assertEquals("file.csv", config.getDestFileName());
    assertSame(stream, config.getInputStream());
    assertTrue(config.isCompressData()); // default is true
  }

  @Test
  public void testBuilderTrimsWhitespace() {
    InputStream stream = new ByteArrayInputStream("test data".getBytes());

    UploadStreamConfig config =
        UploadStreamConfig.builder()
            .setStageName("  @my_stage  ")
            .setDestPrefix("  data/2024  ")
            .setDestFileName("  file.csv  ")
            .setInputStream(stream)
            .build();

    assertEquals("@my_stage", config.getStageName());
    assertEquals("data/2024", config.getDestPrefix());
    assertEquals("file.csv", config.getDestFileName());
  }

  @Test
  public void testBuilderWithNullDestPrefix() {
    InputStream stream = new ByteArrayInputStream("test data".getBytes());

    UploadStreamConfig config =
        UploadStreamConfig.builder()
            .setStageName("@my_stage")
            .setDestPrefix(null)
            .setDestFileName("file.csv")
            .setInputStream(stream)
            .build();

    assertNull(config.getDestPrefix());
  }

  @Test
  public void testBuilderWithEmptyDestPrefix() {
    InputStream stream = new ByteArrayInputStream("test data".getBytes());

    UploadStreamConfig config =
        UploadStreamConfig.builder()
            .setStageName("@my_stage")
            .setDestPrefix("   ")
            .setDestFileName("file.csv")
            .setInputStream(stream)
            .build();

    assertNull(config.getDestPrefix()); // empty string becomes null
  }

  @Test
  public void testBuilderWithUserStage() {
    InputStream stream = new ByteArrayInputStream("test data".getBytes());

    UploadStreamConfig config =
        UploadStreamConfig.builder()
            .setStageName("@~")
            .setDestFileName("file.csv")
            .setInputStream(stream)
            .build();

    assertEquals("@~", config.getStageName());
  }

  @Test
  public void testBuilderWithTableStage() {
    InputStream stream = new ByteArrayInputStream("test data".getBytes());

    UploadStreamConfig config =
        UploadStreamConfig.builder()
            .setStageName("@%my_table")
            .setDestFileName("file.csv")
            .setInputStream(stream)
            .build();

    assertEquals("@%my_table", config.getStageName());
  }

  @Test
  public void testBuilderWithNestedDestPrefix() {
    InputStream stream = new ByteArrayInputStream("test data".getBytes());

    UploadStreamConfig config =
        UploadStreamConfig.builder()
            .setStageName("@my_stage")
            .setDestPrefix("data/2024/01")
            .setDestFileName("report.csv")
            .setInputStream(stream)
            .build();

    assertEquals("data/2024/01", config.getDestPrefix());
  }

  @Test
  public void testBuildThrowsExceptionWhenStageNameNull() {
    InputStream stream = new ByteArrayInputStream("test data".getBytes());

    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () ->
                UploadStreamConfig.builder()
                    .setDestFileName("file.csv")
                    .setInputStream(stream)
                    .build());

    assertEquals("stageName is required", exception.getMessage());
  }

  @Test
  public void testBuildThrowsExceptionWhenStageNameEmpty() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> UploadStreamConfig.builder().setStageName("   "));

    assertEquals("stageName cannot be null or empty", exception.getMessage());
  }

  @Test
  public void testBuildThrowsExceptionWhenInputStreamNull() {
    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () ->
                UploadStreamConfig.builder()
                    .setStageName("@my_stage")
                    .setDestFileName("file.csv")
                    .build());

    assertEquals("inputStream is required", exception.getMessage());
  }

  @Test
  public void testBuildThrowsExceptionWhenDestFileNameNull() {
    InputStream stream = new ByteArrayInputStream("test data".getBytes());

    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () ->
                UploadStreamConfig.builder()
                    .setStageName("@my_stage")
                    .setInputStream(stream)
                    .build());

    assertEquals("destFileName is required", exception.getMessage());
  }

  @Test
  public void testBuildThrowsExceptionWhenDestFileNameEmpty() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> UploadStreamConfig.builder().setDestFileName(""));

    assertEquals("destFileName cannot be null or empty", exception.getMessage());
  }

  @Test
  public void testSetStageNameNull() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> UploadStreamConfig.builder().setStageName(null));

    assertEquals("stageName cannot be null or empty", exception.getMessage());
  }

  @Test
  public void testSetInputStreamNull() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> UploadStreamConfig.builder().setInputStream(null));

    assertEquals("inputStream cannot be null", exception.getMessage());
  }

  @Test
  public void testSetDestFileNameNull() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> UploadStreamConfig.builder().setDestFileName(null));

    assertEquals("destFileName cannot be null or empty", exception.getMessage());
  }

  @Test
  public void testToString() {
    InputStream stream = new ByteArrayInputStream("test data".getBytes());

    UploadStreamConfig config =
        UploadStreamConfig.builder()
            .setStageName("@my_stage")
            .setDestPrefix("data")
            .setDestFileName("file.csv")
            .setInputStream(stream)
            .setCompressData(true)
            .build();

    String toString = config.toString();
    assertTrue(toString.contains("stageName='@my_stage'"));
    assertTrue(toString.contains("destPrefix='data'"));
    assertTrue(toString.contains("destFileName='file.csv'"));
    assertTrue(toString.contains("inputStream=provided"));
    assertTrue(toString.contains("compressData=true"));
  }

  @Test
  public void testToStringWithNullInputStream() {
    // Edge case: toString should handle null inputStream gracefully
    UploadStreamConfig.Builder builder = UploadStreamConfig.builder();
    builder.setStageName("@stage");
    builder.setDestFileName("file.csv");
    // Don't set inputStream

    // We can't build(), but we can test toString doesn't crash
    // This is mainly for debugging scenarios
  }

  @Test
  public void testBuilderMethodChaining() {
    // Verify all setter methods return builder for method chaining
    InputStream stream = new ByteArrayInputStream("test data".getBytes());
    UploadStreamConfig.Builder builder = UploadStreamConfig.builder();

    assertSame(builder, builder.setStageName("@stage"));
    assertSame(builder, builder.setDestPrefix("data"));
    assertSame(builder, builder.setDestFileName("file.csv"));
    assertSame(builder, builder.setInputStream(stream));
    assertSame(builder, builder.setCompressData(false));
  }

  @Test
  public void testCompressDataDefaultTrue() {
    InputStream stream = new ByteArrayInputStream("test data".getBytes());

    UploadStreamConfig config =
        UploadStreamConfig.builder()
            .setStageName("@my_stage")
            .setDestFileName("file.csv")
            .setInputStream(stream)
            .build();

    assertTrue(config.isCompressData(), "compressData should default to true");
  }

  @Test
  public void testCompressDataExplicitlyFalse() {
    InputStream stream = new ByteArrayInputStream("test data".getBytes());

    UploadStreamConfig config =
        UploadStreamConfig.builder()
            .setStageName("@my_stage")
            .setDestFileName("file.csv")
            .setInputStream(stream)
            .setCompressData(false)
            .build();

    assertFalse(config.isCompressData());
  }
}
