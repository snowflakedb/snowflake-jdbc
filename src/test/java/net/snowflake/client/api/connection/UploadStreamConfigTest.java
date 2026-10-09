package net.snowflake.client.api.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class UploadStreamConfigTest {

  @Test
  public void defaultConfigHas128MiBBufferThreshold() {
    UploadStreamConfig config = UploadStreamConfig.builder().build();
    assertEquals(1 << 27, config.getFileBackedBufferThreshold());
  }

  @Test
  public void customBufferThresholdIsPreserved() {
    int threshold = 1024 * 1024; // 1 MiB
    UploadStreamConfig config =
        UploadStreamConfig.builder().setFileBackedBufferThreshold(threshold).build();
    assertEquals(threshold, config.getFileBackedBufferThreshold());
  }

  @Test
  public void zeroBufferThresholdThrowsIllegalArgumentException() {
    assertThrows(
        IllegalArgumentException.class,
        () -> UploadStreamConfig.builder().setFileBackedBufferThreshold(0));
  }

  @Test
  public void negativeBufferThresholdThrowsIllegalArgumentException() {
    assertThrows(
        IllegalArgumentException.class,
        () -> UploadStreamConfig.builder().setFileBackedBufferThreshold(-1));
  }

  @Test
  public void toStringIncludesBufferThreshold() {
    UploadStreamConfig config =
        UploadStreamConfig.builder().setFileBackedBufferThreshold(512).build();
    assertTrue(config.toString().contains("fileBackedBufferThreshold=512"));
  }

  @Test
  public void builderDefaultsAreNotAffectedByBufferThreshold() {
    UploadStreamConfig config =
        UploadStreamConfig.builder().setFileBackedBufferThreshold(4096).build();
    assertTrue(config.isCompressData(), "compressData should still default to true");
    assertEquals(null, config.getDestPrefix(), "destPrefix should still default to null");
  }
}
