package net.snowflake.client.internal.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import net.snowflake.client.internal.core.OCSPMode;
import org.junit.jupiter.api.Test;

public class SnowflakeFileTransferConfigTest {

  private static final SnowflakeFileTransferMetadataV1 SNOWFLAKE_FILE_TRANSFER_METADATA =
      new SnowflakeFileTransferMetadataV1(
          "", "", "", "", 0L, SFBaseFileTransferAgent.CommandType.UPLOAD, null);
  private static final ByteArrayInputStream UPLOAD_STREAM = new ByteArrayInputStream(new byte[0]);

  @Test
  public void shouldBuildDefaultConfig() {
    SnowflakeFileTransferConfig config = createObligatoryConfigPartInBuilder().build();

    assertObligatoryParameters(config);
    assertFalse(config.isSilentException());
  }

  private static void assertObligatoryParameters(SnowflakeFileTransferConfig config) {
    assertNotNull(config);
    assertEquals(SNOWFLAKE_FILE_TRANSFER_METADATA, config.getSnowflakeFileTransferMetadata());
    assertEquals(UPLOAD_STREAM, config.getUploadStream());
    assertEquals(OCSPMode.DISABLE_OCSP_CHECKS, config.getOcspMode());
  }

  @Test
  public void shouldBuildConfig() {
    SnowflakeFileTransferConfig config =
        createObligatoryConfigPartInBuilder().setSilentException(true).build();

    assertObligatoryParameters(config);
    assertTrue(config.isSilentException());
  }

  @Test
  public void defaultConfigHas128MiBBufferThreshold() {
    SnowflakeFileTransferConfig config = createObligatoryConfigPartInBuilder().build();
    assertEquals(
        SnowflakeFileTransferAgent.DEFAULT_FILE_BACKED_BUFFER_THRESHOLD,
        config.getFileBackedBufferThreshold());
  }

  @Test
  public void customBufferThresholdIsPreserved() {
    int threshold = 1024 * 1024; // 1 MiB
    SnowflakeFileTransferConfig config =
        createObligatoryConfigPartInBuilder().setFileBackedBufferThreshold(threshold).build();
    assertEquals(threshold, config.getFileBackedBufferThreshold());
  }

  @Test
  public void zeroBufferThresholdThrowsIllegalArgumentException() {
    assertThrows(
        IllegalArgumentException.class,
        () -> createObligatoryConfigPartInBuilder().setFileBackedBufferThreshold(0));
  }

  @Test
  public void negativeBufferThresholdThrowsIllegalArgumentException() {
    assertThrows(
        IllegalArgumentException.class,
        () -> createObligatoryConfigPartInBuilder().setFileBackedBufferThreshold(-1));
  }

  private static SnowflakeFileTransferConfig.Builder createObligatoryConfigPartInBuilder() {
    return SnowflakeFileTransferConfig.Builder.newInstance()
        .setSnowflakeFileTransferMetadata(SNOWFLAKE_FILE_TRANSFER_METADATA)
        .setUploadStream(UPLOAD_STREAM)
        .setOcspMode(OCSPMode.DISABLE_OCSP_CHECKS);
  }
}
