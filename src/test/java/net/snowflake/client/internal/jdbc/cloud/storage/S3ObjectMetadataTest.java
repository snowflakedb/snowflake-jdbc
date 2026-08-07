package net.snowflake.client.internal.jdbc.cloud.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class S3ObjectMetadataTest {

  /**
   * Real S3 endpoints decode the aws-chunked checksum trailer server-side, so the S3 upload path
   * keeps requesting the CRC32 flexible checksum by default.
   */
  @Test
  public void requestsCrc32ChecksumByDefault() {
    S3ObjectMetadata metadata = new S3ObjectMetadata();
    metadata.setContentLength(3);

    PutObjectRequest request = metadata.getS3PutObjectRequest();

    assertEquals(ChecksumAlgorithm.CRC32, request.checksumAlgorithm());
  }

  /**
   * SNOW-3888537: on the GCS S3-interop upload path the streaming request body makes AWS SDK v2
   * deliver the checksum as an aws-chunked trailer that GCS stores verbatim, corrupting the file.
   * When the checksum is disabled the PutObjectRequest must carry no checksum algorithm.
   */
  @Test
  public void doesNotRequestChecksumWhenDisabled() {
    S3ObjectMetadata metadata = new S3ObjectMetadata();
    metadata.setContentLength(3);
    metadata.setRequestIntegrityChecksum(false);

    PutObjectRequest request = metadata.getS3PutObjectRequest();

    assertNull(request.checksumAlgorithm());
  }
}
