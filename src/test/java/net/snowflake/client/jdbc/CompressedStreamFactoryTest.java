package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.junit.jupiter.api.Test;

public class CompressedStreamFactoryTest {

  private final CompressedStreamFactory factory = new CompressedStreamFactory();

  @Test
  public void testDetectContentEncodingAndGetInputStream_Gzip() throws Exception {
    // Original data to compress and validate
    String originalData = "Some data in GZIP";

    // Creating encoding header
    Header encodingHeader = new BasicHeader("Content-Encoding", "gzip");

    // Creating a gzip byte array using GZIPOutputStream
    byte[] gzipData;
    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream)) {
      gzipOutputStream.write(originalData.getBytes(StandardCharsets.UTF_8));
      gzipOutputStream.close(); // close to flush and finish the compression
      gzipData = byteArrayOutputStream.toByteArray();
    }

    // Mocking input stream with the gzip data
    InputStream gzipStream = new ByteArrayInputStream(gzipData);

    // Call the private method using reflection
    InputStream resultStream = factory.createBasedOnEncodingHeader(gzipStream, encodingHeader);

    // Decompress and validate the data matches original
    assertTrue(resultStream instanceof GZIPInputStream);
    String decompressedData = IOUtils.toString(resultStream, StandardCharsets.UTF_8);
    assertEquals(originalData, decompressedData);
  }

  @Test
  public void testDetectContentEncodingAndGetInputStream_Zstd() throws Exception {
    // Original data to compress and validate
    String originalData = "Some data in ZSTD";

    // Creating encoding header
    Header encodingHeader = new BasicHeader("Content-Encoding", "zstd");

    // Creating a zstd byte array using ZstdOutputStream
    byte[] zstdData;
    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ZstdOutputStream zstdOutputStream = new ZstdOutputStream(byteArrayOutputStream)) {
      zstdOutputStream.write(originalData.getBytes(StandardCharsets.UTF_8));
      zstdOutputStream.close(); // close to flush and finish the compression
      zstdData = byteArrayOutputStream.toByteArray();
    }

    // Mocking input stream with the zstd data
    InputStream zstdStream = new ByteArrayInputStream(zstdData);

    // Call the private method using reflection
    InputStream resultStream = factory.createBasedOnEncodingHeader(zstdStream, encodingHeader);

    // Decompress and validate the data matches original
    assertTrue(resultStream instanceof ZstdInputStream);
    String decompressedData = IOUtils.toString(resultStream, StandardCharsets.UTF_8);
    assertEquals(originalData, decompressedData);
  }
}
