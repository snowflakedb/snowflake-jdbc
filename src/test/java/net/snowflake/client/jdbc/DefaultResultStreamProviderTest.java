package net.snowflake.client.jdbc;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.junit.Before;
import org.junit.Test;

public class DefaultResultStreamProviderTest {

  private DefaultResultStreamProvider resultStreamProvider;
  private HttpResponse mockResponse;

  @Before
  public void setUp() {
    resultStreamProvider = new DefaultResultStreamProvider();
    mockResponse = mock(HttpResponse.class);
  }

  private InputStream invokeDetectContentEncodingAndGetInputStream(
      HttpResponse response, InputStream inputStream) throws Exception {
    Method method =
        DefaultResultStreamProvider.class.getDeclaredMethod(
            "detectContentEncodingAndGetInputStream", HttpResponse.class, InputStream.class);
    method.setAccessible(true);
    return (InputStream) method.invoke(resultStreamProvider, response, inputStream);
  }

  @Test
  public void testDetectContentEncodingAndGetInputStream_Gzip() throws Exception {
    // Mocking gzip content encoding
    Header encodingHeader = mock(Header.class);
    when(encodingHeader.getValue()).thenReturn("gzip");
    when(mockResponse.getFirstHeader("Content-Encoding")).thenReturn(encodingHeader);

    // Original data to compress and validate
    String originalData = "Some data in GZIP";

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
    InputStream resultStream =
        invokeDetectContentEncodingAndGetInputStream(mockResponse, gzipStream);

    // Decompress and validate the data matches original
    ByteArrayOutputStream decompressedOutput = new ByteArrayOutputStream();
    byte[] buffer = new byte[1024];
    int bytesRead;
    try (GZIPInputStream gzipInputStream = (GZIPInputStream) resultStream) {
      while ((bytesRead = gzipInputStream.read(buffer)) != -1) {
        decompressedOutput.write(buffer, 0, bytesRead);
      }
    }
    String decompressedData = new String(decompressedOutput.toByteArray(), StandardCharsets.UTF_8);

    assertEquals(originalData, decompressedData);
  }

  @Test
  public void testDetectContentEncodingAndGetInputStream_Zstd() throws Exception {
    // Mocking zstd content encoding
    Header encodingHeader = mock(Header.class);
    when(encodingHeader.getValue()).thenReturn("zstd");
    when(mockResponse.getFirstHeader("Content-Encoding")).thenReturn(encodingHeader);

    // Original data to compress and validate
    String originalData = "Some data in ZSTD";

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
    InputStream resultStream =
        invokeDetectContentEncodingAndGetInputStream(mockResponse, zstdStream);

    // Decompress and validate the data matches original
    ByteArrayOutputStream decompressedOutput = new ByteArrayOutputStream();
    byte[] buffer = new byte[1024];
    int bytesRead;
    try (ZstdInputStream zstdInputStream = (ZstdInputStream) resultStream) {
      while ((bytesRead = zstdInputStream.read(buffer)) != -1) {
        decompressedOutput.write(buffer, 0, bytesRead);
      }
    }
    String decompressedData = new String(decompressedOutput.toByteArray(), StandardCharsets.UTF_8);

    assertEquals(originalData, decompressedData);
  }
}
