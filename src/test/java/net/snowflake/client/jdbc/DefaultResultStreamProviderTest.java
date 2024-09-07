package net.snowflake.client.jdbc;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.luben.zstd.ZstdInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.entity.InputStreamEntity;
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

    // Creating a valid gzip byte array using a string compressed with GZIPOutputStream
    byte[] validGzipData;
    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream)) {
      gzipOutputStream.write("Some data in GZIP".getBytes());
      gzipOutputStream.finish();
      validGzipData = byteArrayOutputStream.toByteArray();
    }

    // Mocking input stream with the valid gzip data
    InputStream gzipStream = new ByteArrayInputStream(validGzipData);

    // Call the private method using reflection
    InputStream resultStream =
        invokeDetectContentEncodingAndGetInputStream(mockResponse, gzipStream);

    assertTrue(resultStream instanceof GZIPInputStream);
  }

  @Test
  public void testDetectContentEncodingAndGetInputStream_Zstd() throws Exception {
    // Mocking zstd content encoding
    Header encodingHeader = mock(Header.class);
    when(encodingHeader.getValue()).thenReturn("zstd");
    when(mockResponse.getFirstHeader("Content-Encoding")).thenReturn(encodingHeader);

    // Mocking input stream with zstd data
    byte[] zstdData = new byte[] {0x28, (byte) 0xb5, 0x2f}; // Zstd magic numbers
    InputStream zstdStream = new ByteArrayInputStream(zstdData);
    when(mockResponse.getEntity()).thenReturn(new InputStreamEntity(zstdStream));

    // Call the private method using reflection
    InputStream resultStream =
        invokeDetectContentEncodingAndGetInputStream(mockResponse, zstdStream);

    assertTrue(resultStream instanceof ZstdInputStream);
  }
}
