package net.snowflake.client.jdbc;

import static net.snowflake.client.core.Constants.MB;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import net.snowflake.client.core.ExecTimeTelemetryData;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.log.ArgSupplier;
import net.snowflake.client.util.SecretDetector;
import net.snowflake.common.core.SqlState;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;

public class DefaultResultStreamProvider implements ResultStreamProvider {
  // SSE-C algorithm header
  private static final String SSE_C_ALGORITHM = "x-amz-server-side-encryption-customer-algorithm";

  // SSE-C customer key header
  private static final String SSE_C_KEY = "x-amz-server-side-encryption-customer-key";

  // SSE-C algorithm value
  private static final String SSE_C_AES = "AES256";

  private static final int STREAM_BUFFER_SIZE = MB;

  @Override
  public InputStream getInputStream(ChunkDownloadContext context) throws Exception {
    HttpResponse response;
    try {
      response = getResultChunk(context);
    } catch (URISyntaxException | IOException ex) {
      throw new SnowflakeSQLLoggedException(
          context.getSession(),
          ErrorCode.NETWORK_ERROR.getMessageCode(),
          SqlState.IO_ERROR,
          "Error encountered when request a result chunk URL: "
              + context.getResultChunk().getUrl()
              + " "
              + ex.getLocalizedMessage());
    }

    /*
     * return error if we don't get a response or the response code
     * means failure.
     */
    if (response == null || response.getStatusLine().getStatusCode() != 200) {
      SnowflakeResultSetSerializableV1.logger.error(
          "Error fetching chunk from: {}", context.getResultChunk().getScrubbedUrl());

      SnowflakeUtil.logResponseDetails(response, SnowflakeResultSetSerializableV1.logger);

      throw new SnowflakeSQLException(
          SqlState.IO_ERROR,
          ErrorCode.NETWORK_ERROR.getMessageCode(),
          "Error encountered when downloading a result chunk: HTTP "
              + "status="
              + ((response != null) ? response.getStatusLine().getStatusCode() : "null response"));
    }

    InputStream inputStream;
    final HttpEntity entity = response.getEntity();
    try {
      // read the chunk data
      inputStream = detectContentEncodingAndGetInputStream(response, entity.getContent());
    } catch (Exception ex) {
      SnowflakeResultSetSerializableV1.logger.error("Failed to decompress data: {}", response);

      throw new SnowflakeSQLLoggedException(
          context.getSession(),
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          SqlState.INTERNAL_ERROR,
          "Failed to decompress data: " + response.toString());
    }

    // trace the response if requested
    SnowflakeResultSetSerializableV1.logger.debug("Json response: {}", response);

    return inputStream;
  }

  private HttpResponse getResultChunk(ChunkDownloadContext context) throws Exception {
    URIBuilder uriBuilder = new URIBuilder(context.getResultChunk().getUrl());

    HttpGet httpRequest = new HttpGet(uriBuilder.build());

    if (context.getChunkHeadersMap() != null && context.getChunkHeadersMap().size() != 0) {
      for (Map.Entry<String, String> entry : context.getChunkHeadersMap().entrySet()) {
        SnowflakeResultSetSerializableV1.logger.debug(
            "Adding header key={}, value={}", entry.getKey(), entry.getValue());
        httpRequest.addHeader(entry.getKey(), entry.getValue());
      }
    }
    // Add SSE-C headers
    else if (context.getQrmk() != null) {
      httpRequest.addHeader(SSE_C_ALGORITHM, SSE_C_AES);
      httpRequest.addHeader(SSE_C_KEY, context.getQrmk());
      SnowflakeResultSetSerializableV1.logger.debug("Adding SSE-C headers", false);
    }

    SnowflakeResultSetSerializableV1.logger.debug(
        "Thread {} Fetching result #chunk{}: {}",
        Thread.currentThread().getId(),
        context.getChunkIndex(),
        context.getResultChunk().getScrubbedUrl());

    // TODO move this s3 request to HttpUtil class. In theory, upper layer
    // TODO does not need to know about http client
    CloseableHttpClient httpClient =
        HttpUtil.getHttpClient(context.getChunkDownloader().getHttpClientSettingsKey());

    // fetch the result chunk
    HttpResponse response =
        RestRequest.execute(
            httpClient,
            httpRequest,
            context.getNetworkTimeoutInMilli() / 1000, // retry timeout
            context.getAuthTimeout(),
            context.getSocketTimeout(),
            0,
            0, // no socket timeout injection
            null, // no canceling
            false, // no cookie
            false, // no retry parameters in url
            false, // no request_guid
            true, // retry on HTTP403 for AWS S3
            true, // no retry on http request
            new ExecTimeTelemetryData());

    SnowflakeResultSetSerializableV1.logger.debug(
        "Thread {} Call #chunk{} returned for URL: {}, response={}",
        Thread.currentThread().getId(),
        context.getChunkIndex(),
        (ArgSupplier) () -> SecretDetector.maskSASToken(context.getResultChunk().getUrl()),
        response);
    return response;
  }

  private InputStream detectContentEncodingAndGetInputStream(HttpResponse response, InputStream is)
      throws IOException, SnowflakeSQLException {
    InputStream inputStream = is; // Determine the format of the response, if it is not
    // either plain text or gzip, raise an error.
    Header encoding = response.getFirstHeader("Content-Encoding");
    if (encoding != null) {
      if ("gzip".equalsIgnoreCase(encoding.getValue())) {
        /* specify buffer size for GZIPInputStream */
        inputStream = new GZIPInputStream(is, STREAM_BUFFER_SIZE);
      } else {
        throw new SnowflakeSQLException(
            SqlState.INTERNAL_ERROR,
            ErrorCode.INTERNAL_ERROR.getMessageCode(),
            "Exception: unexpected compression got " + encoding.getValue());
      }
    } else {
      inputStream = detectGzipAndGetStream(is);
    }

    return inputStream;
  }

  public static InputStream detectGzipAndGetStream(InputStream is) throws IOException {
    PushbackInputStream pb = new PushbackInputStream(is, 2);
    byte[] signature = new byte[2];
    int len = pb.read(signature);
    pb.unread(signature, 0, len);
    // https://tools.ietf.org/html/rfc1952
    if (signature[0] == (byte) 0x1f && signature[1] == (byte) 0x8b) {
      return new GZIPInputStream(pb);
    } else {
      return pb;
    }
  }
}
