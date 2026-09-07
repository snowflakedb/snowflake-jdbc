package net.snowflake.client.internal.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.zip.GZIPInputStream;
import net.snowflake.client.api.exception.ErrorCode;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.api.http.HttpHeadersCustomizer;
import net.snowflake.client.internal.core.HttpUtil;
import net.snowflake.client.internal.core.SFBaseSession;
import net.snowflake.client.internal.core.SFSession;
import net.snowflake.client.internal.exception.SnowflakeSQLLoggedException;
import net.snowflake.client.internal.jdbc.telemetry.ExecTimeTelemetryData;
import net.snowflake.client.internal.log.ArgSupplier;
import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;
import net.snowflake.client.internal.util.SecretDetector;
import net.snowflake.common.core.SqlState;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;

public class DefaultResultStreamProvider implements ResultStreamProvider {
  private static final SFLogger logger =
      SFLoggerFactory.getLogger(DefaultResultStreamProvider.class);
  // SSE-C algorithm header
  private static final String SSE_C_ALGORITHM = "x-amz-server-side-encryption-customer-algorithm";

  // SSE-C customer key header
  private static final String SSE_C_KEY = "x-amz-server-side-encryption-customer-key";

  // SSE-C algorithm value
  private static final String SSE_C_AES = "AES256";

  private CompressedStreamFactory compressedStreamFactory;

  public DefaultResultStreamProvider() {
    this.compressedStreamFactory = new CompressedStreamFactory();
  }

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
              + context.getResultChunk().getScrubbedUrl()
              + " "
              + ex.getLocalizedMessage());
    }

    /*
     * return error if we don't get a response or the response code
     * means failure.
     */
    if (response == null || response.getStatusLine().getStatusCode() != 200) {
      logger.error("Error fetching chunk from: {}", context.getResultChunk().getScrubbedUrl());

      SnowflakeUtil.logResponseDetails(response, logger);

      throw new SnowflakeSQLException(
          SqlState.IO_ERROR,
          ErrorCode.NETWORK_ERROR.getMessageCode(),
          "Error encountered when downloading a result chunk: HTTP "
              + "status: "
              + ((response != null) ? response.getStatusLine().getStatusCode() : "null response"));
    }

    InputStream inputStream;
    final HttpEntity entity = response.getEntity();
    Header encoding = response.getFirstHeader("Content-Encoding");
    try {
      // create stream based on compression type
      inputStream =
          compressedStreamFactory.createBasedOnEncodingHeader(entity.getContent(), encoding);
    } catch (Exception ex) {
      logger.error(
          "Failed to decompress data: {}", (ArgSupplier) () -> describeForLogging(response));

      throw new SnowflakeSQLLoggedException(
          context.getSession(),
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          SqlState.INTERNAL_ERROR,
          "Failed to decompress data: " + describeForLogging(response));
    }

    // trace the response if requested
    logger.debug("Json response: {}", (ArgSupplier) () -> describeForLogging(response));

    return inputStream;
  }

  /**
   * Renders a response for a log message using its status line and its header names. Header values
   * are omitted: a response can carry the chunk encryption key and other credential material in its
   * headers, and {@link org.apache.http.HttpResponse#toString()} would render every one of them.
   *
   * @param response the response to describe, may be null
   * @return a description safe to place in a log message
   */
  private static String describeForLogging(HttpResponse response) {
    if (response == null) {
      return "null response";
    }

    StringBuilder description = new StringBuilder();
    StatusLine statusLine = response.getStatusLine();
    if (statusLine != null) {
      description
          .append("status: ")
          .append(statusLine.getStatusCode())
          .append(" ")
          .append(statusLine.getReasonPhrase());
    } else {
      description.append("status: unknown");
    }

    Header[] headers = response.getAllHeaders();
    if (headers != null) {
      StringJoiner headerNames = new StringJoiner(", ", "[", "]");
      for (Header header : headers) {
        headerNames.add(header.getName());
      }
      description.append(", header names: ").append(headerNames);
    }

    return description.toString();
  }

  private HttpResponse getResultChunk(ChunkDownloadContext context) throws Exception {
    URIBuilder uriBuilder = new URIBuilder(context.getResultChunk().getUrl());

    HttpGet httpRequest = new HttpGet(uriBuilder.build());

    if (context.getChunkHeadersMap() != null && context.getChunkHeadersMap().size() != 0) {
      for (Map.Entry<String, String> entry : context.getChunkHeadersMap().entrySet()) {
        logger.debug("Adding header key: {}", entry.getKey());
        httpRequest.addHeader(entry.getKey(), entry.getValue());
      }
    }
    // Add SSE-C headers
    else if (context.getQrmk() != null) {
      httpRequest.addHeader(SSE_C_ALGORITHM, SSE_C_AES);
      httpRequest.addHeader(SSE_C_KEY, context.getQrmk());
      logger.debug("Adding SSE-C headers", false);
    }

    logger.debug(
        "Thread {} Fetching result chunk#{}: {}",
        Thread.currentThread().getId(),
        context.getChunkIndex(),
        context.getResultChunk().getScrubbedUrl());

    SFBaseSession session = context.getSession();
    List<HttpHeadersCustomizer> headersCustomizers = null;
    if (session instanceof SFSession) {
      headersCustomizers = ((SFSession) session).getHttpHeadersCustomizers();
    }
    CloseableHttpClient httpClient =
        HttpUtil.getHttpClient(
            context.getChunkDownloader().getHttpClientSettingsKey(), headersCustomizers);

    // fetch the result chunk
    HttpResponse response =
        RestRequest.executeWithRetries(
                httpClient,
                httpRequest,
                context.getNetworkTimeoutInMilli() / 1000, // retry timeout
                0,
                context.getSocketTimeout(),
                0,
                0, // no socket timeout injection
                null, // no canceling
                false, // no cookie
                false, // no retry parameters in url
                false, // no request_guid
                true, // retry on HTTP403 for AWS S3
                true, // no retry on http request
                false,
                new ExecTimeTelemetryData(),
                session,
                context.getChunkDownloader().getHttpClientSettingsKey(),
                headersCustomizers,
                false)
            .getHttpResponse();

    logger.debug(
        "Thread {} Call chunk#{} returned for URL: {}, response: {}",
        Thread.currentThread().getId(),
        context.getChunkIndex(),
        (ArgSupplier) () -> SecretDetector.maskSASToken(context.getResultChunk().getUrl()),
        (ArgSupplier) () -> describeForLogging(response));
    return response;
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
