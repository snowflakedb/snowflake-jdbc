package net.snowflake.client.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import net.snowflake.client.core.ExecTimeTelemetryData;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.log.ArgSupplier;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.client.util.SecretDetector;
import net.snowflake.common.core.SqlState;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
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
              + context.getResultChunk().getUrl()
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
      logger.error("Failed to decompress data: {}", response);

      throw new SnowflakeSQLLoggedException(
          context.getSession(),
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          SqlState.INTERNAL_ERROR,
          "Failed to decompress data: " + response.toString());
    }

    // trace the response if requested
    logger.debug("Json response: {}", response);

    return inputStream;
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

    logger.debug(
        "Thread {} Call chunk#{} returned for URL: {}, response: {}",
        Thread.currentThread().getId(),
        context.getChunkIndex(),
        (ArgSupplier) () -> SecretDetector.maskSASToken(context.getResultChunk().getUrl()),
        response);
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
