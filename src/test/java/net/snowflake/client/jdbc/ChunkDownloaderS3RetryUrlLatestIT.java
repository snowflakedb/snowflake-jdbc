package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.ExecTimeTelemetryData;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFStatement;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.OTHERS)
public class ChunkDownloaderS3RetryUrlLatestIT extends AbstractDriverIT {

  private SFStatement sfStatement;
  private SFBaseSession sfBaseSession;
  private ChunkDownloadContext sfContext;

  @BeforeEach
  public void setup() throws SQLException, InterruptedException {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      sfBaseSession = connection.unwrap(SnowflakeConnectionV1.class).getSFBaseSession();
      sfStatement = statement.unwrap(SnowflakeStatementV1.class).getSfStatement();
      int rowCount = 170000;
      try (ResultSet rs =
          statement.executeQuery(
              "select randstr(100, random()) from table(generator(rowcount => "
                  + rowCount
                  + "))")) {
        List<SnowflakeResultSetSerializable> resultSetSerializables =
            ((SnowflakeResultSet) rs).getResultSetSerializables(100 * 1024 * 1024);
        SnowflakeResultSetSerializable resultSetSerializable = resultSetSerializables.get(0);
        SnowflakeChunkDownloader downloader =
            new SnowflakeChunkDownloader((SnowflakeResultSetSerializableV1) resultSetSerializable);
        SnowflakeResultChunk chunk = downloader.getNextChunkToConsume();
        String qrmk = ((SnowflakeResultSetSerializableV1) resultSetSerializable).getQrmk();
        Map<String, String> chunkHeadersMap =
            ((SnowflakeResultSetSerializableV1) resultSetSerializable).getChunkHeadersMap();
        sfContext =
            new ChunkDownloadContext(
                downloader, chunk, qrmk, 0, chunkHeadersMap, 0, 0, 0, 7, sfBaseSession);
      }
    }
  }

  /**
   * Tests that the driver does not modify the presigned-URL used to download result chunks from AWS
   * S3 bucket
   */
  @Test
  public void testParamsInRetryS3Url() throws Exception {
    HttpGet getRequest = new HttpGet(new URIBuilder(sfContext.getResultChunk().getUrl()).build());
    CloseableHttpClient httpClient =
        HttpUtil.getHttpClient(sfContext.getChunkDownloader().getHttpClientSettingsKey());
    for (Map.Entry<String, String> entry : sfContext.getChunkHeadersMap().entrySet()) {
      getRequest.addHeader(entry.getKey(), entry.getValue());
    }
    RestRequest.execute(
        httpClient,
        getRequest,
        sfContext.getNetworkTimeoutInMilli() / 1000,
        sfContext.getAuthTimeout(),
        sfContext.getSocketTimeout(),
        1, // retry count
        0, // inject socket timeout
        null, // cancelling
        false, // without cookies
        false, // include retry parameters
        false, // include request GUID
        true,
        new ExecTimeTelemetryData()); // retry HTTP 403

    assertFalse(getRequest.containsHeader("retryCount"));
    assertFalse(getRequest.containsHeader("retryReason"));
    assertFalse(getRequest.containsHeader("clientStartTime"));
    assertFalse(getRequest.containsHeader("request_guid"));
  }
}
