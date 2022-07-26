/*
 * Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.category.TestCategoryOthers;
import net.snowflake.client.core.*;
import org.apache.http.client.methods.*;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(TestCategoryOthers.class)
public class ChunkDownloaderS3RetryUrlLatestIT extends AbstractDriverIT {

  private Connection connection;
  private SFStatement sfStatement;
  private SFBaseSession sfBaseSession;
  private ChunkDownloadContext sfContext;

  @Before
  public void setup() throws SQLException, InterruptedException {
    connection = getConnection();
    sfBaseSession = connection.unwrap(SnowflakeConnectionV1.class).getSFBaseSession();
    Statement statement = connection.createStatement();
    sfStatement = statement.unwrap(SnowflakeStatementV1.class).getSfStatement();
    int rowCount = 170000;
    ResultSet rs =
        statement.executeQuery(
            "select randstr(100, random()) from table(generator(rowcount => " + rowCount + "))");
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
            downloader, chunk, qrmk, 0, chunkHeadersMap, 0, 0, 0, sfBaseSession);
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
        true); // retry HTTP 403

    assertFalse(getRequest.containsHeader("retryCount"));
    assertFalse(getRequest.containsHeader("clientStartTime"));
    assertFalse(getRequest.containsHeader("request_guid"));
  }
}
