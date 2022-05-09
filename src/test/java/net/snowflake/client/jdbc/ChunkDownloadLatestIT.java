package net.snowflake.client.jdbc;

import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.SFStatement;
import net.snowflake.client.jdbc.cloud.storage.SnowflakeGCSClient;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static net.snowflake.client.AbstractDriverIT.getConnection;
import static org.junit.Assert.assertEquals;

public class ChunkDownloadLatestIT {

    private Connection connection;
    private SFStatement sfStatement;
    private SFSession sfSession;
    private SFBaseSession sfBaseSession;
    private String command;
    private SnowflakeGCSClient spyingClient;
    private int overMaxRetry;
    private int maxRetry;

    @Before
    public void setup() throws SQLException {

    }

    @Test
    public void testParamsInRetryUrl() throws Exception {
        Properties paramProperties = new Properties();
        connection = getConnection("s3testaccount", paramProperties);
        sfSession = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();
        sfBaseSession = connection.unwrap(SnowflakeConnectionV1.class).getSFBaseSession();
        Statement statement = connection.createStatement();
        sfStatement = statement.unwrap(SnowflakeStatementV1.class).getSfStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM \"SNOWFLAKE_SAMPLE_DATA\".\"TPCH_SF1000\".\"PART\" ");

        List<SnowflakeResultSetSerializable> resultSetSerializables =
                ((SnowflakeResultSet) rs).getResultSetSerializables(100 * 1024 * 1024);

        SnowflakeResultSetSerializable resultSetSerializable = resultSetSerializables.get(0);

        SnowflakeChunkDownloader downloader = new SnowflakeChunkDownloader((SnowflakeResultSetSerializableV1) resultSetSerializable);
        SnowflakeResultChunk chunk = downloader.getNextChunkToConsume();
        String qrmk = ((SnowflakeResultSetSerializableV1) resultSetSerializable)
                .getQrmk();
        Map<String, String> chunkHeadersMap = ((SnowflakeResultSetSerializableV1) resultSetSerializable)
                .getChunkHeadersMap();
        ChunkDownloadContext context = new ChunkDownloadContext(downloader,
                chunk,
                qrmk,
                0,
                chunkHeadersMap,
                0,
                0,
                0,
                sfBaseSession);
        DefaultResultStreamProvider provider = new DefaultResultStreamProvider();
        RestRequest request = new RestRequest();
        RestRequest spy = Mockito.spy(request);
      //  Mockito.doReturn().when(spy)
    }
}
