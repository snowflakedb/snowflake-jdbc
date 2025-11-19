package net.snowflake.client.jdbc;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import net.snowflake.client.core.HttpClientSettingsKey;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.SFBaseStatement;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.SFStatement;
import org.apache.http.client.methods.HttpPost;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class DataframeAstTest {

  @Test
  public void testSendAst() throws SQLException, IOException {
    String ast = "dummyAst";
    SnowflakeConnectionV1 mockedConn = mock(SnowflakeConnectionV1.class);
    SFConnectionHandler mockedHandler = mock(SFConnectionHandler.class);
    when(mockedConn.getHandler()).thenReturn(mockedHandler);
    HttpClientSettingsKey mockedHttpClientSettingsKey = mock(HttpClientSettingsKey.class);
    SFSession mockedSession = mock(SFSession.class);
    SFBaseStatement sfBaseStatement = new SFStatement(mockedSession);
    when(mockedHandler.getSFStatement()).thenReturn(sfBaseStatement);
    when(mockedSession.getServerUrl()).thenReturn("dummy");
    when(mockedSession.getHttpClientKey()).thenReturn(mockedHttpClientSettingsKey);
    when(mockedHttpClientSettingsKey.getGzipDisabled()).thenReturn(true);

    ArgumentCaptor<HttpPost> captor = ArgumentCaptor.forClass(HttpPost.class);
    try (MockedStatic<HttpUtil> mockedHttpUtil = Mockito.mockStatic(HttpUtil.class)) {
      mockedHttpUtil
          .when(
              () ->
                  HttpUtil.executeRequest(
                      captor.capture(),
                      anyInt(),
                      anyInt(),
                      anyInt(),
                      anyInt(),
                      anyInt(),
                      any(),
                      anyBoolean(),
                      anyBoolean(),
                      any(),
                      any()))
          .thenReturn("{\"result\":\"dummy\"}");
      SnowflakeStatementV1 stmt =
          new SnowflakeStatementV1(
              mockedConn,
              ResultSet.TYPE_FORWARD_ONLY,
              ResultSet.CONCUR_READ_ONLY,
              ResultSet.CLOSE_CURSORS_AT_COMMIT);
      stmt.executeDataframeAst(ast);
    } catch (Exception e) {
      // do nothing
      // the remaining part is not related to this feature, let it terminate early.
    }
    HttpPost result = captor.getValue();
    ObjectMapper mapper = new ObjectMapper();
    ByteArrayInputStream bais = (ByteArrayInputStream) result.getEntity().getContent();
    int size = bais.available();
    byte[] buffer = new byte[size];
    bais.read(buffer);
    String resultStr = new String(buffer, StandardCharsets.UTF_8);
    assert mapper.readTree(resultStr).get("dataframeAst").asText().equals(ast);
  }
}
