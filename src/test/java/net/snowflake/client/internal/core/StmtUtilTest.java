package net.snowflake.client.internal.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.internal.core.StmtUtil.StmtInput;
import net.snowflake.client.internal.jdbc.BaseJDBCTest;
import net.snowflake.client.internal.jdbc.telemetry.ExecTimeTelemetryData;
import org.apache.http.Header;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.MockedStatic.Verification;
import org.mockito.Mockito;

@Tag(TestTags.CORE)
public class StmtUtilTest extends BaseJDBCTest {

  /** SNOW-862760 Verify that additional headers are added to request */
  @Test
  public void testForwardedHeaders() throws Throwable {
    SFLoginInput input = createLoginInput();
    Map<String, String> additionalHeaders = new HashMap<>();
    additionalHeaders.put("Extra-Snowflake-Header", "present");
    input.setAdditionalHttpHeadersForSnowsight(additionalHeaders);

    try (MockedStatic<HttpUtil> mockedHttpUtil = mockStatic(HttpUtil.class)) {
      // Both mocks the call _and_ verifies that the headers are forwarded.
      Verification httpCalledWithHeaders =
          () ->
              HttpUtil.executeRequest(
                  Mockito.argThat(
                      arg -> {
                        for (Entry<String, String> definedHeader : additionalHeaders.entrySet()) {
                          Header actualHeader = arg.getLastHeader(definedHeader.getKey());
                          if (actualHeader == null) {
                            return false;
                          }

                          if (!definedHeader.getValue().equals(actualHeader.getValue())) {
                            return false;
                          }
                        }

                        return true;
                      }),
                  Mockito.anyInt(),
                  Mockito.anyInt(),
                  Mockito.anyInt(),
                  Mockito.anyInt(),
                  Mockito.anyInt(),
                  Mockito.nullable(AtomicBoolean.class),
                  Mockito.anyBoolean(),
                  Mockito.anyBoolean(),
                  Mockito.nullable(HttpClientSettingsKey.class),
                  Mockito.nullable(ExecTimeTelemetryData.class),
                  Mockito.nullable(SFBaseSession.class));
      mockedHttpUtil
          .when(httpCalledWithHeaders)
          .thenReturn("{\"data\":null,\"code\":333334,\"message\":null,\"success\":true}");

      mockedHttpUtil
          .when(() -> HttpUtil.applyAdditionalHeadersForSnowsight(any(), any()))
          .thenCallRealMethod();

      StmtInput stmtInput = new StmtInput();
      stmtInput.setAdditionalHttpHeadersForSnowsight(additionalHeaders);
      // Async mode skips result post-processing so we don't need to mock an advanced
      // response
      stmtInput.setAsync(true);
      stmtInput.setHttpClientSettingsKey(new HttpClientSettingsKey(OCSPMode.FAIL_OPEN));
      stmtInput.setRequestId(UUID.randomUUID().toString());
      stmtInput.setServiceName("MOCK_SERVICE_NAME");
      stmtInput.setServerUrl("MOCK_SERVER_URL");
      stmtInput.setSessionToken("MOCK_SESSION_TOKEN");
      stmtInput.setSequenceId(1);
      stmtInput.setSql("SELECT * FROM MOCK_TABLE");

      StmtUtil.execute(stmtInput, new ExecTimeTelemetryData(), null);

      // After login, the only invocation to http should have been with the new
      // headers.
      // No calls should have happened without additional headers.
      mockedHttpUtil.verify(httpCalledWithHeaders, times(1));
    }
  }

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** SNOW-3063492 Verify QCC is merged from a failed query response */
  @Test
  public void testUpdateQueryContextFromFailedResponse() throws Exception {
    String failedResponse =
        "{\"data\":{\"errorCode\":\"200001\",\"sqlState\":\"22000\","
            + "\"queryId\":\"test-query-id\","
            + "\"queryContext\":{\"entries\":[{\"id\":0,\"timestamp\":123456789,\"priority\":0,\"context\":\"opaque\"}]}"
            + "},\"code\":\"200001\","
            + "\"message\":\"A primary key already exists.\","
            + "\"success\":false}";

    JsonNode responseJson = MAPPER.readTree(failedResponse);
    SFSession session = createSessionWithQCC();

    StmtUtil.updateQueryContextFromResponse(responseJson, session);

    QueryContextDTO qcc = session.getQueryContextDTO();
    assertNotNull(qcc, "QCC should be populated from failed response");
    assertEquals(1, qcc.getEntries().size());
    assertEquals(0, qcc.getEntries().get(0).getId());
    assertEquals(123456789L, qcc.getEntries().get(0).getTimestamp());
  }

  private static SFSession createSessionWithQCC() {
    SFSession session = new SFSession();
    session.qcc = new QueryContextCache(session.getQueryContextCacheSize());
    return session;
  }

  private SFLoginInput createLoginInput() {
    SFLoginInput input = new SFLoginInput();
    input.setServerUrl("MOCK_TEST_HOST");
    input.setUserName("MOCK_USERNAME");
    input.setPassword("MOCK_PASSWORD");
    input.setAccountName("MOCK_ACCOUNT_NAME");
    input.setAppId("MOCK_APP_ID");
    input.setOCSPMode(OCSPMode.FAIL_OPEN);
    input.setHttpClientSettingsKey(new HttpClientSettingsKey(OCSPMode.FAIL_OPEN));
    input.setLoginTimeout(1000);
    input.setSessionParameters(new HashMap<>());

    return input;
  }
}
