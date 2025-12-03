package net.snowflake.client.core;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.internal.core.minicore.Minicore;
import net.snowflake.client.jdbc.BaseWiremockTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.CORE)
public class MinicoreTelemetryWiremockIT extends BaseWiremockTest {

  private static final String LOGIN_MAPPING_PATH =
      "/wiremock/mappings/minicore/minicore_telemetry.json";

  private final String WIREMOCK_HOST_WITH_HTTPS_AND_PORT =
      "https://" + WIREMOCK_HOST + ":" + wiremockHttpsPort;

  @Test
  public void testMinicoreTelemetryIncludedInLoginRequest() throws Exception, SFException {
    Minicore.initializeAsync();
    await().atMost(Duration.ofSeconds(5)).until(() -> Minicore.getInstance() != null);

    importMappingFromResources(LOGIN_MAPPING_PATH);
    setCustomTrustStorePropertyPath();

    SFLoginInput loginInput = createLoginInput();
    Map<SFSessionProperty, Object> connectionPropertiesMap = new HashMap<>();
    connectionPropertiesMap.put(SFSessionProperty.TRACING, "ALL");

    SessionUtil.openSession(loginInput, connectionPropertiesMap, "ALL");

    verifyRequestCount(1, "/session/v1/login-request.*");
  }

  private SFLoginInput createLoginInput() {
    SFLoginInput input = new SFLoginInput();
    input.setServerUrl(WIREMOCK_HOST_WITH_HTTPS_AND_PORT);
    input.setUserName("TEST_USER");
    input.setPassword("TEST_PASSWORD");
    input.setAccountName("TEST_ACCOUNT");
    input.setAppId("TEST_APP_ID");
    input.setOCSPMode(OCSPMode.FAIL_OPEN);
    input.setHttpClientSettingsKey(new HttpClientSettingsKey(OCSPMode.FAIL_OPEN));
    input.setLoginTimeout(30);
    input.setSessionParameters(new HashMap<>());
    return input;
  }
}
