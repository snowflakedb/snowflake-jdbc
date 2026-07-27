package net.snowflake.client.internal.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Collections;
import net.snowflake.client.api.driver.SnowflakeDriver;
import net.snowflake.client.internal.jdbc.telemetry.TelemetryClient;
import net.snowflake.client.internal.jdbc.telemetry.TelemetryField;
import net.snowflake.common.core.LoginInfoDTO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

/**
 * Unit tests for {@link ConnectionIdentifierShapeTelemetry}. Mirrors the Go reference ({@code
 * connection_identifier_shape_telemetry_test.go}) and the Python reference ({@code
 * test_connection_identifier_shape_telemetry.py}): emits exactly one record with the expected
 * payload, honors the env-var kill switch case-insensitively, and treats every non-"true" value
 * (including former truthy aliases like {@code "1"} / {@code "yes"}) as enabled.
 */
public class ConnectionIdentifierShapeTelemetryTest {

  private static ConnectionIdentifierShape sampleShape() {
    return ConnectionIdentifierShape.captureFromTomlConfig(
        Collections.singletonMap("account", "myorg-myacct.us-east-1"));
  }

  @Test
  public void emit_writesExpectedPayload() {
    TelemetryClient client = mock(TelemetryClient.class);

    ConnectionIdentifierShapeTelemetry.emit(client, sampleShape(), /* disableEnvValue= */ null);

    ArgumentCaptor<ObjectNode> captor = forClass(ObjectNode.class);
    verify(client, times(1)).addLogToBatch(captor.capture(), anyLong());

    ObjectNode msg = captor.getValue();
    assertEquals(
        TelemetryField.CONNECTION_IDENTIFIER_SHAPE.toString(),
        msg.get(TelemetryField.TYPE.toString()).asText());
    assertEquals(LoginInfoDTO.SF_JDBC_APP_ID, msg.get(TelemetryField.SOURCE.toString()).asText());
    assertEquals(
        LoginInfoDTO.SF_JDBC_APP_ID, msg.get(TelemetryField.DRIVER_TYPE.toString()).asText());
    assertEquals(
        SnowflakeDriver.getImplementationVersion(),
        msg.get(TelemetryField.DRIVER_VERSION.toString()).asText());
    assertEquals("true", msg.get(ConnectionIdentifierShape.ACCOUNT_PROVIDED_KEY).asText());
    assertEquals("true", msg.get(ConnectionIdentifierShape.ACCOUNT_WITH_REGION_KEY).asText());
    assertEquals("true", msg.get(ConnectionIdentifierShape.ACCOUNT_ORG_PROVIDED_KEY).asText());
    // Region structurally false on JDBC; still emitted for cross-driver join consistency.
    assertEquals("false", msg.get(ConnectionIdentifierShape.REGION_PROVIDED_KEY).asText());
    // TOML-path shape: account-only, no host key — host_provided=false.
    assertEquals("false", msg.get(ConnectionIdentifierShape.HOST_PROVIDED_KEY).asText());
  }

  @Test
  public void emit_skipsWhenShapeIsNull() {
    TelemetryClient client = mock(TelemetryClient.class);

    ConnectionIdentifierShapeTelemetry.emit(client, /* shape= */ null, null);

    verify(client, never()).addLogToBatch(any(ObjectNode.class), anyLong());
  }

  @Test
  public void emit_skipsWhenClientIsNull() {
    // Smoke test: passing a null client must not NPE.
    ConnectionIdentifierShapeTelemetry.emit(null, sampleShape(), null);
  }

  @Test
  public void emit_skipsWhenEnvVarIsLiteralTrue() {
    TelemetryClient client = mock(TelemetryClient.class);

    ConnectionIdentifierShapeTelemetry.emit(client, sampleShape(), "true");

    verify(client, never()).addLogToBatch(any(ObjectNode.class), anyLong());
  }

  @ParameterizedTest
  @ValueSource(strings = {"true", "True", "TRUE", "TrUe"})
  public void emit_envVarKillSwitchIsCaseInsensitive(String value) {
    // Strict equality after lower-case folding — no whitespace tolerance — mirroring
    // gosnowflake's strings.EqualFold(..., "true") and the byte-identical Python / Node.js
    // checks. Any drift here breaks the cross-driver promise that an operator setting the
    // kill switch sees the same behavior on every driver.
    TelemetryClient client = mock(TelemetryClient.class);
    ConnectionIdentifierShapeTelemetry.emit(client, sampleShape(), value);
    verify(client, never()).addLogToBatch(any(ObjectNode.class), anyLong());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        "0",
        "1",
        "yes",
        "Yes",
        "false",
        "no",
        "anything-else",
        " true ",
        "\ttrue\n",
        "  true  ",
        "true ",
        " true"
      })
  public void emit_envVarNonTrueValuesLeaveEmissionEnabled(String value) {
    // Match the Go/Python/Node.js reference: only literal case-insensitive "true" disables.
    // Former truthy aliases like "1" / "yes" / "false" / empty all leave emission enabled,
    // and — critically — so do whitespace-wrapped values like " true ", "\ttrue\n", "true ".
    // The siblings explicitly call out the no-trim semantics; JDBC must agree.
    TelemetryClient client = mock(TelemetryClient.class);
    ConnectionIdentifierShapeTelemetry.emit(client, sampleShape(), value);
    verify(client, times(1)).addLogToBatch(any(ObjectNode.class), anyLong());
  }

  @Test
  public void disabledByEnv_only_recognizes_case_insensitive_true() {
    assertTrue(ConnectionIdentifierShapeTelemetry.disabledByEnv("true"));
    assertTrue(ConnectionIdentifierShapeTelemetry.disabledByEnv("True"));
    assertTrue(ConnectionIdentifierShapeTelemetry.disabledByEnv("TRUE"));
    // No whitespace tolerance — see comment on disabledByEnv: the cross-driver contract is
    // "an operator accidentally exporting ` true ` (with stray whitespace) leaves emission
    // enabled". Locking this in here so a future regression to .trim() flips a red test.
    assertFalse(ConnectionIdentifierShapeTelemetry.disabledByEnv("  true  "));
    assertFalse(ConnectionIdentifierShapeTelemetry.disabledByEnv(" true "));
    assertFalse(ConnectionIdentifierShapeTelemetry.disabledByEnv("\ttrue\n"));
    assertFalse(ConnectionIdentifierShapeTelemetry.disabledByEnv("true "));
    assertFalse(ConnectionIdentifierShapeTelemetry.disabledByEnv(" true"));
    assertFalse(ConnectionIdentifierShapeTelemetry.disabledByEnv(null));
    assertFalse(ConnectionIdentifierShapeTelemetry.disabledByEnv(""));
    assertFalse(ConnectionIdentifierShapeTelemetry.disabledByEnv("1"));
    assertFalse(ConnectionIdentifierShapeTelemetry.disabledByEnv("yes"));
    assertFalse(ConnectionIdentifierShapeTelemetry.disabledByEnv("false"));
    assertFalse(ConnectionIdentifierShapeTelemetry.disabledByEnv("trueish"));
  }

  @Test
  public void toTelemetryNode_carriesAllFiveWireKeys() {
    // Defensive: even an all-false shape must emit every key with the expected value.
    ObjectNode node =
        ConnectionIdentifierShapeTelemetry.toTelemetryNode(ConnectionIdentifierShape.empty());

    assertNotNull(node.get(ConnectionIdentifierShape.ACCOUNT_PROVIDED_KEY));
    assertNotNull(node.get(ConnectionIdentifierShape.ACCOUNT_WITH_REGION_KEY));
    assertNotNull(node.get(ConnectionIdentifierShape.ACCOUNT_ORG_PROVIDED_KEY));
    assertNotNull(node.get(ConnectionIdentifierShape.REGION_PROVIDED_KEY));
    assertNotNull(node.get(ConnectionIdentifierShape.HOST_PROVIDED_KEY));
    assertEquals("false", node.get(ConnectionIdentifierShape.ACCOUNT_PROVIDED_KEY).asText());
    assertEquals("false", node.get(ConnectionIdentifierShape.HOST_PROVIDED_KEY).asText());
  }

  @Test
  public void toTelemetryNode_serializesAsQuotedStringBooleans() throws JsonProcessingException {
    // Cross-driver wire format: the five shape flags ship as JSON strings ("true" / "false"),
    // not JSON booleans (true / false). This deliberately matches gosnowflake (PR #1797),
    // snowflake-connector-python (PR #2877), and snowflake-connector-nodejs (PR #1411) — the
    // five-key contract is byte-identical so downstream joins can avoid driver-specific
    // type-coercion. Locking the literal serialization here so a refactor to
    // ObjectNode.put(String, boolean) would flip a red test (it would otherwise compile and
    // produce semantically-similar-but-wire-incompatible output).
    ConnectionIdentifierShape shape =
        new ConnectionIdentifierShape(
            /* accountProvided= */ true,
            /* accountWithRegion= */ false,
            /* accountOrgProvided= */ true,
            /* regionProvided= */ false,
            /* hostProvided= */ true);

    ObjectNode node = ConnectionIdentifierShapeTelemetry.toTelemetryNode(shape);
    String json = new ObjectMapper().writeValueAsString(node);

    assertTrue(
        json.contains("\"account_provided\":\"true\""),
        "account_provided must be a JSON string, not a JSON boolean — got: " + json);
    assertTrue(
        json.contains("\"account_with_region\":\"false\""),
        "account_with_region must be a JSON string — got: " + json);
    assertTrue(
        json.contains("\"account_org_provided\":\"true\""),
        "account_org_provided must be a JSON string — got: " + json);
    assertTrue(
        json.contains("\"region_provided\":\"false\""),
        "region_provided must be a JSON string — got: " + json);
    assertTrue(
        json.contains("\"host_provided\":\"true\""),
        "host_provided must be a JSON string — got: " + json);
    // Negative assertion: catch the most likely accidental regression (Jackson rendering
    // the values as bare booleans). We can't anchor on the comma boundary in either
    // direction without coupling to key ordering, so a substring sweep is the cleanest
    // canary.
    assertFalse(
        json.contains(":true,") || json.contains(":true}") || json.contains(":false,"),
        "no shape key may serialize as a bare JSON boolean — got: " + json);
  }
}
