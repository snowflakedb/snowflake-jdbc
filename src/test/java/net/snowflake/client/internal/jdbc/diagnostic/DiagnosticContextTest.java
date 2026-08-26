package net.snowflake.client.internal.jdbc.diagnostic;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import net.snowflake.client.internal.core.SFSessionProperty;
import org.junit.jupiter.api.Test;

public class DiagnosticContextTest {

  /**
   * SFSessionProperty.checkPropertyValue coerces this property to a Boolean on the real connection
   * path, but the map can also be assembled directly, so both forms must be understood - and an
   * absent property must not enable the opt-out.
   */
  @Test
  public void shouldReadAllowUnderscoresInHostFromConnectionProperties() {
    Map<SFSessionProperty, Object> properties = new HashMap<>();
    assertFalse(DiagnosticContext.getAllowUnderscoresInHost(properties));

    properties.put(SFSessionProperty.ALLOW_UNDERSCORES_IN_HOST, Boolean.TRUE);
    assertTrue(DiagnosticContext.getAllowUnderscoresInHost(properties));

    properties.put(SFSessionProperty.ALLOW_UNDERSCORES_IN_HOST, Boolean.FALSE);
    assertFalse(DiagnosticContext.getAllowUnderscoresInHost(properties));

    properties.put(SFSessionProperty.ALLOW_UNDERSCORES_IN_HOST, "true");
    assertTrue(DiagnosticContext.getAllowUnderscoresInHost(properties));

    properties.put(SFSessionProperty.ALLOW_UNDERSCORES_IN_HOST, "false");
    assertFalse(DiagnosticContext.getAllowUnderscoresInHost(properties));

    properties.put(SFSessionProperty.ALLOW_UNDERSCORES_IN_HOST, null);
    assertFalse(DiagnosticContext.getAllowUnderscoresInHost(properties));
  }
}
