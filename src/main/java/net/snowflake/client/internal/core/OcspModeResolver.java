package net.snowflake.client.internal.core;

import static net.snowflake.client.internal.jdbc.SnowflakeUtil.systemGetEnv;
import static net.snowflake.client.internal.jdbc.SnowflakeUtil.systemGetProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Resolves {@link OCSPMode} from connection properties and reports settings that will be ignored
 * when OCSP is off. Package-private for unit tests.
 *
 * <p>Default is {@link OCSPMode#DISABLE_OCSP_CHECKS}. OCSP is enabled only when {@code
 * ocspFailOpen} is set (connection property or {@code -Dnet.snowflake.jdbc.ocspFailOpen} copied
 * onto the session). {@code disableOCSPChecks=false} and {@code insecureMode=false} are not
 * opt-ins. {@code disableOCSPChecks=true} and the deprecated {@code insecureMode=true} always turn
 * OCSP off, including when {@code ocspFailOpen} is set.
 */
final class OcspModeResolver {

  static final String INSECURE_MODE_DEPRECATED =
      "The 'insecureMode' connection property is deprecated. Please use 'disableOCSPChecks' "
          + "instead.";

  static final String FAIL_OPEN_IGNORED_WHEN_DISABLE_SET =
      "disableOCSPChecks=true is set; ocspFailOpen will be ignored.";

  static final String FAIL_OPEN_IGNORED_WHEN_INSECURE_SET =
      "insecureMode=true is set; ocspFailOpen will be ignored.";

  interface Env {
    String getProperty(String key);

    String getEnv(String key);
  }

  static final Env SYSTEM =
      new Env() {
        @Override
        public String getProperty(String key) {
          return systemGetProperty(key);
        }

        @Override
        public String getEnv(String key) {
          return systemGetEnv(key);
        }
      };

  static final class Result {
    final OCSPMode mode;
    final List<String> warnings;

    Result(OCSPMode mode, List<String> warnings) {
      this.mode = mode;
      this.warnings = Collections.unmodifiableList(warnings);
    }
  }

  private OcspModeResolver() {}

  static Result resolve(Boolean disableOCSPChecks, Boolean ocspFailOpen, Boolean insecureMode) {
    return resolve(disableOCSPChecks, ocspFailOpen, insecureMode, SYSTEM);
  }

  static Result resolve(
      Boolean disableOCSPChecks, Boolean ocspFailOpen, Boolean insecureMode, Env env) {
    List<String> warnings = new ArrayList<>();
    if (insecureMode != null) {
      warnings.add(INSECURE_MODE_DEPRECATED);
    }

    boolean insecureModeEnabled = Boolean.TRUE.equals(insecureMode);
    boolean explicitlyDisabled = Boolean.TRUE.equals(disableOCSPChecks);
    boolean failOpenSet = ocspFailOpen != null;

    OCSPMode mode;
    if (explicitlyDisabled) {
      mode = OCSPMode.DISABLE_OCSP_CHECKS;
    } else if (insecureModeEnabled) {
      mode = OCSPMode.DISABLE_OCSP_CHECKS;
    } else if (failOpenSet) {
      mode = ocspFailOpen ? OCSPMode.FAIL_OPEN : OCSPMode.FAIL_CLOSED;
    } else {
      mode = OCSPMode.DISABLE_OCSP_CHECKS;
    }

    if (failOpenSet && explicitlyDisabled) {
      warnings.add(FAIL_OPEN_IGNORED_WHEN_DISABLE_SET);
    }
    if (failOpenSet && insecureModeEnabled && !explicitlyDisabled) {
      warnings.add(FAIL_OPEN_IGNORED_WHEN_INSECURE_SET);
    }

    if (mode == OCSPMode.DISABLE_OCSP_CHECKS) {
      List<String> ignored = ignoredKnobs(env);
      if (!ignored.isEmpty()) {
        warnings.add(
            "OCSP is disabled; the following setting(s) will be ignored: "
                + String.join(", ", ignored));
      }
    }
    return new Result(mode, warnings);
  }

  private static List<String> ignoredKnobs(Env env) {
    List<String> ignored = new ArrayList<>();
    addIfSet(
        ignored, env.getProperty(SFTrustManager.CACHE_DIR_PROP), SFTrustManager.CACHE_DIR_PROP);
    addIfSet(ignored, env.getEnv(SFTrustManager.CACHE_DIR_ENV), SFTrustManager.CACHE_DIR_ENV);
    addIfSet(
        ignored,
        env.getProperty(SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_URL),
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_URL);
    addIfSet(
        ignored,
        env.getEnv(SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_URL),
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_URL);
    addIfSet(
        ignored,
        env.getProperty(SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED),
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED);
    addIfSet(
        ignored,
        env.getEnv(SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED),
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED);
    addIfSet(
        ignored,
        env.getProperty(SFTrustManager.SF_OCSP_ACTIVATE_NEW_ENDPOINT_JVM),
        SFTrustManager.SF_OCSP_ACTIVATE_NEW_ENDPOINT_JVM);
    addIfSet(
        ignored,
        env.getEnv(SFTrustManager.SF_OCSP_ACTIVATE_NEW_ENDPOINT),
        SFTrustManager.SF_OCSP_ACTIVATE_NEW_ENDPOINT);
    return ignored;
  }

  private static void addIfSet(List<String> dest, String value, String name) {
    if (value != null && !value.isEmpty() && !dest.contains(name)) {
      dest.add(name);
    }
  }
}
