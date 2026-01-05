package net.snowflake.client.core.auth.wif;

import com.amazonaws.auth.AWSCredentials;
import java.io.IOException;
import java.util.regex.Pattern;
import net.snowflake.client.core.HttpClientSettingsKey;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpRequestBase;

@SnowflakeJdbcInternalApi
public class PlatformDetectionUtil {

  private static final SFLogger logger = SFLoggerFactory.getLogger(PlatformDetectionUtil.class);

  // Regex patterns for validating AWS ARN for WIF
  private static final Pattern IAM_USER_ARN_PATTERN =
      Pattern.compile("^arn:[^:]+:iam::[^:]+:user/.+$");
  private static final Pattern ASSUMED_ROLE_ARN_PATTERN =
      Pattern.compile("^arn:[^:]+:sts::[^:]+:assumed-role/.+$");

  public static String performPlatformDetectionRequest(HttpRequestBase httpRequest, int timeoutMs)
      throws SnowflakeSQLException, IOException {
    httpRequest.setConfig(
        RequestConfig.custom()
            .setConnectTimeout(timeoutMs)
            .setSocketTimeout(timeoutMs)
            .setConnectionRequestTimeout(timeoutMs)
            .build());
    return HttpUtil.executeGeneralRequestOmitSnowflakeHeaders(
        httpRequest,
        1,
        timeoutMs / 1000,
        timeoutMs,
        0,
        new HttpClientSettingsKey(OCSPMode.DISABLE_OCSP_CHECKS),
        null);
  }

  public static boolean hasValidAwsIdentityForWif(
      AwsAttestationService attestationService, int timeoutMs) {
    try {
      AWSCredentials credentials = attestationService.getAWSCredentials();
      if (!hasValidAwsCredentials(credentials)) {
        logger.debug("No valid AWS credentials available for identity validation");
        return false;
      }
      String arn = attestationService.getCallerIdentityArn(credentials, timeoutMs);
      if (arn == null) {
        logger.debug("Failed to retrieve caller identity ARN");
        return false;
      }

      boolean isValid = isValidArnForWif(arn);
      if (isValid) {
        logger.debug("Valid AWS identity found with ARN: {}", arn);
      } else {
        logger.debug("ARN is not valid for WIF: {}", arn);
      }
      return isValid;
    } catch (Exception e) {
      logger.debug("Failed to validate AWS identity: {}", e.getMessage());
      return false;
    }
  }

  public static boolean isValidArnForWif(String arn) {
    if (arn == null || arn.trim().isEmpty()) {
      return false;
    }
    return IAM_USER_ARN_PATTERN.matcher(arn).matches()
        || ASSUMED_ROLE_ARN_PATTERN.matcher(arn).matches();
  }

  private static boolean hasValidAwsCredentials(AWSCredentials awsCredentials) {
    if (awsCredentials == null) {
      logger.debug("No AWS credentials found");
      return false;
    }

    String accessKey = awsCredentials.getAWSAccessKeyId();
    String secretKey = awsCredentials.getAWSSecretKey();

    if (SnowflakeUtil.isNullOrEmpty(accessKey) || SnowflakeUtil.isNullOrEmpty(secretKey)) {
      logger.debug("AWS credentials are incomplete");
      return false;
    }

    return true;
  }
}
