package net.snowflake.client.core.auth.wif;

import com.amazonaws.auth.AWSCredentials;
import java.io.IOException;
import net.snowflake.client.core.HttpClientSettingsKey;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.http.client.methods.HttpRequestBase;

@SnowflakeJdbcInternalApi
public class PlatformDetectionUtil {

  private static final SFLogger logger = SFLoggerFactory.getLogger(PlatformDetectionUtil.class);

  public static String performPlatformDetectionRequest(HttpRequestBase httpRequest, int timeoutMs)
      throws SnowflakeSQLException, IOException {
    return HttpUtil.executeGeneralRequestOmitRequestGuid(
        httpRequest,
        1,
        timeoutMs / 1000,
        timeoutMs,
        0,
        new HttpClientSettingsKey(OCSPMode.DISABLE_OCSP_CHECKS),
        null);
  }

  public static boolean hasValidAwsIdentityForWif(AwsAttestationService attestationService) {
    if (!hasValidAwsCredentials(attestationService)) {
      logger.debug("No valid AWS credentials available for identity validation");
      return false;
    }
    return true;
  }

  public static boolean hasValidAwsCredentials(AwsAttestationService attestationService) {
    try {
      AWSCredentials credentials = attestationService.getAWSCredentials();
      return hasValidAwsCredentials(credentials);
    } catch (Exception e) {
      logger.debug("Failed to get AWS credentials from default provider chain: {}", e.getMessage());
      return false;
    }
  }

  public static boolean hasValidAwsCredentials(AWSCredentials awsCredentials) {
    try {
      if (awsCredentials == null) {
        logger.debug("No AWS credentials found");
        return false;
      }

      // Check if we have access key and secret key
      String accessKey = awsCredentials.getAWSAccessKeyId();
      String secretKey = awsCredentials.getAWSSecretKey();

      if (accessKey == null
          || accessKey.trim().isEmpty()
          || secretKey == null
          || secretKey.trim().isEmpty()) {
        logger.debug("AWS credentials are incomplete");
        return false;
      }

      logger.debug("Valid AWS credentials found");
      return true;

    } catch (Exception e) {
      logger.debug("AWS credential check failed: {}", e.getMessage());
      return false;
    }
  }
}
