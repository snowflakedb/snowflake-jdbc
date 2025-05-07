package net.snowflake.client.core.auth.wif;

import com.amazonaws.SignableRequest;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.InstanceMetadataRegionProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityResult;
import java.util.Optional;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.EnvironmentVariables;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

@SnowflakeJdbcInternalApi
public class AwsAttestationService {

  public static final SFLogger logger = SFLoggerFactory.getLogger(AwsAttestationService.class);

  private static final String SECURE_TOKEN_SERVICE_NAME = "sts";
  private static boolean regionInitialized = false;
  private static String region;

  private final AWS4Signer aws4Signer;

  public AwsAttestationService() {
    aws4Signer = new AWS4Signer();
    aws4Signer.setServiceName(SECURE_TOKEN_SERVICE_NAME);
  }

  AWSCredentials getAWSCredentials() {
    return DefaultAWSCredentialsProviderChain.getInstance().getCredentials();
  }

  String getAWSRegion() {
    try {
      if (!regionInitialized) {
        String envRegion = SnowflakeUtil.systemGetEnv(EnvironmentVariables.AWS_REGION.getName());
        region = envRegion != null ? envRegion : new InstanceMetadataRegionProvider().getRegion();
      }
      return region;
    } catch (Exception e) {
      logger.debug("Could not get AWS region", e);
      return null;
    } finally {
      regionInitialized = true;
    }
  }

  String getArn() {
    GetCallerIdentityResult callerIdentity =
        AWSSecurityTokenServiceClientBuilder.defaultClient()
            .getCallerIdentity(new GetCallerIdentityRequest());
    return Optional.ofNullable(callerIdentity).map(GetCallerIdentityResult::getArn).orElse(null);
  }

  void signRequestWithSigV4(SignableRequest<Void> signableRequest, AWSCredentials awsCredentials) {
    aws4Signer.sign(signableRequest, awsCredentials);
  }
}
