package net.snowflake.client.core.auth.wif;

import com.amazonaws.Request;
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

  private static boolean regionInitialized = false;
  private static String region;

  private final AWS4Signer aws4Signer;

  public AwsAttestationService() {
    aws4Signer = new AWS4Signer();
  }

  void initializeSignerRegion() {
    aws4Signer.setRegionName(getAWSRegion());
  }

  public AWSCredentials getAWSCredentials() {
    try {
      return DefaultAWSCredentialsProviderChain.getInstance().getCredentials();
    } catch (Exception e) {
      logger.debug("Failed to retrieve AWS credentials: {}", e.getMessage());
      return null;
    }
  }

  String getAWSRegion() {
    if (!regionInitialized) {
      logger.debug("Getting AWS region from environment variable");
      String envRegion = SnowflakeUtil.systemGetEnv(EnvironmentVariables.AWS_REGION.getName());
      if (envRegion != null) {
        region = envRegion;
      } else {
        logger.debug("Getting AWS region from EC2 metadata service");
        region = new InstanceMetadataRegionProvider().getRegion();
      }
      regionInitialized = true;
    }
    return region;
  }

  public String getArn() {
    GetCallerIdentityResult callerIdentity =
        AWSSecurityTokenServiceClientBuilder.defaultClient()
            .getCallerIdentity(new GetCallerIdentityRequest());
    return Optional.ofNullable(callerIdentity).map(GetCallerIdentityResult::getArn).orElse(null);
  }

  void signRequestWithSigV4(Request<Void> signableRequest, AWSCredentials awsCredentials) {
    aws4Signer.setServiceName(signableRequest.getServiceName());
    aws4Signer.sign(signableRequest, awsCredentials);
  }
}
