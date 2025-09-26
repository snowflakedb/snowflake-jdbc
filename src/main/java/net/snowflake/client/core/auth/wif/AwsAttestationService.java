package net.snowflake.client.core.auth.wif;

import com.amazonaws.Request;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.InstanceMetadataRegionProvider;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.EnvironmentVariables;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
// import software.amazon.awssdk.auth.AwsCredentials;
// import software.amazon.awssdk.auth.DefaultCredentialsProvider;

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

  AWSCredentials getAWSCredentials() {
    return DefaultAWSCredentialsProviderChain.getInstance().getCredentials();
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

  void signRequestWithSigV4(Request<Void> signableRequest, AWSCredentials awsCredentials) {
    aws4Signer.setServiceName(signableRequest.getServiceName());
    aws4Signer.sign(signableRequest, awsCredentials);
  }
}
