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
      String envRegion = SnowflakeUtil.systemGetEnv(EnvironmentVariables.AWS_REGION.getName());
      region = envRegion != null ? envRegion : new InstanceMetadataRegionProvider().getRegion();
      regionInitialized = true;
    }
    return region;
  }

  void signRequestWithSigV4(Request<Void> signableRequest, AWSCredentials awsCredentials) {
    aws4Signer.setServiceName(signableRequest.getServiceName());
    aws4Signer.sign(signableRequest, awsCredentials);
  }
}
