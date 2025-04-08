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

@SnowflakeJdbcInternalApi
public class AwsAttestationService {

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
    if (!regionInitialized) {
      String envRegion = SnowflakeUtil.systemGetEnv(EnvironmentVariables.AWS_REGION.getName());
      region = envRegion != null ? envRegion : new InstanceMetadataRegionProvider().getRegion();
      regionInitialized = true;
    }
    return region;
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
