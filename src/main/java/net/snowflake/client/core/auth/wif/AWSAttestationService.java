package net.snowflake.client.core.auth.wif;

import com.amazonaws.SignableRequest;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.InstanceMetadataRegionProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityResult;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.SnowflakeUtil;

@SnowflakeJdbcInternalApi
public class AWSAttestationService {

  AWSCredentials getAWSCredentials() {
    return DefaultAWSCredentialsProviderChain.getInstance().getCredentials();
  }

  String getAWSRegion() {
    String region = SnowflakeUtil.systemGetEnv("AWS_REGION");
    if (region != null) {
      return region;
    } else {
      return new InstanceMetadataRegionProvider().getRegion();
    }
  }

  String getArn() {
    GetCallerIdentityResult callerIdentity =
        AWSSecurityTokenServiceClientBuilder.defaultClient()
            .getCallerIdentity(new GetCallerIdentityRequest());
    if (callerIdentity != null) {
      return callerIdentity.getArn();
    } else {
      return null;
    }
  }

  void signRequestWithSigV4(SignableRequest<Void> signableRequest, AWSCredentials awsCredentials) {
    AWS4Signer sigV4Signer = new AWS4Signer();
    sigV4Signer.setServiceName("sts");
    sigV4Signer.sign(signableRequest, awsCredentials);
  }
}
