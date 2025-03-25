package net.snowflake.client.core.auth.wif;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.InstanceMetadataRegionProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityResult;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeUtil;

class AwsAttestationProvider implements WorkflowIdentityAttestationProvider {

    @Override
    public WorkflowIdentityAttestation createAttestation() throws SFException {
        AWSCredentials awsCredentials = DefaultAWSCredentialsProviderChain.getInstance().getCredentials();
        if (awsCredentials == null) {
            throw new SFException(ErrorCode.WORKFLOW_IDENTITY_FLOW_ERROR, "No AWS credentials were found.");
        }
        String region = getAwsRegion();
        if (region == null) {
            throw new SFException(ErrorCode.WORKFLOW_IDENTITY_FLOW_ERROR, "No AWS region was found.");
        }
        String arn = getCallerIdentity();
        if (arn == null) {
            throw new SFException(ErrorCode.WORKFLOW_IDENTITY_FLOW_ERROR, "No Caller Identity was found.");
        }

        return null;
    }

    private String getAwsRegion() {
        String region = SnowflakeUtil.systemGetEnv("AWS_REGION");
        if (region != null) {
            return region;
        } else {
            return new InstanceMetadataRegionProvider().getRegion();
        }
    }

    private String getCallerIdentity() {
        GetCallerIdentityResult callerIdentity = AWSSecurityTokenServiceClientBuilder.defaultClient().getCallerIdentity(new GetCallerIdentityRequest());
        if (callerIdentity != null) {
            return callerIdentity.getArn();
        } else {
            return null;
        }
    }
}
