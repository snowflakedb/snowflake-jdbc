package net.snowflake.client.util;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;

/** Enum representing all detectable cloud platforms and identity providers. */
@SnowflakeJdbcInternalApi
public enum Platform {
  IS_AWS_LAMBDA("is_aws_lambda"),
  IS_AZURE_FUNCTION("is_azure_function"),
  IS_GCE_CLOUD_RUN_SERVICE("is_gce_cloud_run_service"),
  IS_GCE_CLOUD_RUN_JOB("is_gce_cloud_run_job"),
  IS_GITHUB_ACTION("is_github_action"),
  IS_EC2_INSTANCE("is_ec2_instance"),
  HAS_AWS_IDENTITY("has_aws_identity"),
  IS_AZURE_VM("is_azure_vm"),
  HAS_AZURE_MANAGED_IDENTITY("has_azure_managed_identity"),
  IS_GCE_VM("is_gce_vm"),
  HAS_GCP_IDENTITY("has_gcp_identity");

  private final String value;

  Platform(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }

  @Override
  public String toString() {
    return value;
  }
}
