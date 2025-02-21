package net.snowflake.client.jdbc.cloud.storage;

import net.snowflake.common.core.RemoteStoreFileEncryptionMaterial;

class QueryIdHelper {
  static String queryIdFromEncMatOr(RemoteStoreFileEncryptionMaterial encMat, String queryId) {
    return encMat != null && encMat.getQueryId() != null ? encMat.getQueryId() : queryId;
  }
}
