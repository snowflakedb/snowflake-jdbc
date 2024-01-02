/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc.cloud.storage;

import net.snowflake.common.core.RemoteStoreFileEncryptionMaterial;

class QueryIdHelper {
  static String queryIdFromEncMatOr(RemoteStoreFileEncryptionMaterial encMat, String queryId) {
    return encMat != null && encMat.getQueryId() != null ? encMat.getQueryId() : queryId;
  }
}
