/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core;

class SSDPubKey {
  private static final String pem_key_dep1 = null;
  private static final String pem_key_dep2 = null;

  static String getPublicKeyInternal(String dep) {
    if (dep.equals("dep1")) {
      return pem_key_dep1;
    } else {
      return pem_key_dep2;
    }
  }
}
