package net.snowflake.client.jdbc.cloud.storage.floe;

import javax.crypto.SecretKey;

class FloeKey {
  private final SecretKey key;

  FloeKey(SecretKey key) {
    this.key = key;
  }

  SecretKey getKey() {
    return key;
  }
}
