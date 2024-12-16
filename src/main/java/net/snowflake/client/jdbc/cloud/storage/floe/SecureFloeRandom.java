package net.snowflake.client.jdbc.cloud.storage.floe;

import java.security.SecureRandom;

class SecureFloeRandom implements FloeRandom {
  private static final ThreadLocal<SecureRandom> random =
      ThreadLocal.withInitial(SecureRandom::new);

  @Override
  public byte[] ofLength(int length) {
    byte[] bytes = new byte[length];
    random.get().nextBytes(bytes);
    return bytes;
  }
}
