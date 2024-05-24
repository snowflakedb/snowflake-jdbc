package net.snowflake.client.core;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class UUIDUtils {
  static Boolean newUUIDEnabled = true;

  public static UUID getUUID() {
    if (UUIDUtils.newUUIDEnabled) {
      return UUIDUtils.UUIDImpl();
    } else {
      return UUID.randomUUID();
    }
  }

  private static UUID UUIDImpl() {
    final byte[] randomBytes = new byte[16];
    ThreadLocalRandom.current().nextBytes(randomBytes);
    randomBytes[6] &= 0x0f;
    randomBytes[6] |= 0x40;
    randomBytes[8] &= 0x3f;
    randomBytes[8] |= 0x80;

    long msb = 0;
    long lsb = 0;
    for (int i = 0; i < 8; i++) {
      msb = (msb << 8) | (randomBytes[i] & 0xff);
    }
    for (int i = 8; i < 16; i++) {
      lsb = (lsb << 8) | (randomBytes[i] & 0xff);
    }

    return new UUID(msb, lsb);
  }
}
