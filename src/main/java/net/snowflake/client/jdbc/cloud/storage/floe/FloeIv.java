package net.snowflake.client.jdbc.cloud.storage.floe;

class FloeIv {
  private final byte[] bytes;

  FloeIv(byte[] bytes) {
    this.bytes = bytes;
  }

  static FloeIv generateRandom(FloeRandom floeRandom, FloeIvLength floeIvLength) {
    return new FloeIv(floeRandom.ofLength(floeIvLength.getLength()));
  }

  byte[] getBytes() {
    return bytes;
  }

  int lengthInBytes() {
    return bytes.length;
  }
}
