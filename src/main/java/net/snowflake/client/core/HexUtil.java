package net.snowflake.client.core;

class HexUtil {

  /**
   * Converts Byte array to hex string
   *
   * @param bytes a byte array
   * @return a string in hexadecimal code
   */
  static String byteToHexString(byte[] bytes) {
    final char[] hexArray = "0123456789ABCDEF".toCharArray();
    char[] hexChars = new char[bytes.length * 2];
    for (int j = 0; j < bytes.length; j++) {
      int v = bytes[j] & 0xFF;
      hexChars[j * 2] = hexArray[v >>> 4];
      hexChars[j * 2 + 1] = hexArray[v & 0x0F];
    }
    return new String(hexChars);
  }
}
