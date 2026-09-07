package net.snowflake.client.internal.jdbc.cloud.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Random;
import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.KeyGenerator;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;

public class FullReadInputStreamTest {

  /** Source that yields at most {@code chunk} bytes per read, like a CipherInputStream. */
  private static final class SmallChunkInputStream extends FilterInputStream {
    private final int chunk;

    SmallChunkInputStream(InputStream in, int chunk) {
      super(in);
      this.chunk = chunk;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return in.read(b, off, Math.min(len, chunk));
    }
  }

  private static byte[] randomBytes(int size, long seed) {
    byte[] data = new byte[size];
    new Random(seed).nextBytes(data);
    return data;
  }

  @Test
  public void fillsBufferAcrossManySmallReads() throws IOException {
    byte[] data = randomBytes(8192, 42);
    byte[] buf = new byte[data.length];
    try (FullReadInputStream in =
        new FullReadInputStream(new SmallChunkInputStream(new ByteArrayInputStream(data), 512))) {
      assertEquals(data.length, in.read(buf, 0, buf.length));
    }
    assertArrayEquals(data, buf);
  }

  /**
   * Mirrors the customer/SDK trace: a 16 KiB demand ({@code cap=16384}) against a source that
   * yields 512 bytes per read. A single {@code read} into the SDK's buffer must come back full.
   */
  @Test
  public void fillsSixteenKiBDemandFromFiveHundredTwelveByteSource() throws IOException {
    byte[] data = randomBytes(16384, 99);
    byte[] buf = new byte[16384];
    try (FullReadInputStream in =
        new FullReadInputStream(new SmallChunkInputStream(new ByteArrayInputStream(data), 512))) {
      assertEquals(buf.length, in.read(buf, 0, buf.length));
    }
    assertArrayEquals(data, buf);
  }

  /** The real source in production: an AES CipherInputStream, read once into a 16 KiB buffer. */
  @Test
  public void fillsSixteenKiBDemandFromCipherInputStream() throws Exception {
    byte[] plaintext = randomBytes(16384, 7);

    KeyGenerator keyGen = KeyGenerator.getInstance("AES");
    keyGen.init(128);
    SecretKeySpec key = new SecretKeySpec(keyGen.generateKey().getEncoded(), "AES");
    byte[] iv = new byte[16];
    new SecureRandom().nextBytes(iv);

    Cipher encrypt = Cipher.getInstance("AES/CBC/PKCS5Padding");
    encrypt.init(Cipher.ENCRYPT_MODE, key, new IvParameterSpec(iv));
    byte[] ciphertext = encrypt.doFinal(plaintext);

    Cipher decrypt = Cipher.getInstance("AES/CBC/PKCS5Padding");
    decrypt.init(Cipher.DECRYPT_MODE, key, new IvParameterSpec(iv));

    byte[] buf = new byte[plaintext.length];
    try (FullReadInputStream in =
        new FullReadInputStream(
            new CipherInputStream(new ByteArrayInputStream(ciphertext), decrypt))) {
      // CipherInputStream returns one block at a time; one full read must reassemble the whole
      // plaintext rather than a single ~512-byte block.
      assertEquals(plaintext.length, in.read(buf, 0, buf.length));
    }
    assertArrayEquals(plaintext, buf);
  }

  @Test
  public void returnsAvailableBytesThenEof() throws IOException {
    byte[] data = randomBytes(1000, 7);
    byte[] buf = new byte[4096];
    try (FullReadInputStream in =
        new FullReadInputStream(new SmallChunkInputStream(new ByteArrayInputStream(data), 128))) {
      assertEquals(data.length, in.read(buf, 0, buf.length));
      assertArrayEquals(data, Arrays.copyOf(buf, data.length));
      assertEquals(-1, in.read(buf, 0, buf.length));
    }
  }

  @Test
  public void reportsEofOnEmptyStream() throws IOException {
    try (FullReadInputStream in = new FullReadInputStream(new ByteArrayInputStream(new byte[0]))) {
      assertEquals(-1, in.read(new byte[16], 0, 16));
    }
  }

  /**
   * A source that never makes progress on a positive request (illegal for a blocking stream) must
   * fail loudly rather than spin forever or hand a short body to the publisher.
   */
  @Test
  @org.junit.jupiter.api.Timeout(5)
  public void throwsWhenSourceNeverMakesProgress() throws IOException {
    InputStream noProgress =
        new InputStream() {
          @Override
          public int read() {
            return 0;
          }

          @Override
          public int read(byte[] b, int off, int len) {
            return 0;
          }
        };
    try (FullReadInputStream in = new FullReadInputStream(noProgress)) {
      assertThrows(IOException.class, () -> in.read(new byte[16], 0, 16));
    }
  }
}
