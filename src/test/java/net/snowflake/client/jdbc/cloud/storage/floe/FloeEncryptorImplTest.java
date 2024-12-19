package net.snowflake.client.jdbc.cloud.storage.floe;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;

class FloeEncryptorImplTest {
  @Test
  void shouldCreateCorrectHeader() {
    FloeParameterSpec parameterSpec =
        new FloeParameterSpec(
            Aead.AES_GCM_256,
            Hash.SHA384,
            12345678,
            new FloeIvLength(4),
            new FixedFloeRandom(new byte[] {11, 22, 33, 44}));
    FloeKey floeKey = new FloeKey(new SecretKeySpec(new byte[32], "FLOE"));
    FloeAad floeAad = new FloeAad("test aad".getBytes(StandardCharsets.UTF_8));
    FloeEncryptorImpl floeEncryptor = new FloeEncryptorImpl(parameterSpec, floeKey, floeAad);
    byte[] header = floeEncryptor.getHeader();
    // AEAD ID
    assertEquals(Aead.AES_GCM_256.getId(), header[0]);
    // HASH ID
    assertEquals(Hash.SHA384.getId(), header[1]);
    // Segment length in BE
    // 12345678(10) = BC614E(16)
    assertEquals(0, header[2]);
    assertEquals((byte) 188, header[3]);
    assertEquals((byte) 97, header[4]);
    assertEquals((byte) 78, header[5]);
    // FLOE IV length in BE
    // 4(10) = 4(16) = 00,00,00,04
    assertEquals(0, header[6]);
    assertEquals(0, header[7]);
    assertEquals(0, header[8]);
    assertEquals(4, header[9]);
    // FLOE IV
    assertEquals(11, header[10]);
    assertEquals(22, header[11]);
    assertEquals(33, header[12]);
    assertEquals(44, header[13]);
  }
}
