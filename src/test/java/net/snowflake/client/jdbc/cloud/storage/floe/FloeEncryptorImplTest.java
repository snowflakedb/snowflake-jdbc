package net.snowflake.client.jdbc.cloud.storage.floe;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;

class FloeEncryptorImplTest {
  byte[] aad = "This is AAD".getBytes(StandardCharsets.UTF_8);
  SecretKey secretKey = new SecretKeySpec(new byte[32], "FLOE");

  @Test
  void shouldCreateCorrectHeader() {
    FloeParameterSpec parameterSpec =
        new FloeParameterSpec(
            Aead.AES_GCM_256,
            Hash.SHA384,
            12345678,
            new FloeIvLength(4),
            new IncrementingFloeRandom(17),
            4);
    parameterSpec.getFloeRandom().ofLength(4); // just to trigger incrementation
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
    assertEquals(0, header[10]);
    assertEquals(0, header[11]);
    assertEquals(0, header[12]);
    assertEquals(18, header[13]);
  }

  @Test
  void testEncryptionMatchesReference() {
    List<String> referenceCiphertextSegments =
        Arrays.asList(
            "ffffffff0000000100000000000000000100007f5713b9827bb806318311fcde197146a144c6b485", // pragma: allowlist secret
            "ffffffff000000020000000000000000f926dfc0a0bac6263d1634ad9a72f86900872033a271a037", // pragma: allowlist secret
            "ffffffff00000003000000000000000080df8fdee872febe574c2b8df0bb34b3fb25bfc5802703a2", // pragma: allowlist secret
            "ffffffff000000040000000000000000f4d81083e57451dbfa538827942245019b8bc3354ecc31e0", // pragma: allowlist secret
            "ffffffff000000050000000000000000d91b774b5b460bd665910114e155f1cbc55a9a262a54f65e", // pragma: allowlist secret
            "ffffffff000000060000000000000000ec723f3807eb71ea42ff03f5420daf34e1a8f4fb58931db1", // pragma: allowlist secret
            "ffffffff00000007000000000000000072960c06ec19ce94c27c9fc72d79164f187f37e86325d849", // pragma: allowlist secret
            "ffffffff000000080000000000000000c00a40fb140d797da818ab57399cb986bddddd174b8d3d6a", // pragma: allowlist secret
            "ffffffff000000090000000000000000065e959cd1ffa521896fb54949a57ad1c1f8291a531c6d60", // pragma: allowlist secret
            "ffffffff0000000a0000000000000000dfde3da3f67a081fb31229ac11e43a629ed120fbf9942513" // pragma: allowlist secret
            );
    FloeParameterSpec parameterSpec =
        new FloeParameterSpec(
            Aead.AES_GCM_256,
            Hash.SHA384,
            40,
            new FloeIvLength(32),
            new IncrementingFloeRandom(0),
            4);
    Floe floe = Floe.getInstance(parameterSpec);
    FloeEncryptor encryptor = floe.createEncryptor(secretKey, aad);
    byte[] header = encryptor.getHeader();
    byte[] testData = new byte[8];
    for (int i = 0; i < referenceCiphertextSegments.size(); i++) {
      byte[] ciphertextBytes = encryptor.processSegment(testData);
      String ciphertextHex = toHex(ciphertextBytes);
      assertEquals(referenceCiphertextSegments.get(i), ciphertextHex);
    }
  }

  private String toHex(byte[] input) {
    StringBuilder result = new StringBuilder();
    for (byte b : input) {
      result.append(String.format("%02x", b));
    }
    return result.toString();
  }
}
