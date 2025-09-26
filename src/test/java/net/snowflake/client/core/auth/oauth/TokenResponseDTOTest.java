package net.snowflake.client.core.auth.oauth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

public class TokenResponseDTOTest {

  private static final String JSON_SAMPLE =
      "{"
          + "\"access_token\":\"abc123\","
          + "\"refresh_token\":\"refresh123\","
          + "\"token_type\":\"bearer\","
          + "\"scope\":\"read write\","
          + "\"username\":\"testUser\","
          + "\"idp_initiated\":true,"
          + "\"expires_in\":3600,"
          + "\"refresh_token_expires_in\":7200"
          + "}";

  @Test
  public void testConstructorAndGetters() {
    TokenResponseDTO tokenResponseDTO =
        new TokenResponseDTO(
            "abc123", "refresh123", "bearer", "read write", "testUser", true, 3600, 7200);
    // Assert that the constructor has correctly initialized the object
    assertEquals("abc123", tokenResponseDTO.getAccessToken());
    assertEquals("bearer", tokenResponseDTO.getTokenType());
    assertEquals("refresh123", tokenResponseDTO.getRefreshToken());
    assertEquals("read write", tokenResponseDTO.getScope());
    assertEquals("testUser", tokenResponseDTO.getUsername());
    assertTrue(tokenResponseDTO.isIdpInitiated());
    assertEquals(3600, tokenResponseDTO.getExpiresIn());
    assertEquals(7200, tokenResponseDTO.getRefreshTokenExpiresIn());
  }

  @Test
  public void testJsonDeserialization() throws Exception {
    // Create an ObjectMapper to convert JSON to TokenResponseDTO
    ObjectMapper objectMapper = new ObjectMapper();

    // Deserialize the JSON string into a TokenResponseDTO object
    TokenResponseDTO deserializedTokenResponseDTO =
        objectMapper.readValue(JSON_SAMPLE, TokenResponseDTO.class);

    // Assert that the deserialized object matches the expected values
    assertEquals("abc123", deserializedTokenResponseDTO.getAccessToken());
    assertEquals("bearer", deserializedTokenResponseDTO.getTokenType());
    assertEquals("refresh123", deserializedTokenResponseDTO.getRefreshToken());
    assertEquals("read write", deserializedTokenResponseDTO.getScope());
    assertEquals("testUser", deserializedTokenResponseDTO.getUsername());
    assertTrue(deserializedTokenResponseDTO.isIdpInitiated());
    assertEquals(3600, deserializedTokenResponseDTO.getExpiresIn());
    assertEquals(7200, deserializedTokenResponseDTO.getRefreshTokenExpiresIn());
  }
}
