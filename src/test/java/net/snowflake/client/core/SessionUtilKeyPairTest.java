package net.snowflake.client.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.SecureRandom;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class SessionUtilKeyPairTest {

  @ParameterizedTest
  @CsvSource({
    "name.azure.deployment,NAME",
    "account.region-cloud,ACCOUNT",
    "multi.part.long.name,MULTI",
    "single,SINGLE"
  })
  public void testAccountNameWithDotsInJwtToken(String testName, String expected)
      throws SFException, Exception {
    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
    keyPairGenerator.initialize(2048, new SecureRandom());
    KeyPair keyPair = keyPairGenerator.generateKeyPair();
    PrivateKey privateKey = keyPair.getPrivate();

    String userName = "testuser";

    SessionUtilKeyPair sessionUtil =
        new SessionUtilKeyPair(privateKey, null, null, null, testName, userName);
    String jwtToken = sessionUtil.issueJwtToken();

    assertNotNull(jwtToken);

    SignedJWT signedJWT = SignedJWT.parse(jwtToken);
    JWTClaimsSet claims = signedJWT.getJWTClaimsSet();
    String expectedSubject = expected + "." + userName.toUpperCase();

    assertEquals(expectedSubject, claims.getSubject());

    String issuer = claims.getIssuer();

    assertTrue(issuer.startsWith(expectedSubject));

    if (testName.contains(".")) {
      assertFalse(issuer.contains(testName.toUpperCase()));
    }
  }
}
