/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core;

import com.google.common.base.Strings;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.*;
import java.security.interfaces.RSAPrivateCrtKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.RSAPublicKeySpec;
import java.util.Date;
import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.commons.codec.binary.Base64;
import org.bouncycastle.util.io.pem.PemReader;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetEnv;

/** Class used to compute jwt token for key pair authentication Created by hyu on 1/16/18. */
class SessionUtilKeyPair {

  static final SFLogger logger = SFLoggerFactory.getLogger(SessionUtilKeyPair.class);

  // user name in upper case
  private final String userName;

  // account name in upper case
  private final String accountName;

  private final PrivateKey privateKey;

  private PublicKey publicKey = null;

  private boolean isFipsMode = false;

  private Provider SecurityProvider = null;

  private SecretKeyFactory secretKeyFactory = null;

  private static final String ISSUER_FMT = "%s.%s.%s";

  private static final String SUBJECT_FMT = "%s.%s";

  private static final int JWT_DEFAULT_AUTH_TIMEOUT = 10;

  SessionUtilKeyPair(
      PrivateKey privateKey,
      String privateKeyFile,
      String privateKeyFilePwd,
      String accountName,
      String userName)
      throws SFException {
    this.userName = userName.toUpperCase();
    this.accountName = accountName.toUpperCase();

    // check if in FIPS mode
    for (Provider p : Security.getProviders()) {
      if ("BCFIPS".equals(p.getName())) {
        this.isFipsMode = true;
        this.SecurityProvider = p;
        break;
      }
    }

    // if there is both a file and a private key, there is a problem
    if (!Strings.isNullOrEmpty(privateKeyFile) && privateKey != null) {
      throw new SFException(
          ErrorCode.INVALID_OR_UNSUPPORTED_PRIVATE_KEY,
          "Cannot have both private key value and private key file.");
    } else {
      // if privateKeyFile has a value and privateKey is null
      this.privateKey =
          Strings.isNullOrEmpty(privateKeyFile)
              ? privateKey
              : extractPrivateKeyFromFile(privateKeyFile, privateKeyFilePwd);
    }
    // construct public key from raw bytes
    if (this.privateKey instanceof RSAPrivateCrtKey) {
      RSAPrivateCrtKey rsaPrivateCrtKey = (RSAPrivateCrtKey) this.privateKey;
      RSAPublicKeySpec rsaPublicKeySpec =
          new RSAPublicKeySpec(rsaPrivateCrtKey.getModulus(), rsaPrivateCrtKey.getPublicExponent());

      try {
        this.publicKey = getKeyFactoryInstance().generatePublic(rsaPublicKeySpec);
      } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
        throw new SFException(e, ErrorCode.INTERNAL_ERROR, "Error retrieving public key");
      }
    } else {
      throw new SFException(
          ErrorCode.INVALID_OR_UNSUPPORTED_PRIVATE_KEY,
          "Use java.security.interfaces.RSAPrivateCrtKey.class for the private key");
    }
  }



  private KeyFactory getKeyFactoryInstance() throws NoSuchAlgorithmException {
    if (isFipsMode) {
      return KeyFactory.getInstance("RSA", this.SecurityProvider);
    } else {
      return KeyFactory.getInstance("RSA");
    }
  }

  private SecretKeyFactory getSecretKeyFactory(String algorithm) throws NoSuchAlgorithmException {
    if (isFipsMode) {
      return SecretKeyFactory.getInstance(algorithm, this.SecurityProvider);
    } else {
      return SecretKeyFactory.getInstance(algorithm);
    }
  }

  private PrivateKey extractPrivateKeyFromFile(String privateKeyFile, String privateKeyFilePwd)
      throws SFException {
    try {
      String privateKeyContent = new String(Files.readAllBytes(Paths.get(privateKeyFile)));
      if (Strings.isNullOrEmpty(privateKeyFilePwd)) {
        // unencrypted private key file
        PemReader pr = new PemReader(new StringReader(privateKeyContent));
        byte[] decoded = pr.readPemObject().getContent();
        pr.close();
        PKCS8EncodedKeySpec encodedKeySpec = new PKCS8EncodedKeySpec(decoded);
        KeyFactory keyFactory = getKeyFactoryInstance();
        return keyFactory.generatePrivate(encodedKeySpec);
      } else {
        // encrypted private key file
        PemReader pr = new PemReader(new StringReader(privateKeyContent));
        byte[] decoded = pr.readPemObject().getContent();
        pr.close();
        EncryptedPrivateKeyInfo pkInfo = new EncryptedPrivateKeyInfo(decoded);
        PBEKeySpec keySpec = new PBEKeySpec(privateKeyFilePwd.toCharArray());
        SecretKeyFactory pbeKeyFactory = this.getSecretKeyFactory(pkInfo.getAlgName());
        PKCS8EncodedKeySpec encodedKeySpec =
            pkInfo.getKeySpec(pbeKeyFactory.generateSecret(keySpec));
        KeyFactory keyFactory = getKeyFactoryInstance();
        return keyFactory.generatePrivate(encodedKeySpec);
      }
    } catch (NoSuchAlgorithmException
        | InvalidKeySpecException
        | IOException
        | IllegalArgumentException
        | NullPointerException
        | InvalidKeyException e) {
      throw new SFException(
          e, ErrorCode.INVALID_OR_UNSUPPORTED_PRIVATE_KEY, privateKeyFile + ": " + e.getMessage());
    }
  }

  public String issueJwtToken() throws SFException {
    JWTClaimsSet.Builder builder = new JWTClaimsSet.Builder();
    String sub = String.format(SUBJECT_FMT, this.accountName, this.userName);
    String iss =
        String.format(
            ISSUER_FMT,
            this.accountName,
            this.userName,
            this.calculatePublicKeyFingerprint(this.publicKey));

    // iat is now
    Date iat = new Date(System.currentTimeMillis());

    // expiration is 60 seconds later
    Date exp = new Date(iat.getTime() + 60L * 1000);

    JWTClaimsSet claimsSet =
        builder.issuer(iss).subject(sub).issueTime(iat).expirationTime(exp).build();

    SignedJWT signedJWT = new SignedJWT(new JWSHeader(JWSAlgorithm.RS256), claimsSet);
    JWSSigner signer = new RSASSASigner(this.privateKey);

    try {
      signedJWT.sign(signer);
    } catch (JOSEException e) {
      throw new SFException(e, ErrorCode.FAILED_TO_GENERATE_JWT);
    }
    // Log the contents of the token, displaying expiration and issue time in epoch time
    logger.debug(
        "JWT:\n'{'\niss: {}\nsub: {}\niat: {}\nexp: {}\n'}'",
        iss,
        sub,
        String.valueOf(iat.getTime() / 1000),
        String.valueOf(exp.getTime() / 1000));
    return signedJWT.serialize();
  }

  private String calculatePublicKeyFingerprint(PublicKey publicKey) throws SFException {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      byte[] sha256Hash = md.digest(publicKey.getEncoded());
      return "SHA256:" + Base64.encodeBase64String(sha256Hash);
    } catch (NoSuchAlgorithmException e) {
      throw new SFException(e, ErrorCode.INTERNAL_ERROR, "Error when calculating fingerprint");
    }
  }
  public static int getTimeout()  {
    String jwtAuthTimeoutStr = systemGetEnv("JWT_AUTH_TIMEOUT");
    int jwtAuthTimeout = JWT_DEFAULT_AUTH_TIMEOUT;
    if(jwtAuthTimeoutStr != null) {
      jwtAuthTimeout = Integer.parseInt(jwtAuthTimeoutStr);
    }
    return jwtAuthTimeout;
  }
}
