package net.snowflake.client.core;

import static net.snowflake.client.jdbc.SnowflakeUtil.isNullOrEmpty;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetEnv;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.PublicKey;
import java.security.Security;
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
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.bouncycastle.pkcs.PKCSException;
import org.bouncycastle.util.io.pem.PemReader;

/** Class used to compute jwt token for key pair authentication. */
class SessionUtilKeyPair {

  private static final SFLogger logger = SFLoggerFactory.getLogger(SessionUtilKeyPair.class);

  // user name in upper case
  private final String userName;

  // account name in upper case
  private final String accountName;

  private final PrivateKey privateKey;

  private PublicKey publicKey = null;

  private boolean isFipsMode = false;

  private Provider SecurityProvider = null;

  private static final String ISSUER_FMT = "%s.%s.%s";

  private static final String SUBJECT_FMT = "%s.%s";

  private static final int JWT_DEFAULT_AUTH_TIMEOUT = 10;

  private boolean isBouncyCastleProviderEnabled = false;

  SessionUtilKeyPair(
      PrivateKey privateKey,
      String privateKeyFile,
      String privateKeyBase64,
      String privateKeyPwd,
      String accountName,
      String userName)
      throws SFException {
    this.userName = userName.toUpperCase();
    this.accountName = accountName.toUpperCase();
    String enableBouncyCastleJvm =
        System.getProperty(SecurityUtil.ENABLE_BOUNCYCASTLE_PROVIDER_JVM);
    if (enableBouncyCastleJvm != null) {
      isBouncyCastleProviderEnabled = enableBouncyCastleJvm.equalsIgnoreCase("true");
    }
    // check if in FIPS mode
    for (Provider p : Security.getProviders()) {
      if (SecurityUtil.BOUNCY_CASTLE_FIPS_PROVIDER.equals(p.getName())) {
        this.isFipsMode = true;
        this.SecurityProvider = p;
        break;
      }
    }

    ensurePrivateKeyProvidedInOnlyOneProperty(privateKey, privateKeyFile, privateKeyBase64);
    this.privateKey = buildPrivateKey(privateKey, privateKeyFile, privateKeyBase64, privateKeyPwd);

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

  private static void ensurePrivateKeyProvidedInOnlyOneProperty(
      PrivateKey privateKey, String privateKeyFile, String privateKeyBase64) throws SFException {
    if (!isNullOrEmpty(privateKeyFile) && privateKey != null) {
      throw new SFException(
          ErrorCode.INVALID_OR_UNSUPPORTED_PRIVATE_KEY,
          "Cannot have both private key object and private key file.");
    }
    if (!isNullOrEmpty(privateKeyBase64) && !isNullOrEmpty(privateKeyFile)) {
      throw new SFException(
          ErrorCode.INVALID_OR_UNSUPPORTED_PRIVATE_KEY,
          "Cannot have both private key file and private key base64 string value.");
    }
    if (!isNullOrEmpty(privateKeyBase64) && privateKey != null) {
      throw new SFException(
          ErrorCode.INVALID_OR_UNSUPPORTED_PRIVATE_KEY,
          "Cannot have both private key object and private key base64 string value.");
    }
  }

  private PrivateKey buildPrivateKey(
      PrivateKey privateKey, String privateKeyFile, String privateKeyBase64, String privateKeyPwd)
      throws SFException {
    if (!isNullOrEmpty(privateKeyBase64)) {
      logger.trace("Reading private key from base64 string");
      return extractPrivateKeyFromBase64(privateKeyBase64, privateKeyPwd);
    }
    if (!isNullOrEmpty(privateKeyFile)) {
      logger.trace("Reading private key from file");
      return extractPrivateKeyFromFile(privateKeyFile, privateKeyPwd);
    }
    return privateKey;
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

  private PrivateKey extractPrivateKeyFromFile(String privateKeyFile, String privateKeyPwd)
      throws SFException {

    try {
      Path privKeyPath = Paths.get(privateKeyFile);
      FileUtil.logFileUsage(privKeyPath, "Extract private key from file", true);
      byte[] bytes = Files.readAllBytes(privKeyPath);
      return extractPrivateKeyFromBytes(bytes, privateKeyPwd);
    } catch (IOException ie) {
      logger.error("Could not read private key from file", ie);
      throw new SFException(ie, ErrorCode.INVALID_PARAMETER_VALUE, ie.getCause());
    }
  }

  private PrivateKey extractPrivateKeyFromBytes(byte[] privateKeyBytes, String privateKeyPwd)
      throws SFException {
    if (isBouncyCastleProviderEnabled) {
      try {
        return extractPrivateKeyWithBouncyCastle(privateKeyBytes, privateKeyPwd);
      } catch (IOException | PKCSException | OperatorCreationException e) {
        logger.error("Could not extract private key using Bouncy Castle provider", e);
        throw new SFException(e, ErrorCode.INVALID_OR_UNSUPPORTED_PRIVATE_KEY, e.getCause());
      }
    } else {
      try {
        return extractPrivateKeyWithJdk(privateKeyBytes, privateKeyPwd);
      } catch (NoSuchAlgorithmException
          | InvalidKeySpecException
          | IOException
          | IllegalArgumentException
          | NullPointerException
          | InvalidKeyException e) {
        logger.error(
            "Could not extract private key using standard JDK. Try setting the JVM argument: "
                + "-D{}"
                + "=TRUE",
            SecurityUtil.ENABLE_BOUNCYCASTLE_PROVIDER_JVM);
        throw new SFException(e, ErrorCode.INVALID_OR_UNSUPPORTED_PRIVATE_KEY, e.getMessage());
      }
    }
  }

  private PrivateKey extractPrivateKeyFromBase64(String privateKeyBase64, String privateKeyPwd)
      throws SFException {
    byte[] decodedKey = Base64.decodeBase64(privateKeyBase64);
    return extractPrivateKeyFromBytes(decodedKey, privateKeyPwd);
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

  public static int getTimeout() {
    String jwtAuthTimeoutStr = systemGetEnv("JWT_AUTH_TIMEOUT");
    int jwtAuthTimeout = JWT_DEFAULT_AUTH_TIMEOUT;
    if (jwtAuthTimeoutStr != null) {
      jwtAuthTimeout = Integer.parseInt(jwtAuthTimeoutStr);
    }
    return jwtAuthTimeout;
  }

  private PrivateKey extractPrivateKeyWithBouncyCastle(byte[] privateKeyBytes, String privateKeyPwd)
      throws IOException, PKCSException, OperatorCreationException {
    logger.trace("Extracting private key using Bouncy Castle provider");
    PrivateKeyInfo privateKeyInfo = null;
    PEMParser pemParser =
        new PEMParser(new StringReader(new String(privateKeyBytes, StandardCharsets.UTF_8)));
    Object pemObject = pemParser.readObject();
    if (pemObject instanceof PKCS8EncryptedPrivateKeyInfo) {
      // Handle the case where the private key is encrypted.
      PKCS8EncryptedPrivateKeyInfo encryptedPrivateKeyInfo =
          (PKCS8EncryptedPrivateKeyInfo) pemObject;
      InputDecryptorProvider pkcs8Prov =
          new JceOpenSSLPKCS8DecryptorProviderBuilder().build(privateKeyPwd.toCharArray());
      privateKeyInfo = encryptedPrivateKeyInfo.decryptPrivateKeyInfo(pkcs8Prov);
    } else if (pemObject instanceof PEMKeyPair) {
      // PKCS#1 private key
      privateKeyInfo = ((PEMKeyPair) pemObject).getPrivateKeyInfo();
    } else if (pemObject instanceof PrivateKeyInfo) {
      // Handle the case where the private key is unencrypted.
      privateKeyInfo = (PrivateKeyInfo) pemObject;
    }
    pemParser.close();
    JcaPEMKeyConverter converter =
        new JcaPEMKeyConverter()
            .setProvider(
                isFipsMode
                    ? SecurityUtil.BOUNCY_CASTLE_FIPS_PROVIDER
                    : SecurityUtil.BOUNCY_CASTLE_PROVIDER);
    return converter.getPrivateKey(privateKeyInfo);
  }

  private PrivateKey extractPrivateKeyWithJdk(byte[] privateKeyFileBytes, String privateKeyPwd)
      throws IOException, NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException {
    logger.trace("Extracting private key using JDK");
    String privateKeyContent = new String(privateKeyFileBytes, StandardCharsets.UTF_8);
    if (isNullOrEmpty(privateKeyPwd)) {
      // unencrypted private key file
      return generatePrivateKey(false, privateKeyContent, privateKeyPwd);
    } else {
      // encrypted private key file
      return generatePrivateKey(true, privateKeyContent, privateKeyPwd);
    }
  }

  private PrivateKey generatePrivateKey(
      boolean isEncrypted, String privateKeyContent, String privateKeyPwd)
      throws IOException, NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException {
    if (isEncrypted) {
      try (PemReader pr = new PemReader(new StringReader(privateKeyContent))) {
        byte[] decoded = pr.readPemObject().getContent();
        pr.close();
        EncryptedPrivateKeyInfo pkInfo = new EncryptedPrivateKeyInfo(decoded);
        PBEKeySpec keySpec = new PBEKeySpec(privateKeyPwd.toCharArray());
        SecretKeyFactory pbeKeyFactory = this.getSecretKeyFactory(pkInfo.getAlgName());
        PKCS8EncodedKeySpec encodedKeySpec =
            pkInfo.getKeySpec(pbeKeyFactory.generateSecret(keySpec));
        KeyFactory keyFactory = getKeyFactoryInstance();
        return keyFactory.generatePrivate(encodedKeySpec);
      }
    } else {
      try (PemReader pr = new PemReader(new StringReader(privateKeyContent))) {
        byte[] decoded = pr.readPemObject().getContent();
        pr.close();
        PKCS8EncodedKeySpec encodedKeySpec = new PKCS8EncodedKeySpec(decoded);
        KeyFactory keyFactory = getKeyFactoryInstance();
        return keyFactory.generatePrivate(encodedKeySpec);
      }
    }
  }
}
