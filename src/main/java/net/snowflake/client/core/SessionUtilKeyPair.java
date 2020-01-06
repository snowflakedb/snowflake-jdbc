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
import net.snowflake.client.jdbc.ErrorCode;
import org.apache.commons.codec.binary.Base64;

import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.interfaces.RSAPrivateCrtKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.RSAPublicKeySpec;
import java.util.Date;

import static java.util.Base64.getMimeDecoder;

/**
 * Class used to compute jwt token for key pair authentication
 * Created by hyu on 1/16/18.
 */
class SessionUtilKeyPair
{
  // user name in upper case
  final private String userName;

  // account name in upper case
  final private String accountName;

  final private PrivateKey privateKey;

  private PublicKey publicKey = null;

  static private final String ISSUER_FMT = "%s.%s.%s";

  static private final String SUBJECT_FMT = "%s.%s";


  SessionUtilKeyPair(PrivateKey privateKey, String privateKeyFile, String privateKeyFilePwd,
                     String accountName,
                     String userName) throws SFException
  {
    this.userName = userName.toUpperCase();
    this.accountName = accountName.toUpperCase();

    // if there is both a file and a private key, there is a problem
    if (!Strings.isNullOrEmpty(privateKeyFile) && privateKey != null)
    {
      throw new SFException(ErrorCode.INVALID_OR_UNSUPPORTED_PRIVATE_KEY,
                            "Cannot have both private key value and private key file.");
    }
    else
    {
      // if privateKeyFile has a value and privateKey is null
      this.privateKey = Strings.isNullOrEmpty(privateKeyFile) ?
                        privateKey :
                        extractPrivateKeyFromFile(privateKeyFile, privateKeyFilePwd);
    }
    // construct public key from raw bytes
    if (this.privateKey instanceof RSAPrivateCrtKey)
    {
      RSAPrivateCrtKey rsaPrivateCrtKey = (RSAPrivateCrtKey) this.privateKey;
      RSAPublicKeySpec rsaPublicKeySpec = new RSAPublicKeySpec(
          rsaPrivateCrtKey.getModulus(), rsaPrivateCrtKey.getPublicExponent());

      try
      {
        this.publicKey = KeyFactory.getInstance("RSA")
            .generatePublic(rsaPublicKeySpec);
      }
      catch (NoSuchAlgorithmException | InvalidKeySpecException e)
      {
        throw new SFException(e, ErrorCode.INTERNAL_ERROR,
                              "Error retrieving public key");
      }
    }
    else
    {
      throw new SFException(ErrorCode.INVALID_OR_UNSUPPORTED_PRIVATE_KEY,
                            "Use java.security.interfaces.RSAPrivateCrtKey.class for the private key");
    }
  }

  private PrivateKey extractPrivateKeyFromFile(String privateKeyFile, String privateKeyFilePwd) throws SFException
  {
    try
    {
      String privateKeyContent = new String(Files.readAllBytes(Paths.get(privateKeyFile)));
      if (Strings.isNullOrEmpty(privateKeyFilePwd))
      {
        // private key file is unencrypted
        privateKeyContent = privateKeyContent.replace("-----BEGIN RSA PRIVATE KEY-----", "");
        privateKeyContent = privateKeyContent.replace("-----END RSA PRIVATE KEY-----", "");
        byte[] decoded = getMimeDecoder().decode(privateKeyContent);
        PKCS8EncodedKeySpec encodedKeySpec = new PKCS8EncodedKeySpec(decoded);
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        return keyFactory.generatePrivate(encodedKeySpec);
      }
      else
      {
        // private key file is encrypted
        privateKeyContent = privateKeyContent.replace("-----BEGIN ENCRYPTED PRIVATE KEY-----", "");
        privateKeyContent = privateKeyContent.replace("-----END ENCRYPTED PRIVATE KEY-----", "");
        EncryptedPrivateKeyInfo pkInfo = new EncryptedPrivateKeyInfo(getMimeDecoder().decode(privateKeyContent));
        PBEKeySpec keySpec = new PBEKeySpec(privateKeyFilePwd.toCharArray());
        SecretKeyFactory pbeKeyFactory = SecretKeyFactory.getInstance(pkInfo.getAlgName());
        PKCS8EncodedKeySpec encodedKeySpec = pkInfo.getKeySpec(pbeKeyFactory.generateSecret(keySpec));
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        return keyFactory.generatePrivate(encodedKeySpec);
      }
    }
    catch (NoSuchAlgorithmException | InvalidKeySpecException | IOException | IllegalArgumentException | InvalidKeyException e)
    {
      throw new SFException(
          e, ErrorCode.INVALID_OR_UNSUPPORTED_PRIVATE_KEY, privateKeyFile);
    }
  }

  public String issueJwtToken() throws SFException
  {
    JWTClaimsSet.Builder builder = new JWTClaimsSet.Builder();
    String sub = String.format(SUBJECT_FMT, this.accountName, this.userName);
    String iss = String.format(ISSUER_FMT, this.accountName, this.userName,
                               this.calculatePublicKeyFingerprint(this.publicKey));

    // iat is now
    Date iat = new Date(System.currentTimeMillis());

    // expiration is 60 seconds later
    Date exp = new Date(iat.getTime() + 60L * 1000);

    JWTClaimsSet claimsSet = builder.issuer(iss)
        .subject(sub)
        .issueTime(iat)
        .expirationTime(exp)
        .build();

    SignedJWT signedJWT = new SignedJWT(new JWSHeader(JWSAlgorithm.RS256),
                                        claimsSet);
    JWSSigner signer = new RSASSASigner(this.privateKey);

    try
    {
      signedJWT.sign(signer);
    }
    catch (JOSEException e)
    {
      throw new SFException(e, ErrorCode.FAILED_TO_GENERATE_JWT);
    }

    return signedJWT.serialize();
  }

  private String calculatePublicKeyFingerprint(PublicKey publicKey)
  throws SFException
  {
    try
    {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      byte[] sha256Hash = md.digest(publicKey.getEncoded());
      return "SHA256:" + Base64.encodeBase64String(sha256Hash);
    }
    catch (NoSuchAlgorithmException e)
    {
      throw new SFException(e, ErrorCode.INTERNAL_ERROR,
                            "Error when calculating fingerprint");
    }
  }
}
