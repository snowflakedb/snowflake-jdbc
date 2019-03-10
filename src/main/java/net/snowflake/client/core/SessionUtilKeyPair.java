/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import net.snowflake.client.jdbc.ErrorCode;
import org.apache.commons.codec.binary.Base64;

import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.interfaces.RSAPrivateCrtKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.RSAPublicKeySpec;
import java.util.Date;

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

  final private PublicKey publicKey;

  static private final String ISSUER_FMT = "%s.%s.%s";

  static private final String SUBJECT_FMT = "%s.%s";

  SessionUtilKeyPair(PrivateKey privateKey,
                     String accountName,
                     String userName) throws SFException
  {
    this.userName = userName.toUpperCase();
    this.accountName = accountName.toUpperCase();
    this.privateKey = privateKey;

    // construct public key from raw bytes
    if (privateKey instanceof RSAPrivateCrtKey)
    {
      RSAPrivateCrtKey rsaPrivateCrtKey = (RSAPrivateCrtKey) privateKey;
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
                            "Please use java.security.interfaces.RSAPrivateCrtKey.class");
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
