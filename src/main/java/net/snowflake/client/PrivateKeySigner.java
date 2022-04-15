package net.snowflake.client;

import java.security.PublicKey;

/**
 * Interface for customer signer implementations for key pair authentication.
 */
public interface PrivateKeySigner {
    /**
     * Returns a signature for the given input.
     *
     * The signature must be compatible with the "RS256" JWT signing algorithm,
     * a.k.a. "RSASSA-PKCS1-v1_5 using SHA-256"
     */
    byte[] sign(byte[] input);

    /**
     * Returns the public key associated with the private key used by the sign() method.
     */
    PublicKey publicKey();
}
