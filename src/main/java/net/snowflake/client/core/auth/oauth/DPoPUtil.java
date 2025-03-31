package net.snowflake.client.core.auth.oauth;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.gen.ECKeyGenerator;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.oauth2.sdk.dpop.DPoPProofFactory;
import com.nimbusds.oauth2.sdk.dpop.DefaultDPoPProofFactory;
import com.nimbusds.oauth2.sdk.dpop.JWKThumbprintConfirmation;
import com.nimbusds.openid.connect.sdk.Nonce;
import java.net.URI;
import java.net.URISyntaxException;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.ErrorCode;
import org.apache.http.client.methods.HttpRequestBase;

@SnowflakeJdbcInternalApi
public class DPoPUtil {

  private final ECKey jwk;

  DPoPUtil() throws SFException {
    try {
      jwk = new ECKeyGenerator(Curve.P_256).generate();
    } catch (JOSEException e) {
      throw new SFException(
          ErrorCode.INTERNAL_ERROR, "Error during DPoP JWK initialization: " + e.getMessage());
    }
  }

  public DPoPUtil(String jsonKey) throws SFException {
    try {
      jwk = ECKey.parse(jsonKey);
    } catch (Exception e) {
      throw new SFException(
          ErrorCode.INTERNAL_ERROR, "Error during DPoP JWK initialization: " + e.getMessage());
    }
  }

  String getPublicKey() {
    return jwk.toJSONString();
  }

  JWKThumbprintConfirmation getThumbprint() throws SFException {
    try {
      return JWKThumbprintConfirmation.of(this.jwk);
    } catch (JOSEException e) {
      throw new SFException(
          ErrorCode.INTERNAL_ERROR, "Error during JWK thumbprint generation: " + e.getMessage());
    }
  }

  public void addDPoPProofHeaderToRequest(HttpRequestBase httpRequest, String nonce)
      throws SFException {
    SignedJWT signedJWT = generateDPoPProof(httpRequest, nonce);
    httpRequest.setHeader("DPoP", signedJWT.serialize());
  }

  private SignedJWT generateDPoPProof(HttpRequestBase httpRequest, String nonce)
      throws SFException {
    try {
      DPoPProofFactory proofFactory = new DefaultDPoPProofFactory(jwk, JWSAlgorithm.ES256);
      if (nonce != null) {
        return proofFactory.createDPoPJWT(
            httpRequest.getMethod(), httpRequest.getURI(), new Nonce(nonce));
      } else {
        return proofFactory.createDPoPJWT(
            httpRequest.getMethod(), getUriWithoutQuery(httpRequest.getURI()));
      }
    } catch (Exception e) {
      throw new SFException(
          ErrorCode.INTERNAL_ERROR, " Error during DPoP proof generation: " + e.getMessage());
    }
  }

  /**
   * Method needed for sake of DPoP proof JWT creation. URI claim (htu) does not support query
   * parameters.
   */
  private URI getUriWithoutQuery(URI uri) throws URISyntaxException {
    return new URI(uri.getScheme(), uri.getAuthority(), uri.getPath(), null, null);
  }
}
