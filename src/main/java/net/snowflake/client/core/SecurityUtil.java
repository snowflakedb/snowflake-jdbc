package net.snowflake.client.core;

import java.lang.reflect.InvocationTargetException;
import java.security.Provider;
import java.security.Security;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

@SnowflakeJdbcInternalApi
public class SecurityUtil {

  private static final SFLogger LOGGER = SFLoggerFactory.getLogger(SecurityUtil.class);

  /** provider name for FIPS */
  public static final String BOUNCY_CASTLE_FIPS_PROVIDER = "BCFIPS";

  public static final String BOUNCY_CASTLE_PROVIDER = "BC";
  private static final String DEFAULT_SECURITY_PROVIDER_NAME =
      "org.bouncycastle.jce.provider.BouncyCastleProvider";

  public static final String ENABLE_BOUNCYCASTLE_PROVIDER_JVM =
      "net.snowflake.jdbc.enableBouncyCastle";

  public static void addBouncyCastleProvider() {
    // Add Bouncy Castle to the list of security providers. This is required to
    // verify the signature on OCSP response and attached certificates.
    // It is also required to decrypt password protected private keys.
    // Check to see if the BouncyCastleFipsProvider has already been added.
    // If so, then we don't want to add the provider BouncyCastleProvider.
    // The addProvider() method won't add the provider if it already exists.
    if (Security.getProvider(BOUNCY_CASTLE_FIPS_PROVIDER) == null) {
      Security.addProvider(instantiateSecurityProvider());
    }
  }

  private static Provider instantiateSecurityProvider() {

    try {
      Class klass = Class.forName(DEFAULT_SECURITY_PROVIDER_NAME);
      return (Provider) klass.getDeclaredConstructor().newInstance();
    } catch (ExceptionInInitializerError
        | ClassNotFoundException
        | NoSuchMethodException
        | InstantiationException
        | IllegalAccessException
        | IllegalArgumentException
        | InvocationTargetException
        | SecurityException ex) {
      String errMsg =
          String.format(
              "Failed to load %s, err=%s. If you use Snowflake JDBC for FIPS jar, "
                  + "import BouncyCastleFipsProvider in the application.",
              DEFAULT_SECURITY_PROVIDER_NAME, ex.getMessage());
      LOGGER.error(errMsg, true);
      throw new RuntimeException(errMsg, ex);
    }
  }
}
