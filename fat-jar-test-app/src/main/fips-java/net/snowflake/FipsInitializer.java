package net.snowflake;

import java.security.Provider;
import java.security.Security;
import java.util.Arrays;
import java.util.List;
import org.bouncycastle.crypto.CryptoServicesRegistrar;
import org.bouncycastle.crypto.fips.FipsStatus;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;

/**
 * FIPS initialization class that registers BouncyCastle FIPS provider
 * before the main application runs. This class is only compiled when
 * the 'fips' Maven profile is active.
 */
public class FipsInitializer {

    private static final String JCE_PROVIDER_BOUNCY_CASTLE_FIPS = "BCFIPS";
    private static final String JCE_PROVIDER_SUN_JCE = "SunJCE";
    private static final String JCE_PROVIDER_SUN_RSA_SIGN = "SunRsaSign";
    private static final String JCE_KEYSTORE_BOUNCY_CASTLE = "BCFKS";
    private static final String JCE_KEYSTORE_JKS = "JKS";
    private static final String BOUNCY_CASTLE_RNG_HYBRID_MODE = "C:HYBRID;ENABLE{All};";
    private static final String SSL_ENABLED_PROTOCOLS = "TLSv1.2,TLSv1.1,TLSv1";
    private static final String JAVA_SYSTEM_PROPERTY_SSL_NAMEDGROUPS = "jdk.tls.namedGroups";
    private static final String JAVA_SYSTEM_PROPERTY_SSL_KEYSTORE_TYPE = "javax.net.ssl.keyStoreType";
    private static final String JAVA_SYSTEM_PROPERTY_SSL_TRUSTSTORE_TYPE = "javax.net.ssl.trustStoreType";
    private static final String JAVA_SYSTEM_PROPERTY_SSL_PROTOCOLS = "jdk.tls.client.protocols";

    // Static initializer block executes when class is loaded
    static {
        try {
            initializeFipsMode();
            System.out.println("[FIPS] Initialization completed successfully");
        } catch (Exception e) {
            System.err.println("[FIPS] Initialization failed: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Failed to initialize FIPS mode", e);
        }
    }

    private static void initializeFipsMode() {
        // Set named groups to avoid test failure on GCP environment
        System.setProperty(JAVA_SYSTEM_PROPERTY_SSL_NAMEDGROUPS,
            "secp256r1, secp384r1, ffdhe2048, ffdhe3072");

        // Set keystore types for BouncyCastle libraries
        System.setProperty(JAVA_SYSTEM_PROPERTY_SSL_KEYSTORE_TYPE, JCE_KEYSTORE_BOUNCY_CASTLE);
        System.setProperty(JAVA_SYSTEM_PROPERTY_SSL_TRUSTSTORE_TYPE, JCE_KEYSTORE_JKS);

        // Set SSL protocols
        System.setProperty(JAVA_SYSTEM_PROPERTY_SSL_PROTOCOLS, SSL_ENABLED_PROTOCOLS);

        // Remove Java's standard encryption and SSL providers
        List<Provider> providers = Arrays.asList(Security.getProviders());
        Provider sunJceProvider = Security.getProvider(JCE_PROVIDER_SUN_JCE);
        Provider sunRsaSignProvider = Security.getProvider(JCE_PROVIDER_SUN_RSA_SIGN);

        if (sunJceProvider != null) {
            Security.removeProvider(JCE_PROVIDER_SUN_JCE);
        }
        if (sunRsaSignProvider != null) {
            Security.removeProvider(JCE_PROVIDER_SUN_RSA_SIGN);
        }

        /*
         * Insert BouncyCastle's FIPS-compliant encryption and SSL providers.
         */
        BouncyCastleFipsProvider bcFipsProvider =
            new BouncyCastleFipsProvider(BOUNCY_CASTLE_RNG_HYBRID_MODE);

        /*
         * We remove BCFIPS provider pessimistically. This is a no-op if provider
         * does not exist. This is necessary to always add it to the first
         * position when calling insertProviderAt.
         *
         * JavaDoc for insertProviderAt states:
         *   "A provider cannot be added if it is already installed."
         */
        Security.removeProvider(JCE_PROVIDER_BOUNCY_CASTLE_FIPS);
        Security.insertProviderAt(bcFipsProvider, 1);

        // Enable approved-only mode
        if (!CryptoServicesRegistrar.isInApprovedOnlyMode()) {
            if (FipsStatus.isReady()) {
                CryptoServicesRegistrar.setApprovedOnlyMode(true);
            } else {
                throw new RuntimeException(
                    "FIPS is not ready to be enabled and FIPS mode is required");
            }
        }
    }

    /**
     * Force class loading - this method will be called from FatJarTestApp.
     * The method body is empty; just loading the class is enough to trigger
     * the static initializer block.
     */
    public static void ensureInitialized() {
        // Method body is empty; just loading the class is enough
    }
}
