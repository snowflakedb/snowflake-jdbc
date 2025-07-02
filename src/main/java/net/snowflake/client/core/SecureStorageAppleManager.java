package net.snowflake.client.core;

import static net.snowflake.client.jdbc.SnowflakeUtil.isNullOrEmpty;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import java.nio.charset.StandardCharsets;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

public class SecureStorageAppleManager implements SecureStorageManager {
  private static final SFLogger logger = SFLoggerFactory.getLogger(SecureStorageAppleManager.class);

  private final SecurityLib securityLib;

  private SecureStorageAppleManager() {
    securityLib = SecurityLibManager.getInstance();
  }

  public static SecureStorageAppleManager builder() {
    logger.debug("Using Apple Keychain as a token cache storage");
    return new SecureStorageAppleManager();
  }

  public SecureStorageStatus setCredential(String host, String user, String type, String cred) {
    if (isNullOrEmpty(cred)) {
      logger.debug("No credential provided", false);
      return SecureStorageStatus.SUCCESS;
    }

    String target = SecureStorageManager.buildCredentialsKey(host, user, type);
    byte[] targetBytes = target.getBytes(StandardCharsets.UTF_8);
    byte[] userBytes = user.toUpperCase().getBytes(StandardCharsets.UTF_8);
    byte[] credBytes = cred.getBytes(StandardCharsets.UTF_8);

    Pointer[] itemRef = new Pointer[1];
    int errCode = 0;
    synchronized (securityLib) {
      errCode =
          securityLib.SecKeychainFindGenericPassword(
              null,
              targetBytes.length,
              targetBytes,
              userBytes.length,
              userBytes,
              null,
              null,
              itemRef);
    }

    if (errCode != SecurityLib.ERR_SEC_SUCCESS && errCode != SecurityLib.ERR_SEC_ITEM_NOT_FOUND) {
      logger.warn(
          String.format(
              "Failed to check the existence of the item in keychain. Error code = %d",
              Native.getLastError()));
      return SecureStorageStatus.FAILURE;
    }

    if (itemRef[0] != null) {
      synchronized (securityLib) {
        errCode =
            securityLib.SecKeychainItemModifyContent(itemRef[0], null, credBytes.length, credBytes);
      }
    } else {
      synchronized (securityLib) {
        errCode =
            securityLib.SecKeychainAddGenericPassword(
                Pointer.NULL,
                targetBytes.length,
                targetBytes,
                userBytes.length,
                userBytes,
                credBytes.length,
                credBytes,
                null);
      }
    }

    if (errCode != SecurityLib.ERR_SEC_SUCCESS) {
      logger.warn(
          String.format(
              "Failed to set/modify the item in keychain. Error code = %d", Native.getLastError()));
      return SecureStorageStatus.FAILURE;
    }

    logger.debug("Set the item in keychain successfully");
    return SecureStorageStatus.SUCCESS;
  }

  public String getCredential(String host, String user, String type) {
    String target = SecureStorageManager.buildCredentialsKey(host, user, type);
    byte[] targetBytes = target.getBytes(StandardCharsets.UTF_8);
    byte[] userBytes = user.toUpperCase().getBytes(StandardCharsets.UTF_8);

    int[] dataLength = new int[1];
    Pointer[] data = new Pointer[1];

    try {
      int errCode = 0;
      synchronized (securityLib) {
        errCode =
            securityLib.SecKeychainFindGenericPassword(
                null,
                targetBytes.length,
                targetBytes,
                userBytes.length,
                userBytes,
                dataLength,
                data,
                null);
      }

      if (errCode != SecurityLib.ERR_SEC_SUCCESS) {
        logger.warn(
            String.format(
                "Failed to find the item in keychain or item not exists. Error code = %d",
                Native.getLastError()));
        return null;
      }
      if (dataLength[0] == 0 || data[0] == null) {
        logger.warn("Found empty item or no item is found", false);
        return null;
      }

      byte[] credBytes = data[0].getByteArray(0, dataLength[0]);
      String res = new String(credBytes, StandardCharsets.UTF_8);

      logger.debug("Successfully read the credential. Will return it as String now");
      return res;
    } finally {
      if (data[0] != null) {
        synchronized (securityLib) {
          securityLib.SecKeychainItemFreeContent(null, data[0]);
        }
      }
    }
  }

  public SecureStorageStatus deleteCredential(String host, String user, String type) {
    String target = SecureStorageManager.buildCredentialsKey(host, user, type);
    byte[] targetBytes = target.getBytes(StandardCharsets.UTF_8);
    byte[] userBytes = user.toUpperCase().getBytes(StandardCharsets.UTF_8);

    Pointer[] itemRef = new Pointer[1];

    int errCode = 0;
    synchronized (securityLib) {
      errCode =
          securityLib.SecKeychainFindGenericPassword(
              null,
              targetBytes.length,
              targetBytes,
              userBytes.length,
              userBytes,
              null,
              null,
              itemRef);
    }

    if (errCode != SecurityLib.ERR_SEC_SUCCESS && errCode != SecurityLib.ERR_SEC_ITEM_NOT_FOUND) {
      logger.warn(
          String.format(
              "Failed to delete the item in keychain. Error code = %d", Native.getLastError()));
      return SecureStorageStatus.FAILURE;
    }

    if (itemRef[0] != null) {
      synchronized (securityLib) {
        errCode = securityLib.SecKeychainItemDelete(itemRef[0]);
      }

      if (errCode != SecurityLib.ERR_SEC_SUCCESS) {
        logger.warn(
            String.format(
                "Failed to delete the item in keychain. Error code = %d", Native.getLastError()));
        return SecureStorageStatus.FAILURE;
      }
    }

    return SecureStorageStatus.SUCCESS;
  }

  static class SecurityLibManager {
    private static SecurityLib INSTANCE = null;

    private static class ResourceHolder {
      private static final SecurityLib INSTANCE =
          (SecurityLib) Native.loadLibrary("Security", SecurityLib.class);
    }

    public static SecurityLib getInstance() {
      if (INSTANCE == null) {
        INSTANCE = ResourceHolder.INSTANCE;
      }
      return INSTANCE;
    }

    /** This function is used only for unit test */
    public static void setInstance(SecurityLib instance) {
      INSTANCE = instance;
    }

    /** This function is a helper function for testing */
    public static void resetInstance() {
      if (Constants.getOS() == Constants.OS.MAC) {
        INSTANCE = ResourceHolder.INSTANCE;
      }
    }
  }

  /** the java mapping of OS X Security Library */
  interface SecurityLib extends Library {
    // SecurityLib INSTANCE = (SecurityLib) Native.loadLibrary("Security", SecurityLib.class);

    int ERR_SEC_SUCCESS = 0;
    int ERR_SEC_ITEM_NOT_FOUND = -25300;

    /**
     * https://developer.apple.com/documentation/security/1397301-seckeychainfindgenericpassword
     *
     * <p>func SecKeychainFindGenericPassword(_ keychainOrArray: CFTypeRef?, _ serviceNameLength:
     * UInt32, _ serviceName: UnsafePointer<Int8>?, const char* _ accountNameLength: UInt32, _
     * accountName: UnsafePointer<Int8>?, const char* _ passwordLength:
     * UnsafeMutablePointer<UInt32>?, UInt32* _ passwordData:
     * UnsafeMutablePointer<UnsafeMutableRawPointer?>?, void** _ itemRef:
     * UnsafeMutablePointer<SecKeychainItem?>?), SecKeychainItemRef* -> OSStatus
     */
    public int SecKeychainFindGenericPassword(
        Pointer keychainOrArray,
        int serviceNameLength,
        byte[] serviceName,
        int accountNameLength,
        byte[] accountName,
        int[] passwordLength,
        Pointer[] passwordData,
        Pointer[] itemRef);

    /**
     * func SecKeychainAddGenericPassword(_ keychain: SecKeychain?, SecKeychainRef _
     * serviceNameLength: UInt32, _ serviceName: UnsafePointer<Int8>?, const char* _
     * accountNameLength: UInt32, _ accountName: UnsafePointer<Int8>?, const char* _ passwordLength:
     * UInt32, _ passwordData: UnsafeRawPointer, const void* _ itemRef:
     * UnsafeMutablePointer<SecKeychainItem?>?) SecKeychainItemRef* -> OSStatus
     */
    public int SecKeychainAddGenericPassword(
        Pointer keychain,
        int serviceNameLength,
        byte[] serviceName,
        int accountNameLength,
        byte[] accountName,
        int passwordLength,
        byte[] passwordData,
        Pointer[] itemRef);

    /**
     * OSStatus SecKeychainItemModifyContent(SecKeychainItemRef itemRef, const
     * SecKeychainAttributeList *attrList, UInt32 length, const void *data);
     */
    public int SecKeychainItemModifyContent(
        Pointer itemRef, Pointer attrList, int length, byte[] data);

    /** OSStatus SecKeychainItemDelete(SecKeychainItemRef itemRef); */
    public int SecKeychainItemDelete(Pointer itemRef);

    /** OSStatus SecKeychainItemFreeContent(SecKeychainAttributeList *attrList, void *data); */
    public int SecKeychainItemFreeContent(Pointer[] attrList, Pointer data);
  }
}
