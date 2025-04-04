package net.snowflake.client.core;

import static net.snowflake.client.jdbc.SnowflakeUtil.isNullOrEmpty;

import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import com.sun.jna.WString;
import com.sun.jna.platform.win32.WinBase.FILETIME;
import com.sun.jna.ptr.PointerByReference;
import com.sun.jna.win32.StdCallLibrary;
import com.sun.jna.win32.W32APIOptions;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

public class SecureStorageWindowsManager implements SecureStorageManager {
  private static final SFLogger logger =
      SFLoggerFactory.getLogger(SecureStorageWindowsManager.class);

  private final Advapi32Lib advapi32Lib;

  private SecureStorageWindowsManager() {
    advapi32Lib = Advapi32LibManager.getInstance();
  }

  public static SecureStorageWindowsManager builder() {
    logger.debug("Using Windows Credential Manager as a token cache storage");
    return new SecureStorageWindowsManager();
  }

  public SecureStorageStatus setCredential(String host, String user, String type, String token) {
    if (isNullOrEmpty(token)) {
      logger.warn("No token provided", false);
      return SecureStorageStatus.SUCCESS;
    }

    byte[] credBlob = token.getBytes(StandardCharsets.UTF_16LE);
    Memory credBlobMem = new Memory(credBlob.length);
    credBlobMem.write(0, credBlob, 0, credBlob.length);

    String target = SecureStorageManager.buildCredentialsKey(host, user, type);

    SecureStorageWindowsCredential cred = new SecureStorageWindowsCredential();
    cred.Type = SecureStorageWindowsCredentialType.CRED_TYPE_GENERIC.getType();
    cred.TargetName = new WString(target);
    cred.CredentialBlobSize = (int) credBlobMem.size();
    cred.CredentialBlob = credBlobMem;
    cred.Persist = SecureStorageWindowsCredentialPersistType.CRED_PERSIST_LOCAL_MACHINE.getType();
    cred.UserName = new WString(user.toUpperCase());

    boolean ret = false;
    synchronized (advapi32Lib) {
      ret = advapi32Lib.CredWriteW(cred, 0);
    }

    if (!ret) {
      logger.warn(
          String.format(
              "Failed to write to Windows Credential Manager. Error code = %d",
              Native.getLastError()));
      return SecureStorageStatus.FAILURE;
    }
    logger.debug("Wrote to Windows Credential Manager successfully", false);

    return SecureStorageStatus.SUCCESS;
  }

  public String getCredential(String host, String user, String type) {
    PointerByReference pCredential = new PointerByReference();
    String target = SecureStorageManager.buildCredentialsKey(host, user, type);

    try {
      boolean ret = false;
      synchronized (advapi32Lib) {
        ret =
            advapi32Lib.CredReadW(
                target,
                SecureStorageWindowsCredentialType.CRED_TYPE_GENERIC.getType(),
                0,
                pCredential);
      }

      if (!ret) {
        logger.warn(
            String.format(
                "Failed to read target or could not find it in Windows Credential Manager. Error code = %d",
                Native.getLastError()));
        return null;
      }

      logger.debug("Found the token from Windows Credential Manager and now copying it", false);

      SecureStorageWindowsCredential cred =
          new SecureStorageWindowsCredential(pCredential.getValue());

      if (SecureStorageWindowsCredentialType.typeOf(cred.Type)
          != SecureStorageWindowsCredentialType.CRED_TYPE_GENERIC) {
        logger.warn("Wrong type of credential. Expected: CRED_TYPE_GENERIC", false);
        return null;
      }

      if (cred.CredentialBlobSize == 0) {
        logger.debug("Returned credential is empty", false);
        return null;
      }

      byte[] credBytes = cred.CredentialBlob.getByteArray(0, cred.CredentialBlobSize);
      String res = new String(credBytes, StandardCharsets.UTF_16LE);
      logger.debug("Successfully read the token. Will return it as String now", false);
      return res;
    } finally {
      if (pCredential.getValue() != null) {
        synchronized (advapi32Lib) {
          advapi32Lib.CredFree(pCredential.getValue());
        }
      }
    }
  }

  public SecureStorageStatus deleteCredential(String host, String user, String type) {
    String target = SecureStorageManager.buildCredentialsKey(host, user, type);

    boolean ret = false;
    synchronized (advapi32Lib) {
      ret =
          advapi32Lib.CredDeleteW(
              target, SecureStorageWindowsCredentialType.CRED_TYPE_GENERIC.getType(), 0);
    }

    if (!ret) {
      logger.warn(
          String.format(
              "Failed to delete target in Windows Credential Manager. Error code = %d",
              Native.getLastError()));
      return SecureStorageStatus.FAILURE;
    }

    logger.debug("Deleted target in Windows Credential Manager successfully", false);
    return SecureStorageStatus.SUCCESS;
  }

  public static class SecureStorageWindowsCredential extends Structure {
    /**
     * typedef struct _CREDENTIAL { DWORD Flags; DWORD Type; LPTSTR TargetName; LPTSTR Comment;
     * FILETIME LastWritten; DWORD CredentialBlobSize; LPBYTE CredentialBlob; DWORD Persist; DWORD
     * AttributeCount; PCREDENTIAL_ATTRIBUTE Attributes; LPTSTR TargetAlias; LPTSTR UserName; }
     * CREDENTIAL, *PCREDENTIAL;
     */
    public int Flags;

    public int Type;
    public WString TargetName;
    public WString Comment;
    public FILETIME LastWritten = new FILETIME();
    public int CredentialBlobSize;
    public Pointer CredentialBlob;
    public int Persist;
    public int AttributeCount;
    public Pointer Attributes;
    public WString TargetAlias;
    public WString UserName;

    @Override
    protected List<String> getFieldOrder() {
      return Arrays.asList(
          "Flags",
          "Type",
          "TargetName",
          "Comment",
          "LastWritten",
          "CredentialBlobSize",
          "CredentialBlob",
          "Persist",
          "AttributeCount",
          "Attributes",
          "TargetAlias",
          "UserName");
    }

    public SecureStorageWindowsCredential() {
      super();
    }

    public SecureStorageWindowsCredential(Pointer p) {
      super(p);
      read();
    }
  }

  /** Windows credential types */
  enum SecureStorageWindowsCredentialType {
    CRED_TYPE_GENERIC(1),
    CRED_TYPE_DOMAIN_PASSWORD(2),
    CRED_TYPE_DOMAIN_CERTIFICATE(3),
    CRED_TYPE_DOMAIN_VISIBLE_PASSWORD(4),
    CRED_TYPE_GENERIC_CERTIFICATE(5),
    CRED_TYPE_DOMAIN_EXTENDED(6),
    CRED_TYPE_MAXIMUM(7);

    private int type;
    private static Map<Integer, SecureStorageWindowsCredentialType> map =
        new HashMap<Integer, SecureStorageWindowsCredentialType>();

    SecureStorageWindowsCredentialType(int type) {
      this.type = type;
    }

    static {
      for (SecureStorageWindowsCredentialType credType :
          SecureStorageWindowsCredentialType.values()) {
        map.put(credType.type, credType);
      }
    }

    public static SecureStorageWindowsCredentialType typeOf(int type) {
      return map.get(type);
    }

    public int getType() {
      return type;
    }
  }

  enum SecureStorageWindowsCredentialPersistType {
    CRED_PERSIST_NONE(0),
    CRED_PERSIST_SESSION(1),
    CRED_PERSIST_LOCAL_MACHINE(2),
    CRED_PERSIST_ENTERPRISE(3);

    private int type;
    private static Map<Integer, SecureStorageWindowsCredentialPersistType> map =
        new HashMap<Integer, SecureStorageWindowsCredentialPersistType>();

    SecureStorageWindowsCredentialPersistType(int type) {
      this.type = type;
    }

    static {
      for (SecureStorageWindowsCredentialPersistType credPersistType :
          SecureStorageWindowsCredentialPersistType.values()) {
        map.put(credPersistType.type, credPersistType);
      }
    }

    public int getType() {
      return type;
    }
  }

  static class Advapi32LibManager {
    private static Advapi32Lib INSTANCE = null;

    private static class ResourceHolder {
      // map Windows advapi32.dll to Interface Advapi32Lib
      private static final Advapi32Lib INSTANCE =
          (Advapi32Lib)
              Native.loadLibrary("advapi32", Advapi32Lib.class, W32APIOptions.UNICODE_OPTIONS);
    }

    public static Advapi32Lib getInstance() {
      if (INSTANCE == null) {
        INSTANCE = ResourceHolder.INSTANCE;
      }
      return INSTANCE;
    }

    /** This function is used only for unit test */
    public static void setInstance(Advapi32Lib instance) {
      INSTANCE = instance;
    }

    /** This function is a helper function for testing */
    public static void resetInstance() {
      if (Constants.getOS() == Constants.OS.WINDOWS) {
        INSTANCE = ResourceHolder.INSTANCE;
      }
    }
  }

  interface Advapi32Lib extends StdCallLibrary {
    /** BOOL CredReadW( LPCWSTR TargetName, DWORD Type, DWORD Flags, PCREDENTIALW *Credential ); */
    boolean CredReadW(String targetName, int type, int flags, PointerByReference pcred);

    /** BOOL CredWriteW( PCREDENTIALW Credential, DWORD Flags ); */
    boolean CredWriteW(SecureStorageWindowsManager.SecureStorageWindowsCredential cred, int flags);

    /** BOOL CredDeleteW( LPCWSTR TargetName, DWORD Type, DWORD Flags ); */
    boolean CredDeleteW(String targetName, int type, int flags);

    /** void CredFree( PVOID Buffer ); */
    void CredFree(Pointer cred);
  }
}
