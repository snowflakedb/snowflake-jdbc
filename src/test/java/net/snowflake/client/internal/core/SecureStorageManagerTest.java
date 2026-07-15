package net.snowflake.client.internal.core;

import static net.snowflake.client.internal.jdbc.SnowflakeUtil.systemGetProperty;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import com.sun.jna.Memory;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import net.snowflake.client.annotations.RunOnLinux;
import net.snowflake.client.annotations.RunOnMac;
import net.snowflake.client.annotations.RunOnWindows;
import net.snowflake.client.annotations.RunOnWindowsOrMac;
import net.snowflake.client.internal.jdbc.SnowflakeUtil;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class MockAdvapi32Lib implements SecureStorageWindowsManager.Advapi32Lib {
  @Override
  public boolean CredReadW(String targetName, int type, int flags, PointerByReference pcred) {
    Pointer target = MockWindowsCredentialManager.getCredential(targetName);
    pcred.setValue(target);
    return target == null ? false : true;
  }

  @Override
  public boolean CredWriteW(
      SecureStorageWindowsManager.SecureStorageWindowsCredential cred, int flags) {
    MockWindowsCredentialManager.addCredential(cred);
    return true;
  }

  @Override
  public boolean CredDeleteW(String targetName, int type, int flags) {
    MockWindowsCredentialManager.deleteCredential(targetName);
    return true;
  }

  @Override
  public void CredFree(Pointer cred) {
    // mock function
  }
}

class MockSecurityLib implements SecureStorageAppleManager.SecurityLib {
  @Override
  public int SecKeychainFindGenericPassword(
      Pointer keychainOrArray,
      int serviceNameLength,
      byte[] serviceName,
      int accountNameLength,
      byte[] accountName,
      int[] passwordLength,
      Pointer[] passwordData,
      Pointer[] itemRef) {
    MockMacKeychainManager.MockMacKeychainItem credItem =
        MockMacKeychainManager.getCredential(serviceName, accountName);
    if (credItem == null) {
      return SecureStorageAppleManager.SecurityLib.ERR_SEC_ITEM_NOT_FOUND;
    }

    if (passwordLength != null && passwordLength.length > 0) {
      passwordLength[0] = credItem.getLength();
    }

    if (passwordData != null && passwordData.length > 0) {
      passwordData[0] = credItem.getPointer();
    }

    if (itemRef != null && itemRef.length > 0) {
      itemRef[0] = credItem.getPointer();
    }
    return SecureStorageAppleManager.SecurityLib.ERR_SEC_SUCCESS;
  }

  @Override
  public int SecKeychainAddGenericPassword(
      Pointer keychain,
      int serviceNameLength,
      byte[] serviceName,
      int accountNameLength,
      byte[] accountName,
      int passwordLength,
      byte[] passwordData,
      Pointer[] itemRef) {
    MockMacKeychainManager.addCredential(serviceName, accountName, passwordLength, passwordData);
    return SecureStorageAppleManager.SecurityLib.ERR_SEC_SUCCESS;
  }

  @Override
  public int SecKeychainItemModifyContent(
      Pointer itemRef, Pointer attrList, int length, byte[] data) {
    MockMacKeychainManager.replaceCredential(itemRef, length, data);
    return SecureStorageAppleManager.SecurityLib.ERR_SEC_SUCCESS;
  }

  @Override
  public int SecKeychainItemDelete(Pointer itemRef) {
    MockMacKeychainManager.deleteCredential(itemRef);
    return SecureStorageAppleManager.SecurityLib.ERR_SEC_SUCCESS;
  }

  @Override
  public int SecKeychainItemFreeContent(Pointer[] attrList, Pointer data) {
    // mock function
    return SecureStorageAppleManager.SecurityLib.ERR_SEC_SUCCESS;
  }
}

class MockWindowsCredentialManager {
  private static final Map<String, Pointer> credentialManager = new HashMap<>();

  static void addCredential(SecureStorageWindowsManager.SecureStorageWindowsCredential cred) {
    cred.write();
    credentialManager.put(cred.TargetName.toString(), cred.getPointer());
  }

  static Pointer getCredential(String target) {
    return credentialManager.get(target);
  }

  static void deleteCredential(String target) {
    credentialManager.remove(target);
  }
}

class MockMacKeychainManager {
  private static final Map<String, Map<String, MockMacKeychainItem>> keychainManager =
      new HashMap<>();

  static void addCredential(byte[] targetName, byte[] userName, int credLength, byte[] credData) {
    String target = new String(targetName);
    String user = new String(userName);

    keychainManager.computeIfAbsent(target, newMap -> new HashMap<>());
    Map<String, MockMacKeychainItem> currentTargetMap = keychainManager.get(target);

    currentTargetMap.put(user, buildMacKeychainItem(credLength, credData));
  }

  static MockMacKeychainItem getCredential(byte[] targetName, byte[] userName) {
    Map<String, MockMacKeychainItem> targetMap = keychainManager.get(new String(targetName));
    return targetMap != null ? targetMap.get(new String(userName)) : null;
  }

  static void replaceCredential(Pointer itemRef, int credLength, byte[] credData) {
    for (Map.Entry<String, Map<String, MockMacKeychainItem>> elem : keychainManager.entrySet()) {
      Map<String, MockMacKeychainItem> targetMap = elem.getValue();
      for (Map.Entry<String, MockMacKeychainItem> elem0 : targetMap.entrySet()) {
        if (elem0.getValue().getPointer().toString().equals(itemRef.toString())) {
          targetMap.put(elem0.getKey(), buildMacKeychainItem(credLength, credData));
          return;
        }
      }
    }
  }

  static void deleteCredential(Pointer itemRef) {
    Iterator<Map.Entry<String, Map<String, MockMacKeychainItem>>> targetIter =
        keychainManager.entrySet().iterator();
    while (targetIter.hasNext()) {
      Map.Entry<String, Map<String, MockMacKeychainItem>> targetMap = targetIter.next();
      Iterator<Map.Entry<String, MockMacKeychainItem>> userIter =
          targetMap.getValue().entrySet().iterator();
      while (userIter.hasNext()) {
        Map.Entry<String, MockMacKeychainItem> cred = userIter.next();
        if (cred.getValue().getPointer().toString().equals(itemRef.toString())) {
          userIter.remove();
          return;
        }
      }
    }
  }

  static MockMacKeychainItem buildMacKeychainItem(int itemLength, byte[] itemData) {
    Memory itemMem = new Memory(itemLength);
    itemMem.write(0, itemData, 0, itemLength);
    return new MockMacKeychainItem(itemLength, itemMem);
  }

  static class MockMacKeychainItem {
    private int length;
    private Pointer pointer;

    MockMacKeychainItem(int length, Pointer pointer) {
      this.length = length;
      this.pointer = pointer;
    }

    void setLength(int length) {
      this.length = length;
    }

    int getLength() {
      return length;
    }

    void setPointer(Pointer pointer) {
      this.pointer = pointer;
    }

    Pointer getPointer() {
      return pointer;
    }
  }
}

public class SecureStorageManagerTest {

  private static final String host = "fakeHost";
  private static final String user = "fakeUser";
  private static final String idToken = "fakeIdToken";
  private static final String idToken0 = "fakeIdToken0";
  private static final String mfaToken = "fakeMfaToken";

  // -------------------------------------------------------------------------
  // Golden hash — must match the cross-driver spec exactly (do not change)
  // -------------------------------------------------------------------------

  @Test
  void shouldReproduceGoldenHashFromSpec() {
    // Raw inputs: quoted identifiers already carry their final display case (uppercase inside
    // quotes), matching the pre-normalized golden vector in the cross-driver spec (00-INDEX.md §3).
    // normalizeIdentifier preserves quoted segments verbatim, so "FIRST LAST" stays "FIRST LAST".
    CacheKeyInput input =
        new CacheKeyInput(
            "DPOP_BUNDLED_ACCESS_TOKEN",
            "https://login.microsoftonline.com:443/tenant-id/oauth2/v2.0",
            "https://myorg-myaccount.privatelink.snowflakecomputing.com",
            "\"FIRST LAST\"@long-corporate-domain.example.com",
            "\"ANALYST ROLE WITH SPACES\":north_america:prod:readonly");
    assertThat(
        SecureStorageManager.buildCacheKey(input),
        // pragma: allowlist nextline secret
        is(
            "SnowflakeTokenCache.v2.75ff2ad65a68afb402f125f62894697673c5ef3d863aba466d16b7a81053d1f4"));
  }

  // -------------------------------------------------------------------------
  // buildCacheKey — one key per credential type
  // -------------------------------------------------------------------------

  @Test
  void shouldBuildCredentialsKeyForAllTypes() {
    CacheKeyInput base =
        new CacheKeyInput(CachedCredentialType.OAUTH_ACCESS_TOKEN.getValue(), host, host, user, "");
    assertThat(SecureStorageManager.buildCacheKey(base).startsWith("SnowflakeTokenCache.v2."), is(true));

    CacheKeyInput refresh =
        new CacheKeyInput(
            CachedCredentialType.OAUTH_REFRESH_TOKEN.getValue(), host, host, user, "");
    assertThat(SecureStorageManager.buildCacheKey(refresh).startsWith("SnowflakeTokenCache.v2."), is(true));

    CacheKeyInput id =
        new CacheKeyInput(CachedCredentialType.ID_TOKEN.getValue(), host, host, user, "");
    assertThat(SecureStorageManager.buildCacheKey(id).startsWith("SnowflakeTokenCache.v2."), is(true));

    CacheKeyInput mfa =
        new CacheKeyInput(CachedCredentialType.MFA_TOKEN.getValue(), host, host, user, "");
    assertThat(SecureStorageManager.buildCacheKey(mfa).startsWith("SnowflakeTokenCache.v2."), is(true));
  }

  // -------------------------------------------------------------------------
  // normalizeUrl
  // -------------------------------------------------------------------------

  @Test
  void shouldNormalizeUrl() {
    assertThat(SecureStorageManager.normalizeUrl(null), is(""));
    assertThat(SecureStorageManager.normalizeUrl(""), is(""));
    assertThat(
        SecureStorageManager.normalizeUrl("https://myhost.snowflakecomputing.com"),
        is("MYHOST.SNOWFLAKECOMPUTING.COM"));
    assertThat(
        SecureStorageManager.normalizeUrl("http://myhost.snowflakecomputing.com/"),
        is("MYHOST.SNOWFLAKECOMPUTING.COM"));
    assertThat(
        SecureStorageManager.normalizeUrl(
            "https://login.microsoftonline.com:443/tenant-id/oauth2/v2.0"),
        is("LOGIN.MICROSOFTONLINE.COM:443/TENANT-ID/OAUTH2/V2.0"));
    assertThat(
        SecureStorageManager.normalizeUrl("https://user:pass@host.example.com/path"),
        is("HOST.EXAMPLE.COM/PATH"));
    assertThat(
        SecureStorageManager.normalizeUrl("https://host.example.com/path?foo=bar#frag"),
        is("HOST.EXAMPLE.COM/PATH"));
  }

  // -------------------------------------------------------------------------
  // normalizeIdentifier
  // -------------------------------------------------------------------------

  @Test
  void shouldNormalizeIdentifier() {
    assertThat(SecureStorageManager.normalizeIdentifier(null), is(""));
    assertThat(SecureStorageManager.normalizeIdentifier(""), is(""));
    assertThat(SecureStorageManager.normalizeIdentifier("simple_user"), is("SIMPLE_USER"));
    // Quoted segments are preserved verbatim (Snowflake case-sensitive identifier semantics).
    assertThat(
        SecureStorageManager.normalizeIdentifier("\"First Last\"@long-domain.example.com"),
        is("\"First Last\"@LONG-DOMAIN.EXAMPLE.COM"));
    assertThat(
        SecureStorageManager.normalizeIdentifier(
            "\"Analyst Role With Spaces\":north_america:prod:readonly"),
        is("\"Analyst Role With Spaces\":NORTH_AMERICA:PROD:READONLY"));
    // Already-uppercase quoted segment — unchanged.
    assertThat(
        SecureStorageManager.normalizeIdentifier(
            "\"FIRST LAST\"@long-domain.example.com"),
        is("\"FIRST LAST\"@LONG-DOMAIN.EXAMPLE.COM"));
  }

  // -------------------------------------------------------------------------
  // Dimension isolation — different inputs must produce different keys
  // -------------------------------------------------------------------------

  @Test
  void shouldProduceDifferentKeysForDifferentSnowflakeHosts() {
    String key1 =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput("ID_TOKEN", "idp.example.com", "account1.snowflake.com", user, ""));
    String key2 =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput("ID_TOKEN", "idp.example.com", "account2.snowflake.com", user, ""));
    assertThat(key1, is(not(key2)));
  }

  @Test
  void shouldProduceDifferentKeysForDifferentRoles() {
    String key1 =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput("OAUTH_ACCESS_TOKEN", host, host, user, "ROLE_A"));
    String key2 =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput("OAUTH_ACCESS_TOKEN", host, host, user, "ROLE_B"));
    assertThat(key1, is(not(key2)));
  }

  @Test
  void shouldProduceStableKeyForMfaWithEmptyRole() {
    String key1 =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput("MFA_TOKEN", host, host, user, ""));
    String key2 =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput("MFA_TOKEN", host, host, user, ""));
    assertThat(key1, is(key2));
    assertThat(
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput("MFA_TOKEN", host, host, user, "ROLE_X")),
        is(not(key1)));
  }

  @Test
  void shouldProduceDifferentKeysForDifferentTokenTypes() {
    String idKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput("ID_TOKEN", host, host, user, ""));
    String mfaKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput("MFA_TOKEN", host, host, user, ""));
    String oauthKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput("OAUTH_ACCESS_TOKEN", host, host, user, ""));
    assertThat(idKey, is(not(mfaKey)));
    assertThat(idKey, is(not(oauthKey)));
    assertThat(mfaKey, is(not(oauthKey)));
  }

  // -------------------------------------------------------------------------
  // Platform manager round-trip tests
  // -------------------------------------------------------------------------

  @Test
  @RunOnWindowsOrMac
  public void testLoadNativeLibrary() {
    if (Constants.getOS() == Constants.OS.MAC) {
      assertThat(SecureStorageAppleManager.SecurityLibManager.getInstance(), is(notNullValue()));
    }
    if (Constants.getOS() == Constants.OS.WINDOWS) {
      assertThat(SecureStorageWindowsManager.Advapi32LibManager.getInstance(), is(notNullValue()));
    }
  }

  @Test
  @RunOnWindows
  public void testWindowsManager() {
    SecureStorageWindowsManager.Advapi32LibManager.setInstance(new MockAdvapi32Lib());
    SecureStorageManager manager = SecureStorageWindowsManager.builder();
    testBody(manager);
    SecureStorageWindowsManager.Advapi32LibManager.resetInstance();
  }

  @Test
  @RunOnMac
  public void testMacManager() {
    SecureStorageAppleManager.SecurityLibManager.setInstance(new MockSecurityLib());
    SecureStorageManager manager = SecureStorageAppleManager.builder();
    testBody(manager);
    SecureStorageAppleManager.SecurityLibManager.resetInstance();
  }

  @Test
  @RunOnLinux
  public void testLinuxManager() {
    String cacheDirectory =
        Paths.get(systemGetProperty("user.home"), ".cache", "snowflake_test_cache")
            .toAbsolutePath()
            .toString();
    try (MockedStatic<SnowflakeUtil> snowflakeUtilMockedStatic =
        Mockito.mockStatic(SnowflakeUtil.class)) {
      snowflakeUtilMockedStatic
          .when(() -> SnowflakeUtil.byteToHexStringLower(Mockito.any(byte[].class)))
          .thenCallRealMethod();
      snowflakeUtilMockedStatic
          .when(() -> SnowflakeUtil.isNullOrEmpty(Mockito.any()))
          .thenCallRealMethod();
      snowflakeUtilMockedStatic
          .when(
              () ->
                  SnowflakeUtil.systemGetProperty("net.snowflake.jdbc.temporaryCredentialCacheDir"))
          .thenReturn(cacheDirectory);
      SecureStorageManager manager = SecureStorageLinuxManager.getInstance();

      testBody(manager);
      testDeleteLinux(manager);
    }
  }

  private String buildTestKey(String tokenType) {
    return SecureStorageManager.buildCacheKey(
        new CacheKeyInput(tokenType, host, host, user, ""));
  }

  private void testBody(SecureStorageManager manager) {
    String cacheKey = buildTestKey("ID_TOKEN");

    // first delete possible old credential
    assertThat(
        manager.deleteCredential(cacheKey),
        equalTo(SecureStorageManager.SecureStorageStatus.SUCCESS));

    // ensure no old credential exists
    assertThat(manager.getCredential(cacheKey), is(nullValue()));

    // set token
    assertThat(
        manager.setCredential(cacheKey, idToken),
        equalTo(SecureStorageManager.SecureStorageStatus.SUCCESS));
    assertThat(manager.getCredential(cacheKey), equalTo(idToken));

    // update token
    assertThat(
        manager.setCredential(cacheKey, idToken0),
        equalTo(SecureStorageManager.SecureStorageStatus.SUCCESS));
    assertThat(manager.getCredential(cacheKey), equalTo(idToken0));

    // delete token
    assertThat(
        manager.deleteCredential(cacheKey),
        equalTo(SecureStorageManager.SecureStorageStatus.SUCCESS));
    assertThat(manager.getCredential(cacheKey), is(nullValue()));
  }

  private void testDeleteLinux(SecureStorageManager manager) {
    // Verifies that partial deletion works — removing one key leaves other keys intact.
    String idKey = buildTestKey("ID_TOKEN");
    String mfaKey = buildTestKey("MFA_TOKEN");

    assertThat(
        manager.setCredential(idKey, idToken),
        equalTo(SecureStorageManager.SecureStorageStatus.SUCCESS));
    assertThat(
        manager.setCredential(mfaKey, mfaToken),
        equalTo(SecureStorageManager.SecureStorageStatus.SUCCESS));
    assertThat(manager.getCredential(idKey), equalTo(idToken));
    assertThat(manager.getCredential(mfaKey), equalTo(mfaToken));

    assertThat(
        manager.deleteCredential(idKey),
        equalTo(SecureStorageManager.SecureStorageStatus.SUCCESS));
    assertThat(manager.getCredential(idKey), equalTo(null));

    assertThat(manager.getCredential(mfaKey), equalTo(mfaToken));

    assertThat(
        manager.deleteCredential(mfaKey),
        equalTo(SecureStorageManager.SecureStorageStatus.SUCCESS));
  }
}
