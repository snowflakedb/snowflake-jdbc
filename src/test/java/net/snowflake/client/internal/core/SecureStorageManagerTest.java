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
  // Golden hashes — do not change these expected values
  // -------------------------------------------------------------------------

  @Test
  void shouldReproduceOAuthGoldenHashFromSpec() {
    // idp and snowflake are lowercased; username and role contain double quotes so they are
    // returned verbatim (case-sensitive quoted identifiers preserved). Token type in the
    // prefix is PascalCase.
    CacheKeyInput input =
        new CacheKeyInput(
            "DpopBundledAccessToken",
            "https://login.microsoftonline.com:443/tenant-id/oauth2/v2.0",
            "https://myorg-myaccount.privatelink.snowflakecomputing.com",
            "\"First Last\"@long-corporate-domain.example.com",
            "\"Analyst Role With Spaces\":north_america:prod:readonly");
    assertThat(
        SecureStorageManager.buildCacheKey(input),
        // pragma: allowlist nextline secret
        is(
            "SnowflakeTokenCache.v2.DpopBundledAccessToken.741b6d66d252666d6821bfd19e0151511cf4efdaaeba2b3c87673aa4de6d2c0b"));
  }

  @Test
  void shouldReproduceMfaGoldenHashFromSpec() {
    // MFA keyData contains only snowflake + username (no idp, role, or token_type).
    CacheKeyInput input =
        new CacheKeyInput(
            "MfaToken",
            "",
            "https://myorg-myaccount.privatelink.snowflakecomputing.com",
            "\"First Last\"@long-corporate-domain.example.com",
            "");
    assertThat(
        SecureStorageManager.buildCacheKey(input),
        // pragma: allowlist nextline secret
        is(
            "SnowflakeTokenCache.v2.MfaToken.10c5dde84bb8f584c0df06ea826d418c4f580e08f9db10187c0cb5e2a732a0d6"));
  }

  // -------------------------------------------------------------------------
  // buildCacheKey — one key per credential type
  // -------------------------------------------------------------------------

  @Test
  void shouldBuildCredentialsKeyForAllTypes() {
    CacheKeyInput base =
        new CacheKeyInput(CachedCredentialType.OAUTH_ACCESS_TOKEN.getValue(), host, host, user, "");
    assertThat(
        SecureStorageManager.buildCacheKey(base)
            .startsWith("SnowflakeTokenCache.v2.OauthAccessToken."),
        is(true));

    CacheKeyInput refresh =
        new CacheKeyInput(
            CachedCredentialType.OAUTH_REFRESH_TOKEN.getValue(), host, host, user, "");
    assertThat(
        SecureStorageManager.buildCacheKey(refresh)
            .startsWith("SnowflakeTokenCache.v2.OauthRefreshToken."),
        is(true));

    CacheKeyInput id =
        new CacheKeyInput(CachedCredentialType.ID_TOKEN.getValue(), "", host, user, "");
    assertThat(
        SecureStorageManager.buildCacheKey(id).startsWith("SnowflakeTokenCache.v2.IdToken."),
        is(true));

    CacheKeyInput mfa =
        new CacheKeyInput(CachedCredentialType.MFA_TOKEN.getValue(), "", host, user, "");
    assertThat(
        SecureStorageManager.buildCacheKey(mfa).startsWith("SnowflakeTokenCache.v2.MfaToken."),
        is(true));
  }

  // -------------------------------------------------------------------------
  // normalizeUrl
  // -------------------------------------------------------------------------

  @Test
  void shouldNormalizeUrl() {
    assertThat(SecureStorageManager.normalizeUrl(null), is(""));
    assertThat(SecureStorageManager.normalizeUrl(""), is(""));
    assertThat(
        SecureStorageManager.normalizeUrl("https://MyHost.SnowflakeComputing.com"),
        is("myhost.snowflakecomputing.com"));
    assertThat(
        SecureStorageManager.normalizeUrl("http://MyHost.SnowflakeComputing.com/"),
        is("myhost.snowflakecomputing.com"));
    assertThat(
        SecureStorageManager.normalizeUrl(
            "https://Login.MicrosoftOnline.com:443/Tenant-ID/OAuth2/v2.0"),
        is("login.microsoftonline.com:443/tenant-id/oauth2/v2.0"));
    assertThat(
        SecureStorageManager.normalizeUrl("https://user:pass@Host.Example.com/Path"),
        is("host.example.com/path"));
    assertThat(
        SecureStorageManager.normalizeUrl("https://Host.Example.com/Path?foo=bar#frag"),
        is("host.example.com/path"));
  }

  // -------------------------------------------------------------------------
  // normalizeIdentifier
  // -------------------------------------------------------------------------

  @Test
  void shouldNormalizeIdentifier() {
    assertThat(SecureStorageManager.normalizeIdentifier(null), is(""));
    assertThat(SecureStorageManager.normalizeIdentifier(""), is(""));
    // No double quotes → lowercased.
    assertThat(SecureStorageManager.normalizeIdentifier("SIMPLE_USER"), is("simple_user"));
    assertThat(
        SecureStorageManager.normalizeIdentifier("JOHN.DOE@EXAMPLE.COM"),
        is("john.doe@example.com"));
    // Any double quote → returned verbatim, unchanged (case-sensitive quoted identifier).
    assertThat(
        SecureStorageManager.normalizeIdentifier("\"First Last\"@long-domain.example.com"),
        is("\"First Last\"@long-domain.example.com"));
    assertThat(
        SecureStorageManager.normalizeIdentifier(
            "\"Analyst Role With Spaces\":north_america:prod:readonly"),
        is("\"Analyst Role With Spaces\":north_america:prod:readonly"));
    // A double quote that is not at position 0 still triggers the verbatim path.
    assertThat(
        SecureStorageManager.normalizeIdentifier("prefix-\"Segment\""), is("prefix-\"Segment\""));
  }

  // -------------------------------------------------------------------------
  // Dimension isolation — different inputs must produce different keys
  // -------------------------------------------------------------------------

  @Test
  void shouldProduceDifferentKeysForDifferentSnowflakeHosts() {
    String key1 =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput("IdToken", "", "account1.snowflake.com", user, ""));
    String key2 =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput("IdToken", "", "account2.snowflake.com", user, ""));
    assertThat(key1, is(not(key2)));
  }

  @Test
  void shouldProduceDifferentKeysForDifferentRoles() {
    String key1 =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput("OauthAccessToken", host, host, user, "ROLE_A"));
    String key2 =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput("OauthAccessToken", host, host, user, "ROLE_B"));
    assertThat(key1, is(not(key2)));
  }

  @Test
  void shouldProduceStableKeyForMfaRegardlessOfRole() {
    String key1 =
        SecureStorageManager.buildCacheKey(new CacheKeyInput("MfaToken", "", host, user, ""));
    String key2 =
        SecureStorageManager.buildCacheKey(new CacheKeyInput("MfaToken", "", host, user, ""));
    assertThat(key1, is(key2));
    // MFA keyData excludes role — role variation does not affect the key.
    assertThat(
        SecureStorageManager.buildCacheKey(new CacheKeyInput("MfaToken", "", host, user, "ROLE_X")),
        is(key1));
  }

  @Test
  void shouldProduceDifferentKeysForDifferentTokenTypes() {
    String idKey =
        SecureStorageManager.buildCacheKey(new CacheKeyInput("IdToken", "", host, user, ""));
    String mfaKey =
        SecureStorageManager.buildCacheKey(new CacheKeyInput("MfaToken", "", host, user, ""));
    String oauthKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput("OauthAccessToken", host, host, user, ""));
    assertThat(idKey, is(not(mfaKey)));
    assertThat(idKey, is(not(oauthKey)));
    assertThat(mfaKey, is(not(oauthKey)));
  }

  @Test
  void shouldProduceKeyWithoutIdpOrRoleForMfa() throws Exception {
    // Serialized keyData for MFA must contain exactly the two fields snowflake and username.
    CacheKeyInput input =
        new CacheKeyInput("MfaToken", "some-idp.example.com", host, user, "SOME_ROLE");
    String mfaKey = SecureStorageManager.buildCacheKey(input);
    // Key must start with the MfaToken prefix segment.
    assertThat(mfaKey.startsWith("SnowflakeTokenCache.v2.MfaToken."), is(true));
    // MFA key must match one built with empty idp and role — they are not in the keyData.
    String mfaKeyNoIdpRole =
        SecureStorageManager.buildCacheKey(new CacheKeyInput("MfaToken", "", host, user, ""));
    assertThat(mfaKey, is(mfaKeyNoIdpRole));
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
    return SecureStorageManager.buildCacheKey(new CacheKeyInput(tokenType, "", host, user, ""));
  }

  private void testBody(SecureStorageManager manager) {
    String cacheKey = buildTestKey("IdToken");

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
    String idKey = buildTestKey("IdToken");
    String mfaKey = buildTestKey("MfaToken");

    assertThat(
        manager.setCredential(idKey, idToken),
        equalTo(SecureStorageManager.SecureStorageStatus.SUCCESS));
    assertThat(
        manager.setCredential(mfaKey, mfaToken),
        equalTo(SecureStorageManager.SecureStorageStatus.SUCCESS));
    assertThat(manager.getCredential(idKey), equalTo(idToken));
    assertThat(manager.getCredential(mfaKey), equalTo(mfaToken));

    assertThat(
        manager.deleteCredential(idKey), equalTo(SecureStorageManager.SecureStorageStatus.SUCCESS));
    assertThat(manager.getCredential(idKey), equalTo(null));

    assertThat(manager.getCredential(mfaKey), equalTo(mfaToken));

    assertThat(
        manager.deleteCredential(mfaKey),
        equalTo(SecureStorageManager.SecureStorageStatus.SUCCESS));
  }
}
