package net.snowflake.client.core;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
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
import net.snowflake.client.jdbc.SnowflakeUtil;
import org.junit.jupiter.api.Assertions;
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

  private static final String ID_TOKEN = "ID_TOKEN";
  private static final String MFA_TOKEN = "MFATOKEN";

  @Test
  public void testBuildCredentialsKey() {
    // hex values obtained using https://emn178.github.io/online-tools/sha256.html
    String hashedKey =
        SecureStorageManager.buildCredentialsKey(
            host, user, CachedCredentialType.OAUTH_ACCESS_TOKEN.getValue());
    Assertions.assertEquals(
        "A7C7EBB89312E88552CD00664A0E20929801FACFBD682BF7C2363FB6EC8F914E", hashedKey);

    hashedKey =
        SecureStorageManager.buildCredentialsKey(
            host, user, CachedCredentialType.OAUTH_REFRESH_TOKEN.getValue());
    Assertions.assertEquals(
        "DB37028833FA02B125FBD6DE8CE679C7E62E7D38FAC585E98060E00987F96772", hashedKey);

    hashedKey =
        SecureStorageManager.buildCredentialsKey(
            host, user, CachedCredentialType.ID_TOKEN.getValue());
    Assertions.assertEquals(
        "6AA3F783E07D1D2182DAB59442806E2433C55C2BD4D9240790FD5B4B91FD4FDB", hashedKey);

    hashedKey =
        SecureStorageManager.buildCredentialsKey(
            host, user, CachedCredentialType.MFA_TOKEN.getValue());
    Assertions.assertEquals(
        "9D10D4EFE45605D85993C6AC95334F1B63D36611B83615656EC7F277A947BF4B", hashedKey);
  }

  @Test
  @RunOnWindowsOrMac
  public void testLoadNativeLibrary() {
    // Only run on Mac or Windows. Make sure the loading of native platform library won't break.
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
          .when(
              () ->
                  SnowflakeUtil.systemGetProperty("net.snowflake.jdbc.temporaryCredentialCacheDir"))
          .thenReturn(cacheDirectory);
      SecureStorageManager manager = SecureStorageLinuxManager.getInstance();

      testBody(manager);
      testDeleteLinux(manager);
    }
  }

  private void testBody(SecureStorageManager manager) {
    // first delete possible old credential
    assertThat(
        manager.deleteCredential(host, user, ID_TOKEN),
        equalTo(SecureStorageManager.SecureStorageStatus.SUCCESS));

    // ensure no old credential exists
    assertThat(manager.getCredential(host, user, ID_TOKEN), is(nullValue()));

    // set token
    assertThat(
        manager.setCredential(host, user, ID_TOKEN, idToken),
        equalTo(SecureStorageManager.SecureStorageStatus.SUCCESS));
    assertThat(manager.getCredential(host, user, ID_TOKEN), equalTo(idToken));

    // update token
    assertThat(
        manager.setCredential(host, user, ID_TOKEN, idToken0),
        equalTo(SecureStorageManager.SecureStorageStatus.SUCCESS));
    assertThat(manager.getCredential(host, user, ID_TOKEN), equalTo(idToken0));

    // delete token
    assertThat(
        manager.deleteCredential(host, user, ID_TOKEN),
        equalTo(SecureStorageManager.SecureStorageStatus.SUCCESS));
    assertThat(manager.getCredential(host, user, ID_TOKEN), is(nullValue()));
  }

  private void testDeleteLinux(SecureStorageManager manager) {
    // The old delete api of local file cache on Linux was to remove the whole file, where we can't
    // partially remove some credentials
    // This test aims to test the new delete api

    // first create two credentials
    assertThat(
        manager.setCredential(host, user, ID_TOKEN, idToken),
        equalTo(SecureStorageManager.SecureStorageStatus.SUCCESS));
    assertThat(
        manager.setCredential(host, user, MFA_TOKEN, mfaToken),
        equalTo(SecureStorageManager.SecureStorageStatus.SUCCESS));
    assertThat(manager.getCredential(host, user, ID_TOKEN), equalTo(idToken));
    assertThat(manager.getCredential(host, user, MFA_TOKEN), equalTo(mfaToken));

    // delete one of them
    assertThat(
        manager.deleteCredential(host, user, ID_TOKEN),
        equalTo(SecureStorageManager.SecureStorageStatus.SUCCESS));
    assertThat(manager.getCredential(host, user, ID_TOKEN), equalTo(null));

    // check another one
    assertThat(manager.getCredential(host, user, MFA_TOKEN), equalTo(mfaToken));

    assertThat(
        manager.deleteCredential(host, user, MFA_TOKEN),
        equalTo(SecureStorageManager.SecureStorageStatus.SUCCESS));
  }
}
