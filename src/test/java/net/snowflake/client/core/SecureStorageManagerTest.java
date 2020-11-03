/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import com.sun.jna.Memory;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningNotOnWinMac;
import org.junit.Rule;
import org.junit.Test;

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
  // This required to use ConditionalIgnore annotation
  @Rule public ConditionalIgnoreRule rule = new ConditionalIgnoreRule();

  private static final String host = "fakeHost";
  private static final String user = "fakeUser";
  private static final String idToken = "fakeIdToken";
  private static final String idToken0 = "fakeIdToken0";

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningNotOnWinMac.class)
  public void testLoadNativeLibrary() {
    // Make sure the loading of native platform library won't break.
    if (Constants.getOS() == Constants.OS.MAC) {
      assertThat(SecureStorageAppleManager.SecurityLibManager.getInstance(), is(nullValue()));
    }

    if (Constants.getOS() == Constants.OS.WINDOWS) {
      assertThat(SecureStorageWindowsManager.Advapi32LibManager.getInstance(), is(notNullValue()));
    }
  }

  @Test
  public void testWindowsManager() {
    SecureStorageWindowsManager.Advapi32LibManager.setInstance(new MockAdvapi32Lib());
    SecureStorageManager manager = SecureStorageWindowsManager.builder();

    testBody(manager);
  }

  @Test
  public void testMacManager() {
    SecureStorageAppleManager.SecurityLibManager.setInstance(new MockSecurityLib());
    SecureStorageManager manager = SecureStorageAppleManager.builder();

    testBody(manager);
  }

  @Test
  public void testLinuxManager() {
    SecureStorageManager manager = SecureStorageLinuxManager.builder();

    testBody(manager);
  }

  private void testBody(SecureStorageManager manager) {
    // first delete possible old credential
    assertThat(
        manager.deleteCredential(host, user),
        equalTo(SecureStorageManager.SecureStorageStatus.SUCCESS));

    // ensure no old credential exists
    assertThat(manager.getCredential(host, user), is(nullValue()));

    // set token
    assertThat(
        manager.setCredential(host, user, idToken),
        equalTo(SecureStorageManager.SecureStorageStatus.SUCCESS));
    assertThat(manager.getCredential(host, user), equalTo(idToken));

    // update token
    assertThat(
        manager.setCredential(host, user, idToken0),
        equalTo(SecureStorageManager.SecureStorageStatus.SUCCESS));
    assertThat(manager.getCredential(host, user), equalTo(idToken0));

    // delete token
    assertThat(
        manager.deleteCredential(host, user),
        equalTo(SecureStorageManager.SecureStorageStatus.SUCCESS));
    assertThat(manager.getCredential(host, user), is(nullValue()));
  }
}
