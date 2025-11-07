# ✅ Driver Migration Test Fix

## Problem

After migrating `SnowflakeDriver` from `net.snowflake.client.jdbc` to `net.snowflake.client.api.driver`, the test `JDK14LoggerTest.testLegacyLoggerInit` started failing.

**Error:**
```
JDK14LoggerTest.testLegacyLoggerInit:29 expected: <true> but was: <false>
```

---

## Root Cause

The `SFFormatter` class was dynamically calculating the `CLASS_NAME_PREFIX` from the `SnowflakeDriver` package:

**Before Migration:**
```java
public static final String CLASS_NAME_PREFIX =
    SnowflakeDriver.class
        .getPackage()
        .getName()
        .substring(0, SnowflakeDriver.class.getPackage().getName().lastIndexOf('.'));
```

- Driver package: `net.snowflake.client.jdbc`
- Calculated PREFIX: `net.snowflake.client` ✅ Correct

**After Migration:**
- Driver package: `net.snowflake.client.api.driver`
- Calculated PREFIX: `net.snowflake.client.api` ❌ Wrong!

This broke the logger hierarchy initialization because loggers were being created under the wrong package prefix.

---

## Fix

Hardcoded the `CLASS_NAME_PREFIX` to its correct value since it should always be `"net.snowflake.client"` regardless of where the `SnowflakeDriver` class is located:

```java
// Fixed to "net.snowflake.client" since SnowflakeDriver moved to api.driver package
// Previously calculated dynamically, but should always be the root client package
public static final String CLASS_NAME_PREFIX = "net.snowflake.client";
```

---

## Test Results After Fix

```
✅ Tests run:   858
✅ Failures:    0
✅ Errors:      0
✅ Skipped:     7

🎉 BUILD SUCCESS - All tests passing!
```

---

## Files Changed

- `src/main/java/net/snowflake/client/log/SFFormatter.java`
  - Changed `CLASS_NAME_PREFIX` from dynamic calculation to hardcoded value

---

**Migration now 100% complete with all tests passing!** ✅
