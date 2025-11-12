=======================================
Old Driver Backward Compatibility Tests
=======================================

Overview
========

The ``TestOnly/`` directory contains tests that verify backward compatibility by running the 
**current test suite** against an **old driver version** (e.g., 3.13.21).

This ensures that tests remain compatible across driver versions.

Status: DISABLED for 4.0.0
===========================

Old driver tests are **currently disabled** for the 4.0.0 release due to breaking API changes:

- Package restructuring (``net.snowflake.client.jdbc.*`` → ``net.snowflake.client.api.*``)
- Interface extraction for implementation classes
- Old driver (3.x) is incompatible with new test structure

The tests are **commented out** in ``ci/container/test_component.sh`` to allow easy re-enabling.

Re-enabling Old Driver Tests (Post-4.0.0)
==========================================

Once 4.0.0 is released, to test compatibility between 4.x versions (e.g., 4.0.0 → 4.1.0):

1. **Update TestOnly/pom.xml**
   
   .. code-block:: xml
   
      <version>4.0.0</version>  <!-- Change from 3.13.21 to 4.0.0 -->

2. **Uncomment the test block in ci/container/test_component.sh**
   
   Search for ``DISABLED for 4.0.0`` and uncomment the ``if [[ "$is_old_driver" == "true" ]]`` block

3. **Run tests**
   
   The CI will now test 4.0.0 driver with 4.x test suite

Configuration
=============

**Environment Variables:**

- ``is_old_driver=true``: Enables old driver test mode (set by CI when configured)

**Files Modified for 4.0.0:**

- ``ci/container/test_component.sh``: Old driver test block commented out
- ``ci/log_analyze_setup.sh``: Added note about disabled tests

Why Disabled for 4.0.0?
========================

**Breaking Changes in 4.0.0:**

1. **Package Restructuring**
   
   - Old: ``net.snowflake.client.jdbc.SnowflakeConnection``
   - New: ``net.snowflake.client.api.connection.SnowflakeConnection``

2. **Interface Extraction**
   
   - Old: Concrete classes (``SnowflakeDatabaseMetaData``)
   - New: Interfaces + Implementations (``SnowflakeDatabaseMetaData`` interface + ``SnowflakeDatabaseMetaDataImpl``)

3. **Test Imports**
   
   - Current tests use new package structure
   - Old driver (3.13.21) doesn't have these packages
   - Result: Compilation failures

Migration Path
==============

**For 4.0.0 Release:**

1. Old driver tests are disabled ✅
2. Regular current-version tests run ✅
3. Breaking changes documented ✅

**For 4.1.0+ Releases:**

1. Update ``TestOnly/pom.xml`` to reference 4.0.0
2. Uncomment test block in ``ci/container/test_component.sh``
3. Tests now verify 4.0.0 → 4.1.0 compatibility ✅

**For 5.0.0 Release:**

1. Re-evaluate and likely disable again for major version changes
2. Update to 4.x baseline version in ``TestOnly/pom.xml``
3. Repeat the cycle
