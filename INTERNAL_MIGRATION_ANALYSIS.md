# 📊 Internal Package Migration - Analysis & Strategy

## Packages to Migrate

| Package | Files | Complexity | Notes |
|---------|-------|------------|-------|
| `config` | 4 | Low | Configuration classes |
| `log` | 16 | Low | Logging utilities |
| `util` | 18 | Low | General utilities |
| `loader` | 7 | Low | Internal loader implementation |
| `jdbc` | 100 | **High** | Main JDBC implementation |
| `core` | 182 | **High** | Core internal classes (auth, arrow, bind, etc.) |
| **TOTAL** | **327** | - | - |

## Package Dependencies

```
core/          (foundational - used by everything)
  ├── arrow/
  ├── auth/
  ├── bind/
  ├── crl/
  ├── json/
  └── structs/

jdbc/          (depends on core, uses log/util)
  ├── cloud/storage/
  ├── diagnostic/
  ├── telemetry/
  └── telemetryOOB/

loader/        (depends on jdbc, core)
log/           (minimal dependencies)
util/          (minimal dependencies)
config/        (minimal dependencies)
```

## Migration Strategies

### Option A: "Big Bang" Migration ⚡
**Approach:** Move all 327 files in one operation

**Steps:**
1. Create `internal/` directory structure
2. Move all packages: `config`, `core`, `jdbc`, `loader`, `log`, `util`
3. Update package declarations in all files
4. Use regex to update all imports across entire codebase
5. Compile and fix any remaining issues
6. Run all tests

**Pros:**
- ✅ Fast (single operation)
- ✅ Clean final state
- ✅ Simpler import updates (one regex pass)
- ✅ No intermediate states to maintain

**Cons:**
- ❌ Higher risk if something breaks
- ❌ Harder to debug issues
- ❌ Large changeset

**Estimated Time:** 15-20 minutes

### Option B: Package-by-Package Migration 🐢
**Approach:** Move packages individually in dependency order

**Order:**
1. log, util, config (minimal dependencies)
2. core (foundational)
3. jdbc (depends on core)
4. loader (depends on jdbc)

**Pros:**
- ✅ Lower risk per step
- ✅ Easier to debug
- ✅ Can test incrementally

**Cons:**
- ❌ Very slow (4+ separate migrations)
- ❌ Complex intermediate states
- ❌ Repeated import updates
- ❌ Multiple compile/test cycles

**Estimated Time:** 1-2 hours

## Recommendation: **Option A - Big Bang Migration** ⚡

**Rationale:**
1. We've already successfully done 9 package migrations - proven track record
2. The internal packages are well-understood
3. Automated regex can handle most import updates
4. Single test cycle is more efficient
5. No customer-facing risk (all internal code)

## Migration Command Structure

```bash
# 1. Create internal directory structure
mkdir -p src/main/java/net/snowflake/client/internal/{config,core,jdbc,loader,log,util}
mkdir -p src/test/java/net/snowflake/client/internal/{config,core,jdbc,loader,log,util}

# 2. Move packages
mv src/main/java/net/snowflake/client/config/* src/main/java/net/snowflake/client/internal/config/
mv src/main/java/net/snowflake/client/core/* src/main/java/net/snowflake/client/internal/core/
# ... etc

# 3. Update package declarations
find src -name "*.java" -path "*/internal/*" -exec sed -i '' 's/package net.snowflake.client.config/package net.snowflake.client.internal.config/g' {} +
# ... etc

# 4. Update imports
find src -name "*.java" -exec sed -i '' 's/import net.snowflake.client.config/import net.snowflake.client.internal.config/g' {} +
# ... etc

# 5. Compile and test
./mvnw clean test
```

## Ready to Proceed?

Awaiting approval to execute **Option A: Big Bang Migration**.
