# 📋 Internal Package Migration Plan

## Goal
Move all internal implementation classes from `net.snowflake.client.*` to `net.snowflake.client.internal.*`

## Current State
```
net.snowflake.client/
├── api/              ✅ PUBLIC API (already migrated)
│   ├── exception/
│   ├── connection/
│   ├── statement/
│   ├── resultset/
│   ├── metadata/
│   ├── driver/
│   ├── datasource/
│   ├── pooling/
│   └── loader/
├── jdbc/             ❌ INTERNAL (needs migration)
├── core/             ❌ INTERNAL (needs migration)
├── loader/           ❌ INTERNAL (needs migration)
├── log/              ❌ INTERNAL (needs migration)
└── [other packages]  ❌ INTERNAL (needs migration)
```

## Target State
```
net.snowflake.client/
├── api/              ✅ PUBLIC API
│   └── [all public packages]
└── internal/         ✅ INTERNAL
    ├── jdbc/
    ├── core/
    ├── loader/
    ├── log/
    └── [other packages]
```

## Packages to Migrate

### Phase 1: Identify all packages
- [ ] List all packages under `net.snowflake.client.*` (excluding `api`)
- [ ] Count files in each package
- [ ] Identify dependencies between packages

### Phase 2: Migration Strategy
**Option A: Big Bang Migration**
- Move all packages at once
- Update all imports in one go
- Single test run

**Option B: Package-by-Package Migration**
- Move one package at a time
- Test after each migration
- More controlled but slower

### Phase 3: Execution
TBD based on analysis

## Notes
- The `com.snowflake.client.jdbc` package has a deprecated wrapper that should remain for backward compatibility
- Test files should also be organized (move internal tests to corresponding internal test packages)
