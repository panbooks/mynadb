# Migration Guide: Schema Configuration

## Overview

This guide helps you migrate to the new configurable schema extraction feature introduced in version 0.2.0.

## Breaking Changes

**✅ None!** This release is fully backward compatible.

If you don't add the `[schema]` section to your config, the system will use the default behavior which matches the previous version.

## What's New

You can now customize how schemas are extracted from metric tags:

- **Custom tag keys**: Use any tag (not just "service")
- **Hierarchical extraction**: Handle `MONITOR.service` → `service`
- **Configurable defaults**: Set your own default schema name
- **Flexible separators**: Use `.`, `/`, or any separator

## Migration Scenarios

### Scenario 1: No Changes Needed (Recommended for Most Users)

**If you're happy with current behavior**, do nothing! The default config maintains compatibility:

```toml
# No [schema] section needed - these are the defaults:
# tag_key = "service"
# default_name = "default"
# prefix_numeric = true
# extract_last_part = false
```

Your existing data and queries work exactly as before.

### Scenario 2: Enable Hierarchical Extraction

**If you have hierarchical service tags** like `MONITOR.web` or `PROD.api`:

1. **Add to config.toml:**
```toml
[schema]
extract_last_part = true
```

2. **Restart the service:**
```bash
./stats
```

3. **Result:**
   - New data: `service="MONITOR.web"` → schema `web`
   - Old data: Remains in `MONITOR.web` schema
   - Both accessible via queries

4. **Optional - Consolidate old data:**
```bash
# After verifying new data works, you can manually move old data or just let it phase out
# Old compaction commands still work
curl -X POST http://localhost:7000/compact/benchmark/MONITOR.web/cpu
# New compaction commands
curl -X POST http://localhost:7000/compact/benchmark/web/cpu
```

### Scenario 3: Switch to Custom Tag Key

**If you need to use a different tag** like `hierarchy_2`:

1. **Update config.toml:**
```toml
[schema]
tag_key = "hierarchy_2"
```

2. **Ensure your metrics include this tag:**
```bash
curl -X POST http://localhost:7000/write/test \
  -d 'cpu,hierarchy_2=my_service,host=server1 value=0.5'
```

3. **Restart and verify:**
```bash
./stats
curl http://localhost:7000/databases/test/schemas
```

### Scenario 4: Customize for Your Organization

**If you need full customization:**

1. **Add comprehensive config:**
```toml
[schema]
tag_key = "app_name"              # Your custom tag
default_name = "uncategorized"    # Your default name
prefix_numeric = true             # Keep numeric prefix
extract_last_part = true          # Extract from hierarchical
separator = "/"                   # Your separator
```

2. **Test with sample data:**
```bash
curl -X POST http://localhost:7000/write/test \
  -d 'cpu,app_name=prod/web,host=server1 value=0.5'

# Should create: data/test/web/cpu/
```

3. **Verify schema extraction:**
```bash
curl http://localhost:7000/databases/test/schemas
# Should show: ["web"]
```

## Migration Checklist

- [ ] Review your current metric tag structure
- [ ] Decide if you need schema configuration
- [ ] Backup your config.toml (if making changes)
- [ ] Add [schema] section (if needed)
- [ ] Test with sample data
- [ ] Restart the service
- [ ] Verify schemas are extracted correctly
- [ ] Update monitoring/alerting if schema names changed
- [ ] Update documentation for your team

## Testing Your Migration

### Before Migration

```bash
# 1. Check current schemas
curl http://localhost:7000/databases/YOUR_DB/schemas

# 2. Sample query
curl "http://localhost:7000/query/YOUR_DB?sql=SELECT+*+FROM+current_schema.table+LIMIT+1"

# 3. Note data locations
ls -la ./data/YOUR_DB/
```

### After Migration

```bash
# 1. Send test data
curl -X POST http://localhost:7000/write/test \
  -d 'cpu,service=web,host=test value=0.5'

# 2. Verify new schema
curl http://localhost:7000/databases/test/schemas

# 3. Check data directory
ls -la ./data/test/

# 4. Query new data
curl "http://localhost:7000/query/test?sql=SELECT+*+FROM+web.cpu+LIMIT+10"
```

## Rollback Plan

If you need to rollback:

1. **Stop the service:**
```bash
# Find and stop the process
pkill stats
```

2. **Revert config.toml:**
```bash
# Remove [schema] section or restore from backup
cp config.toml.backup config.toml
```

3. **Restart:**
```bash
./stats
```

**Note:** Data written during the new configuration remains in its directories but is still accessible.

## Common Migration Questions

### Q: Will my existing data be affected?

**A:** No. Existing data remains in its current location. Only new data uses the new schema extraction rules.

### Q: Can I query old and new data together?

**A:** Yes, but you'll need separate queries:
```sql
-- Old schema
SELECT * FROM benchmark.MONITOR.web.cpu WHERE ...
UNION ALL
-- New schema (if extracted)
SELECT * FROM benchmark.web.cpu WHERE ...
```

### Q: How do I consolidate data after changing extraction rules?

**A:** You have two options:
1. Let old data age out naturally (recommended)
2. Manually move/compact data (advanced users only)

### Q: What happens if I change tag_key and the new tag is missing?

**A:** Data goes to the default schema (configurable via `default_name`).

### Q: Can I change configuration without downtime?

**A:** No, you need to restart the service for config changes to take effect. Plan a maintenance window.

### Q: Will compaction still work with old data?

**A:** Yes! Use the original schema names in compaction commands:
```bash
# Old data
curl -X POST http://localhost:7000/compact/benchmark/MONITOR.web/cpu
# New data
curl -X POST http://localhost:7000/compact/benchmark/web/cpu
```

## Example Migration: Microservices Company

**Before:** All services use tag `service` with values like `payment`, `auth`, `notification`

**Goal:** Keep same behavior but add support for hierarchical tags in the future

**Migration:**

1. **Add to config.toml (explicit defaults for clarity):**
```toml
[schema]
tag_key = "service"
default_name = "default"
prefix_numeric = true
extract_last_part = false
separator = "."
```

2. **Test:**
```bash
# Should work exactly as before
curl -X POST http://localhost:7000/write/prod \
  -d 'requests,service=payment,endpoint=/checkout value=150'

curl http://localhost:7000/databases/prod/schemas
# Shows: ["payment"]
```

3. **Later, enable hierarchical extraction:**
```toml
[schema]
extract_last_part = true
```

4. **New metrics with environment prefixes:**
```bash
curl -X POST http://localhost:7000/write/prod \
  -d 'requests,service=PROD.payment,endpoint=/checkout value=150'
# Creates: data/prod/payment/requests/
```

## Support and Help

- 📖 **Full Documentation:** [SCHEMA_EXTRACTION.md](SCHEMA_EXTRACTION.md)
- ⚡ **Quick Reference:** [SCHEMA_QUICK_REF.md](SCHEMA_QUICK_REF.md)
- 🔧 **Configuration Guide:** [CONFIG.md](CONFIG.md)
- 📝 **Change Summary:** [SCHEMA_CONFIG_CHANGES.md](SCHEMA_CONFIG_CHANGES.md)

## Best Practices for Migration

1. **Test in Non-Production First:** Always test configuration changes in a dev/staging environment
2. **Document Your Schema Strategy:** Keep a record of your schema organization
3. **Monitor After Changes:** Watch for data going to unexpected schemas
4. **Start Conservative:** Begin with defaults, then customize as needed
5. **Coordinate with Team:** Ensure all stakeholders understand the new schema structure

## Conclusion

The schema configuration feature is designed to be non-disruptive. Most users won't need to change anything. If you do need customization, the migration is straightforward with minimal risk.
