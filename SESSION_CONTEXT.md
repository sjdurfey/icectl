# Iceberg Viewer Setup - Session Context Export

**Date**: 2026-04-05  
**Original Machine**: Local development machine  
**Target Machine**: Remote homelab machine

## Current Status Summary

### ✅ Completed Successfully
1. **SSL Certificate Fixed**
   - Installed homelab CA: `/Users/stephen/Development/git/sjdurfey/homelab/configs/ssl/ca/ca.crt`
   - Added to both macOS keychain and Python cert bundle
   - Environment variables set in `~/.zshrc`:
     ```bash
     export REQUESTS_CA_BUNDLE="/Users/stephen/Development/git/sjdurfey/homelab/configs/ssl/ca/ca.crt"
     export ICECTL_CONFIG="/Users/stephen/Development/git/iceberg-viewer/infra/icectl.config.yaml"
     ```

2. **Configuration Updated**
   - `infra/icectl.config.yaml` corrected for homelab setup
   - URI: `https://lakekeeper.homelab/catalog/`  
   - Warehouse: `00000000-0000-0000-0000-000000000000/warehouse`
   - Storage endpoint: RustFS on `http://10.0.0.166:9090`
   - Credentials: `rustfsadmin/rustfsadmin123`

3. **CLI/TUI Working**
   - `make tui` and `./icectl.sh` work perfectly
   - All read operations functional
   - Wrapper script created: `icectl.sh`

4. **Baseball Data Script Ready**
   - `scripts/populate.py` created with comprehensive test data
   - Uses uv script dependencies
   - Ready to create 30 teams, 750+ players, 100K+ records

5. **RustFS Storage Verified**
   - boto3 diagnostics confirm full access
   - Both `lakehouse` and `data` buckets accessible
   - External endpoint `10.0.0.166:9090` working perfectly

### ❌ Current Blocker: Warehouse Creation
**Issue**: Lakekeeper cannot authenticate to RustFS for warehouse creation

**Error**: `S3 write failed: Access Denied` when creating warehouse

**Diagnosis**: 
- RustFS buckets work perfectly with direct access
- Lakekeeper credential conflict: trying to use AWS IAM instead of access keys
- Possible internal DNS resolution issue (`rustfs:9000` vs `10.0.0.166:9090`)

## Files Modified/Created

### Modified Files
1. `/Users/stephen/Development/git/iceberg-viewer/infra/icectl.config.yaml`
   - Updated for RustFS and correct homelab endpoints
2. `/Users/stephen/Development/git/iceberg-viewer/Makefile`
   - Fixed targets, added SSL CA bundle to commands  
3. `/Users/stephen/Development/git/iceberg-viewer/scripts/run_integration.sh`
   - Added REQUESTS_CA_BUNDLE environment variable
4. `~/.zshrc`
   - Added permanent environment variables

### New Files Created
1. `/Users/stephen/Development/git/iceberg-viewer/scripts/populate.py`
   - Comprehensive baseball data population script
2. `/Users/stephen/Development/git/iceberg-viewer/icectl.sh`
   - CLI wrapper with SSL configuration
3. `/Users/stephen/Development/git/iceberg-viewer/diagnose_s3.py`
   - boto3 diagnostics script (confirmed RustFS working)

## Troubleshooting Needed on Remote Machine

### Target: Fix Lakekeeper → RustFS Authentication

**Location**: `/Users/stephen/Development/git/sjdurfey/homelab/services/lakekeeper/`

**Problem Configuration** in `docker-compose.yml`:
```yaml
environment:
  # These cause Lakekeeper to use AWS IAM instead of access keys:
  - LAKEKEEPER__ENABLE_AWS_SYSTEM_CREDENTIALS=true
  - LAKEKEEPER__S3_ENABLE_DIRECT_SYSTEM_CREDENTIALS=true
```

**Also check**: `/configs/local/internal/lakekeeper.env`

### Steps to Try
1. **Check container networking**: Can Lakekeeper reach `rustfs:9000`?
2. **Check Lakekeeper logs**: `docker logs lakekeeper`
3. **Disable AWS system credentials** in environment
4. **Test with external endpoint**: `10.0.0.166:9090` instead of `rustfs:9000`
5. **Restart Lakekeeper** after changes

### Test Command After Fix
```bash
curl -X POST -H "Content-Type: application/json" \
  "https://lakekeeper.homelab/management/v1/warehouse" \
  -d '{
    "warehouse-name": "warehouse",
    "project-id": "00000000-0000-0000-0000-000000000000", 
    "storage-profile": {
      "type": "s3",
      "bucket": "lakehouse",
      "endpoint": "http://rustfs:9000",
      "region": "us-east-1",
      "path-style-access": true,
      "flavor": "s3-compat", 
      "sts-enabled": false
    },
    "delete-profile": { "type": "hard" }
  }'
```

## Next Steps After Warehouse Fixed
1. Populate baseball data: `uv run scripts/populate.py`
2. Test CLI: `./icectl.sh tables list --db teams` 
3. Launch TUI: `make tui`
4. Run integration tests: `scripts/run_integration.sh`

## Key Environment Variables for New Session
```bash
export REQUESTS_CA_BUNDLE="/Users/stephen/Development/git/sjdurfey/homelab/configs/ssl/ca/ca.crt"
export ICECTL_CONFIG="infra/icectl.config.yaml"
```

## Critical Files to Have Access To
- `/Users/stephen/Development/git/sjdurfey/homelab/` (entire homelab directory)
- `/Users/stephen/Development/git/iceberg-viewer/` (this project)

## Context for New Claude Session
"I've been working on setting up icectl CLI with homelab Lakekeeper/RustFS. SSL and config are fixed, CLI reads work perfectly, but warehouse creation fails with S3 access denied. RustFS buckets are fully accessible via boto3, so it's a Lakekeeper authentication issue. Need to fix Lakekeeper environment variables to use basic auth instead of AWS IAM."