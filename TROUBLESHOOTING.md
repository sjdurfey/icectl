# icectl Troubleshooting Guide

This guide provides comprehensive solutions for common issues when using icectl.

## Quick Diagnostics

First, try these commands to identify the issue:

```bash
# Check basic configuration
icectl catalogs list

# Check service connectivity (if using local services)
curl http://localhost:8181/health
docker-compose ps

# Use verbose mode for detailed error information
icectl -v [your-command]
```

## Configuration Issues

### Config File Not Found

**Error:** `No config file found. Set ICECTL_CONFIG or create ~/.config/icectl/config.yaml`

**Solutions:**
1. **Set explicit config path:**
   ```bash
   export ICECTL_CONFIG=/path/to/your/config.yaml
   icectl catalogs list
   ```

2. **Create default config location:**
   ```bash
   mkdir -p ~/.config/icectl
   cp infra/icectl.config.yaml ~/.config/icectl/config.yaml
   ```

3. **Use project config for development:**
   ```bash
   export ICECTL_CONFIG=$(pwd)/infra/icectl.config.yaml
   ```

### Catalog Not Found

**Error:** `Catalog not found: [name]`

**Solutions:**
1. Check catalog name spelling in commands
2. Verify catalog is defined in your config file
3. Use `icectl catalogs list` to see available catalogs

### Wrong Warehouse Format

**Error:** `Warehouse s3://warehouse/ not found`

**Cause:** Lakekeeper expects `project-id/warehouse-name` format, not S3 paths.

**Solution:** Update your config file:
```yaml
catalogs:
  prod:
    warehouse: abc123-def456-789/prod  # Use actual project ID from Lakekeeper
```

## Connection Issues

### Services Not Running

**Error:** Connection timeouts or "connection refused"

**Solutions:**
1. **Start services:**
   ```bash
   make infra-up          # Start services
   make infra-reset       # Full reset and restart
   ```

2. **Check service status:**
   ```bash
   docker-compose ps                    # Container status
   curl http://localhost:8181/health    # Lakekeeper health
   curl http://localhost:9000           # MinIO health
   ```

3. **View logs:**
   ```bash
   make infra-logs                      # All service logs
   docker-compose logs lakekeeper       # Specific service
   ```

### Port Conflicts

**Error:** Services fail to start due to port conflicts

**Solutions:**
1. **Check what's using the ports:**
   ```bash
   lsof -i :8181  # Lakekeeper port
   lsof -i :9000  # MinIO port
   ```

2. **Stop conflicting services or change ports in docker-compose.yaml**

### Network Issues

**Error:** Connection timeouts in Docker

**Solutions:**
1. **Restart Docker:**
   ```bash
   docker system prune -f
   make infra-reset
   ```

2. **Check Docker network:**
   ```bash
   docker network ls
   docker network inspect iceberg-viewer_default
   ```

## Authentication Issues

### MinIO/S3 Credentials

**Error:** Authentication failures or access denied

**Solutions:**
1. **Check credentials in config:**
   ```yaml
   overrides:
     s3.access-key-id: minioadmin
     s3.secret-access-key: minioadmin
   ```

2. **Set environment variables:**
   ```bash
   export MINIO_ROOT_USER=minioadmin
   export MINIO_ROOT_PASSWORD=minioadmin
   ```

3. **For AWS S3, use AWS credentials:**
   ```bash
   export AWS_PROFILE=your-profile
   # or
   export AWS_ACCESS_KEY_ID=your-key
   export AWS_SECRET_ACCESS_KEY=your-secret
   ```

### Warehouse Permissions

**Error:** `Forbidden` or permission denied errors

**Solutions:**
1. **Verify bucket access:**
   ```bash
   # For MinIO (local development)
   docker exec -it infra-minio-1 mc ls local/warehouse
   
   # For AWS S3
   aws s3 ls s3://your-bucket/
   ```

2. **Check IAM policies for AWS S3**
3. **Ensure warehouse bucket exists**

## Data Issues

### Table Not Found

**Error:** `Table 'tablename' not found`

**Solutions:**
1. **List available tables:**
   ```bash
   icectl db list                           # List namespaces
   icectl tables list --db analytics       # List tables in namespace
   ```

2. **Check table name format:**
   - Use `namespace.table` format: `analytics.events`
   - Case-sensitive names

3. **Seed sample data if missing:**
   ```bash
   make infra-seed
   ```

### Empty Results

**Error:** Commands succeed but return no data

**Possible causes and solutions:**
1. **No data in tables:**
   ```bash
   make infra-seed  # Add sample data
   ```

2. **Wrong namespace:**
   ```bash
   icectl db list  # Check available namespaces
   ```

3. **Filters too restrictive:**
   ```bash
   icectl tables sample analytics.events -n 100  # Increase sample size
   ```

### Schema Incompatibilities

**Error:** Schema format or parsing errors

**Solutions:**
1. **Check PyIceberg version compatibility**
2. **Verify table schema:**
   ```bash
   icectl tables describe analytics.events
   ```

3. **Try different table formats or recreate tables**

## Performance Issues

### Slow Queries

**Solutions:**
1. **Reduce sample size:**
   ```bash
   icectl tables sample table.name -n 10  # Instead of default 100
   ```

2. **Use table metadata instead of data:**
   ```bash
   icectl tables describe table.name      # Faster than sampling
   icectl tables schema table.name        # Schema only
   ```

3. **Check service resources:**
   ```bash
   docker stats  # Monitor container resource usage
   ```

### Memory Issues

**Error:** Out of memory errors

**Solutions:**
1. **Limit sample size:**
   ```bash
   icectl tables sample table.name -n 5
   ```

2. **Increase Docker memory limits**
3. **Use JSON output for smaller datasets:**
   ```bash
   icectl --json-output tables sample table.name -n 10 | jq '.'
   ```

## Development Issues

### Installation Problems

**Error:** Installation or import errors

**Solutions:**
1. **Clean installation:**
   ```bash
   make uninstall
   make clean
   make install
   ```

2. **Check Python version:**
   ```bash
   python --version  # Should be 3.12+
   ```

3. **Update dependencies:**
   ```bash
   make deps  # Sync dependencies
   ```

### Testing Issues

**Error:** Tests failing

**Solutions:**
1. **Run specific test types:**
   ```bash
   make test                    # Unit tests only
   make itest                   # Integration tests (requires services)
   ```

2. **Check test environment:**
   ```bash
   make infra-up               # Start test services
   export ICECTL_CONFIG=$(pwd)/infra/icectl.config.yaml
   ```

## Advanced Debugging

### Enable Verbose Logging

Get detailed information about what icectl is doing:

```bash
icectl -v [command]  # CLI verbose mode
```

### PyIceberg Debug Mode

For PyIceberg-specific issues:

```bash
export PYICEBERG_LOG_LEVEL=DEBUG
icectl [command]
```

### Docker Debugging

Check container internals:

```bash
# Access MinIO container
docker exec -it infra-minio-1 bash

# Access Lakekeeper database
docker exec -it infra-db-1 psql -U postgres -d lakekeeper

# Check container logs
docker-compose logs --tail=100 lakekeeper
```

### Network Debugging

Test connectivity between services:

```bash
# From host to services
curl -v http://localhost:8181/health
curl -v http://localhost:9000/minio/health/live

# From inside containers
docker exec infra-lakekeeper-1 curl http://minio:9000/minio/health/live
```

## Getting Additional Help

1. **Check error messages carefully** - they often contain specific guidance
2. **Use `-v/--verbose` flag** for detailed error context and suggestions
3. **Review service logs** with `make infra-logs`
4. **Try `make infra-reset`** to start with a clean state
5. **Check the README.md** for configuration examples
6. **File issues** with detailed error messages and environment info

## Environment Information Template

When reporting issues, include this information:

```bash
# System info
uname -a
python --version
docker --version
docker-compose --version

# Service status
docker-compose ps
curl http://localhost:8181/health
curl http://localhost:9000/minio/health/live

# Configuration (redact secrets!)
echo $ICECTL_CONFIG
icectl catalogs list

# Error with verbose output
icectl -v [failing-command]
```