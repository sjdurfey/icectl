# icectl Configuration Guide

This guide covers all configuration options for icectl.

## Configuration File Locations

icectl searches for configuration files in this order:

1. **`ICECTL_CONFIG`** environment variable (explicit path)
2. **`$XDG_CONFIG_HOME/icectl/config.yaml`** (user config)
3. **`$XDG_CONFIG_DIRS/icectl/config.yaml`** (system config)
4. **`~/.config/icectl/config.yaml`** (fallback if XDG not set)

### Setting Configuration Path

```bash
# Explicit path (recommended for development)
export ICECTL_CONFIG=/path/to/your/config.yaml

# Use project config
export ICECTL_CONFIG=$(pwd)/infra/icectl.config.yaml

# Create user config directory
mkdir -p ~/.config/icectl
cp infra/icectl.config.yaml ~/.config/icectl/config.yaml
```

## Configuration File Format

Configuration files use YAML format with this structure:

```yaml
version: 1                    # Config format version
default_catalog: prod         # Default catalog name

catalogs:                     # Catalog definitions
  catalog_name:
    type: rest                # Catalog type
    uri: http://...           # Catalog URI
    warehouse: name           # Warehouse identifier
    fs: {...}                 # Filesystem config
    sample_engine: {...}      # Sampling engine config
    overrides: {...}          # PyIceberg property overrides
```

## Catalog Types

### REST Catalogs

For Iceberg REST catalog implementations (Lakekeeper, Tabular, etc.):

```yaml
catalogs:
  prod:
    type: rest
    uri: http://localhost:8181/catalog/
    warehouse: project-id/warehouse-name    # For Lakekeeper
    # warehouse: s3://bucket/path/          # For other REST catalogs
```

### Hive Catalogs

For Hive Metastore catalogs:

```yaml
catalogs:
  hive_prod:
    type: hive
    uri: thrift://metastore-host:9083
    warehouse: s3://bucket/warehouse/
```

### AWS Glue Catalogs

For AWS Glue Data Catalog:

```yaml
catalogs:
  glue_prod:
    type: glue
    warehouse: s3://bucket/warehouse/
```

## Configuration Sections

### Global Settings

```yaml
version: 1                    # Required: config format version
default_catalog: prod         # Optional: default catalog for commands
```

### Catalog Configuration

#### Basic Catalog Properties

```yaml
catalogs:
  prod:
    type: rest                # Required: rest, hive, glue, etc.
    uri: http://host:port/    # Required for REST/Hive catalogs
    warehouse: identifier     # Required: warehouse identifier
```

#### Filesystem Configuration

Configure S3/MinIO access:

```yaml
catalogs:
  prod:
    fs:
      endpoint_url: http://localhost:9000    # S3-compatible endpoint
      region: us-east-1                      # AWS region
      profile: null                          # AWS profile name
      path_style_access: true                # Use path-style URLs (MinIO)
```

#### Sample Engine Configuration

Configure how table sampling works:

```yaml
catalogs:
  prod:
    sample_engine:
      type: local             # Required: local, spark, duckdb
      # Additional engine-specific options go here
```

#### PyIceberg Overrides

Pass properties directly to PyIceberg:

```yaml
catalogs:
  prod:
    overrides:
      # S3 configuration
      s3.endpoint: http://localhost:9000
      s3.region: us-east-1
      s3.path-style-access: true
      s3.access-key-id: minioadmin
      s3.secret-access-key: minioadmin
      
      # Warehouse configuration
      warehouse: s3://bucket/path
      
      # Other PyIceberg properties
      catalog-impl: org.apache.iceberg.rest.RESTCatalog
```

## Complete Configuration Examples

### Local Development (MinIO + Lakekeeper)

```yaml
version: 1
default_catalog: prod

catalogs:
  prod:
    type: rest
    uri: http://localhost:8181/catalog/
    warehouse: 0198dd96-5051-7fa1-8ac1-0ad9f06de396/prod
    fs:
      endpoint_url: http://localhost:9000
      region: us-east-1
      profile: null
    sample_engine:
      type: local
    overrides:
      s3.endpoint: http://localhost:9000
      s3.path-style-access: true
      s3.region: us-east-1
      s3.access-key-id: minioadmin
      s3.secret-access-key: minioadmin
```

### AWS Production

```yaml
version: 1
default_catalog: prod

catalogs:
  prod:
    type: rest
    uri: https://api.tabular.io/ws
    warehouse: s3://my-iceberg-warehouse/
    fs:
      region: us-west-2
      profile: production
    sample_engine:
      type: local
    overrides:
      s3.region: us-west-2
      
  glue:
    type: glue
    warehouse: s3://my-glue-warehouse/
    fs:
      region: us-west-2
      profile: production
```

### Multi-Environment

```yaml
version: 1
default_catalog: dev

catalogs:
  dev:
    type: rest
    uri: http://localhost:8181/catalog/
    warehouse: dev-project/warehouse
    fs:
      endpoint_url: http://localhost:9000
      region: us-east-1
    overrides:
      s3.endpoint: http://localhost:9000
      s3.path-style-access: true
      s3.access-key-id: ${MINIO_DEV_USER}
      s3.secret-access-key: ${MINIO_DEV_PASSWORD}
      
  staging:
    type: rest
    uri: https://staging-catalog.company.com/catalog/
    warehouse: s3://staging-warehouse/
    fs:
      region: us-east-1
      profile: staging
      
  prod:
    type: rest  
    uri: https://prod-catalog.company.com/catalog/
    warehouse: s3://prod-warehouse/
    fs:
      region: us-west-2
      profile: production
```

## Environment Variables

### Configuration File

- **`ICECTL_CONFIG`**: Path to configuration file

### Credentials (used in config via `${VAR}` syntax)

#### MinIO/Local Development
- **`MINIO_ROOT_USER`**: MinIO username (default: minioadmin)
- **`MINIO_ROOT_PASSWORD`**: MinIO password (default: minioadmin)

#### AWS
- **`AWS_PROFILE`**: AWS profile name
- **`AWS_ACCESS_KEY_ID`**: AWS access key
- **`AWS_SECRET_ACCESS_KEY`**: AWS secret key
- **`AWS_SESSION_TOKEN`**: AWS session token (for temporary credentials)
- **`AWS_REGION`**: Default AWS region

#### Other
- **`PYICEBERG_LOG_LEVEL`**: PyIceberg logging level (DEBUG, INFO, WARN, ERROR)

### Example with Environment Variables

```yaml
version: 1
default_catalog: ${ICECTL_DEFAULT_CATALOG:-prod}

catalogs:
  prod:
    type: rest
    uri: ${CATALOG_URI}
    warehouse: ${WAREHOUSE_NAME}
    overrides:
      s3.access-key-id: ${AWS_ACCESS_KEY_ID}
      s3.secret-access-key: ${AWS_SECRET_ACCESS_KEY}
      s3.region: ${AWS_REGION:-us-east-1}
```

## Configuration Properties Reference

### Top-Level Properties

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `version` | integer | ✅ | Configuration format version (use `1`) |
| `default_catalog` | string | ❌ | Default catalog name for commands |
| `catalogs` | object | ✅ | Catalog definitions |

### Catalog Properties

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `type` | string | ✅ | Catalog type: `rest`, `hive`, `glue` |
| `uri` | string | ✅* | Catalog URI (*required for REST/Hive) |
| `warehouse` | string | ✅ | Warehouse identifier or S3 path |
| `fs` | object | ❌ | Filesystem configuration |
| `sample_engine` | object | ❌ | Sampling engine configuration |
| `overrides` | object | ❌ | PyIceberg property overrides |

### Filesystem Properties (`fs`)

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `endpoint_url` | string | ❌ | S3-compatible endpoint URL |
| `region` | string | ❌ | AWS region |
| `profile` | string | ❌ | AWS profile name (`null` for default) |
| `path_style_access` | boolean | ❌ | Use path-style URLs (true for MinIO) |

### Sample Engine Properties (`sample_engine`)

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `type` | string | ✅ | Engine type: `local`, `spark`, `duckdb` |

### Override Properties (`overrides`)

Any PyIceberg configuration property can be specified in `overrides`. Common ones:

| Property | Type | Description |
|----------|------|-------------|
| `warehouse` | string | Warehouse location (overrides top-level) |
| `s3.endpoint` | string | S3 endpoint URL |
| `s3.region` | string | S3 region |
| `s3.path-style-access` | boolean | Use path-style S3 URLs |
| `s3.access-key-id` | string | S3 access key |
| `s3.secret-access-key` | string | S3 secret key |
| `catalog-impl` | string | Custom catalog implementation class |

## Security Considerations

### Credential Management

**❌ Don't do this:**
```yaml
overrides:
  s3.access-key-id: AKIAIOSFODNN7EXAMPLE    # Hardcoded credentials
  s3.secret-access-key: wJalrXUtnFEMI/...   # Visible in config
```

**✅ Do this instead:**
```yaml
overrides:
  s3.access-key-id: ${AWS_ACCESS_KEY_ID}     # From environment
  s3.secret-access-key: ${AWS_SECRET_ACCESS_KEY}
```

### File Permissions

Set restrictive permissions on configuration files containing credentials:

```bash
chmod 600 ~/.config/icectl/config.yaml
```

### AWS IAM Policies

Example minimal IAM policy for Iceberg access:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-warehouse-bucket",
        "arn:aws:s3:::your-warehouse-bucket/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:GetPartitions"
      ],
      "Resource": "*"
    }
  ]
}
```

## Validation and Testing

### Validate Configuration

```bash
# Test basic configuration loading
icectl catalogs list

# Test catalog connectivity
icectl catalogs show --catalog prod

# Test with verbose output
icectl -v catalogs show --catalog prod
```

### Debug Configuration Issues

```bash
# Check which config file is being used
icectl -v catalogs list 2>&1 | grep "Loaded config from"

# Test specific catalog
icectl catalogs show --catalog prod

# Test PyIceberg properties
export PYICEBERG_LOG_LEVEL=DEBUG
icectl -v db list --catalog prod
```

## Migration Guide

### From Older Versions

If you have an older configuration format, update to version 1:

```yaml
# Old format (deprecated)
catalogs:
  prod:
    rest_uri: http://localhost:8181/catalog/
    
# New format (version 1)
version: 1
catalogs:
  prod:
    type: rest
    uri: http://localhost:8181/catalog/
```

### Lakekeeper Warehouse Format

Lakekeeper requires `project-id/warehouse-name` format:

```yaml
# Get project ID from Lakekeeper or seeder output
warehouse: 0198dd96-5051-7fa1-8ac1-0ad9f06de396/prod
```

## Best Practices

1. **Use environment variables** for credentials and environment-specific values
2. **Set restrictive file permissions** on config files (600)
3. **Use different catalogs** for different environments (dev/staging/prod)
4. **Test configuration changes** with `icectl catalogs list`
5. **Use version control** for config templates (without secrets)
6. **Document catalog purposes** with comments in config files