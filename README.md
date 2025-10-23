# Client Configuration Deployment Tool

A Python-based tool for deploying client configurations and download rules to AWS infrastructure. This tool manages client configurations in AWS Systems Manager Parameter Store and download rules in DynamoDB.

## Overview

This tool automates the deployment of:
- Client configurations to AWS SSM Parameter Store
- Environment-specific configurations and secrets
- Download rules to DynamoDB tables
- Pipeline-specific rule filtering

## Features

- **Dual Input Options**: Use `--input` for shorthand or `--input-dir` for full paths
- **Dry-Run Mode**: Test deployments without making changes
- **Pipeline-Specific Rules**: Download rules are filtered based on pipeline associations
- **AWS Integration**: Seamless integration with SSM Parameter Store and DynamoDB
- **ID Generation**: Automatic generation of unique IDs for clients, environments, pipelines, and connections
- **Secret Management**: Secure handling of passwords and API keys through AWS SSM

## Prerequisites

- Python 3.7+
- AWS CLI configured with appropriate credentials
- Required Python packages:
  - boto3
  - (install with: `pip install boto3`)

## Project Structure

```
.
├── scripts/
│   ├── deploy_master.py           # Main deployment orchestrator
│   ├── client_config_maker.py     # SSM Parameter Store configuration
│   └── insert_download_rules.py   # DynamoDB rules insertion
├── input/
│   ├── example/                   # Example configuration (included)
│   │   ├── client.json
│   │   ├── environments.json
│   │   └── download_rules.json
│   └── [your-client]/            # Your client configs (gitignored)
├── .dev/
│   └── dry_run_output/           # Dry-run outputs (gitignored)
└── README.md
```

## Configuration Files

### 1. client.json

Defines the client's basic information:

```json
{
  "name": "Your Client Name",
  "tag": "clienttag"
}
```

### 2. environments.json

Defines environments, connections, and pipelines:

```json
{
  "prod": {
    "name": "Production Environment",
    "tag": "prod",
    "connections": {
      "cc_web": {
        "name": "ConstructConnect Web",
        "type": "constructconnect_web",
        "username": "user@example.com",
        "password": "secret.cc_password",
        "login_url": "https://login.cmdgroup.com/Account/Login"
      }
    },
    "pipelines": {
      "cc_pipeline": {
        "name": "ConstructConnect Pipeline",
        "connections": {
          "file_source": "cc_web",
          "record_source": "cc_ftp",
          "target": "salesforce"
        }
      }
    },
    "secret": {
      "cc_password": "actual_password_here"
    }
  }
}
```

### 3. download_rules.json

Defines download rules with pipeline associations:

```json
[
  {
    "description": "Specs: Division 01",
    "type": "division",
    "values": "01",
    "pipeline": "cc_pipeline"
  }
]
```

**Important**: Each rule must include a `pipeline` property that matches a pipeline key from `environments.json`.

## Usage

### Quick Start

1. Create your configuration directory in `input/`:
   ```bash
   mkdir input/yourclient
   ```

2. Add your configuration files:
   - `client.json`
   - `environments.json`
   - `download_rules.json`

3. Run a dry-run to verify:
   ```bash
   python scripts/deploy_master.py --input yourclient --dry-run
   ```

4. Deploy to AWS:
   ```bash
   python scripts/deploy_master.py --input yourclient
   ```

### Command-Line Options

#### Main Deployment Script (deploy_master.py)

```bash
python scripts/deploy_master.py [OPTIONS]
```

**Input Options** (one required):
- `--input NAME` - Name of subfolder in `./input` directory
- `--input-dir PATH` - Full path to directory containing configuration files

**Other Options**:
- `--table-name NAME` - DynamoDB table name (default: `spec-download-rule`)
- `--region REGION` - AWS region (default: `us-east-1`)
- `--dry-run` - Show what would be done without making changes
- `--skip-rules` - Skip download rules insertion (only create client config)

**Examples**:

```bash
# Using --input (shorthand)
python scripts/deploy_master.py --input yourclient --dry-run

# Using --input-dir (full path)
python scripts/deploy_master.py --input-dir input/yourclient --dry-run

# Deploy to different region
python scripts/deploy_master.py --input yourclient --region us-west-2

# Skip download rules insertion
python scripts/deploy_master.py --input yourclient --skip-rules
```

### Individual Scripts

#### Client Configuration Maker

```bash
python scripts/client_config_maker.py --input-dir input/yourclient [OPTIONS]
```

#### Download Rules Insertion

```bash
python scripts/insert_download_rules.py \
  --env-id ENV_ID \
  --client-id CLIENT_ID \
  --pipeline-id PIPELINE_ID \
  --input-dir input/yourclient \
  [OPTIONS]
```

## Dry-Run Mode

Dry-run mode generates all configurations without making AWS changes:

```bash
python scripts/deploy_master.py --input yourclient --dry-run
```

**Outputs** are saved to:
```
.dev/dry_run_output/YYYYMMDD_HHMMSS/
├── client_config.json
├── environment_prod_config.json
├── environment_prod_secrets.json
└── rules_[pipeline-id].json
```

Review these files before running the actual deployment.

## Pipeline-Specific Download Rules

The tool filters download rules based on the `pipeline` property in each rule. This ensures rules are only created for their associated pipeline, preventing duplication across all pipelines.

**Example**:

If you have two pipelines (`cc_pipeline` and `dodge_pipeline`) and 5 rules with `"pipeline": "cc_pipeline"`:
- **cc_pipeline**: Gets all 5 rules
- **dodge_pipeline**: Gets 0 rules

## AWS Resources Created

### SSM Parameter Store

```
/spec/enrichment/clients/{client-id}/config
/spec/enrichment/clients/{client-id}/envs/{env-id}/config
/spec/enrichment/clients/{client-id}/envs/{env-id}/secrets/{secret-name}
```

### DynamoDB

Records created in the specified table (default: `spec-download-rule`) with:
- `rule_id` (primary key)
- `env_id`
- `client_id`
- `pipeline_id`
- `description`
- `type`
- `values`

## ID Generation

The tool automatically generates unique IDs using the format:
- **Client ID**: `{tag}-cid-{hash}`
- **Environment ID**: `{tag}-{env-tag}-{hash}`
- **Pipeline ID**: `{tag}-pipe-{hash}`
- **Connection ID**: `{tag}-con-{hash}`

## Error Handling

The tool validates:
- Configuration file existence and format
- Required fields in JSON files
- AWS connectivity before operations
- DynamoDB table existence
- Pipeline-rule associations

## Security Notes

- **Secrets**: Store actual passwords in the `secret` section of `environments.json`
- **Gitignore**: Client-specific input directories are gitignored by default
- **AWS IAM**: Ensure AWS credentials have appropriate permissions:
  - `ssm:PutParameter` for Parameter Store
  - `ssm:DescribeParameters` for validation
  - `dynamodb:PutItem` for rules insertion
  - `dynamodb:DescribeTable` for validation

## Troubleshooting

### "Configuration directory does not exist"
Ensure your directory path is correct. Use `--input` for folders in `./input/` or `--input-dir` for full paths.

### "No rules found for pipeline"
Verify the `pipeline` property in your download rules matches a pipeline key in `environments.json`.

### "Unable to connect to AWS"
Check your AWS credentials are configured:
```bash
aws configure
```

### "DynamoDB table does not exist"
Verify the table name with:
```bash
aws dynamodb list-tables
```

## Contributing

When adding new features:
1. Test with `--dry-run` mode first
2. Update this README if adding new configuration options
3. Maintain backward compatibility where possible

## Example Workflow

```bash
# 1. Create new client configuration
mkdir input/newclient
cp -r input/example/* input/newclient/

# 2. Edit configuration files
# (Update client.json, environments.json, download_rules.json)

# 3. Test with dry-run
python scripts/deploy_master.py --input newclient --dry-run

# 4. Review output in .dev/dry_run_output/

# 5. Deploy to AWS
python scripts/deploy_master.py --input newclient

# 6. Verify deployment
aws ssm get-parameter --name /spec/enrichment/clients/{client-id}/config
```

## License

(Add your license information here)

## Support

For issues or questions, contact: (Add contact information)
