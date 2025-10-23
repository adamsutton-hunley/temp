#!/usr/bin/env python3
"""
insert_download_rules.py
Modified to support being called from other scripts
"""

import json
import sys
import argparse
import uuid
import boto3
from pathlib import Path
from typing import Dict, Any, List
from botocore.exceptions import ClientError

def generate_rule_id(guid: str, rule_num: int) -> str:
    """Generate rule ID using GUID and incrementor."""
    return f"{guid}#rule_{rule_num}"

def insert_item_to_dynamodb(dynamodb_client, table_name: str, item: Dict[str, Any]) -> bool:
    """Insert item into DynamoDB table."""
    try:
        response = dynamodb_client.put_item(
            TableName=table_name,
            Item=item
        )
        print(f"Successfully inserted rule: {item['rule_id']['S']}")
        return True
        
    except ClientError as e:
        print(f"Error inserting item {item.get('rule_id', {}).get('S', 'unknown')}: {e.response['Error']['Message']}")
        return False
    except Exception as e:
        print(f"Unexpected error inserting item: {str(e)}")
        return False

def convert_to_dynamodb_item(rule_data: Dict[str, Any], rule_id: str, env_id: str, client_id: str, pipeline_id: str) -> Dict[str, Any]:
    """Convert rule data to DynamoDB item format."""
    return {
        'rule_id': {'S': rule_id},
        'env_id': {'S': env_id},
        'client_id': {'S': client_id},
        'pipeline_id': {'S': pipeline_id},
        'description': {'S': rule_data.get('description', '')},
        'type': {'S': rule_data.get('type', '')},
        'values': {'S': rule_data.get('values', '')}
    }

def validate_rule_data(rule_data: Dict[str, Any], index: int) -> bool:
    """Validate rule data structure."""
    required_fields = ['description', 'type', 'values']
    
    for field in required_fields:
        if field not in rule_data:
            print(f"Error: Rule {index + 1} is missing required field '{field}'")
            return False
    
    # Validate data types
    if not isinstance(rule_data['description'], str):
        print(f"Error: Rule {index + 1} 'description' must be a string")
        return False
    
    if not isinstance(rule_data['type'], str):
        print(f"Error: Rule {index + 1} 'type' must be a string")
        return False
    
    if not isinstance(rule_data['values'], str):
        print(f"Error: Rule {index + 1} 'values' must be a string")
        return False
    
    return True

def insert_download_rules(env_id: str, client_id: str, pipeline_id: str, input_dir: str, table_name: str, region: str, dry_run: bool = False, dry_run_output_dir: Path = None, pipeline_key: str = None) -> Dict[str, Any]:
    """Insert download rules and return results."""
    input_dir = Path(input_dir)
    rules_file = input_dir / "download_rules.json"

    # Verify rules file exists
    if not rules_file.exists():
        return {"success": False, "error": f"Rules file '{rules_file}' does not exist"}

    try:
        # Initialize AWS DynamoDB client
        if not dry_run:
            dynamodb_client = boto3.client('dynamodb', region_name=region)

            # Test AWS connectivity and table existence
            try:
                dynamodb_client.describe_table(TableName=table_name)
                print(f"Successfully connected to DynamoDB table: {table_name}")
            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceNotFoundException':
                    return {"success": False, "error": f"DynamoDB table '{table_name}' does not exist"}
                else:
                    raise

    except Exception as e:
        return {"success": False, "error": f"Unable to connect to AWS DynamoDB: {str(e)}"}

    try:
        # Read rules from JSON file
        print("Reading rules from JSON file...")

        with open(rules_file, 'r') as f:
            rules_data = json.load(f)

        if not isinstance(rules_data, list):
            return {"success": False, "error": "Rules file must contain a JSON array"}

        if len(rules_data) == 0:
            return {"success": True, "total_rules": 0, "successful_inserts": 0, "failed_inserts": 0, "rule_guid": None}

        print(f"Found {len(rules_data)} total rules in file")

        # Filter rules by pipeline_key if provided
        if pipeline_key:
            filtered_rules = [rule for rule in rules_data if rule.get('pipeline') == pipeline_key]
            print(f"Filtered to {len(filtered_rules)} rules for pipeline '{pipeline_key}'")
            rules_data = filtered_rules

            if len(rules_data) == 0:
                print(f"No rules found for pipeline '{pipeline_key}'")
                return {"success": True, "total_rules": 0, "successful_inserts": 0, "failed_inserts": 0, "rule_guid": None}
        else:
            print(f"No pipeline filter applied - processing all {len(rules_data)} rules")
        
        # Validate all rules before processing
        for i, rule_data in enumerate(rules_data):
            if not isinstance(rule_data, dict):
                return {"success": False, "error": f"Rule {i + 1} must be a JSON object"}
            
            if not validate_rule_data(rule_data, i):
                return {"success": False, "error": f"Rule {i + 1} validation failed"}
        
        # Generate a single GUID for all rules
        rule_guid = str(uuid.uuid4())
        print(f"Generated GUID for rules: {rule_guid}")
        
        # Process rules
        successful_inserts = 0
        failed_inserts = 0
        dry_run_items = []

        for i, rule_data in enumerate(rules_data):
            rule_num = i + 1
            rule_id = generate_rule_id(rule_guid, rule_num)

            # Convert to DynamoDB item format
            dynamodb_item = convert_to_dynamodb_item(
                rule_data,
                rule_id,
                env_id,
                client_id,
                pipeline_id
            )

            if dry_run:
                print(f"Would insert rule {rule_num}: {rule_id}")
                print(f"  Description: {rule_data['description']}")
                print(f"  Type: {rule_data['type']}")
                print(f"  Values: {rule_data['values']}")
                print()

                # Convert DynamoDB format to readable JSON for dry-run output
                readable_item = {
                    'rule_id': dynamodb_item['rule_id']['S'],
                    'env_id': dynamodb_item['env_id']['S'],
                    'client_id': dynamodb_item['client_id']['S'],
                    'pipeline_id': dynamodb_item['pipeline_id']['S'],
                    'description': dynamodb_item['description']['S'],
                    'type': dynamodb_item['type']['S'],
                    'values': dynamodb_item['values']['S']
                }
                dry_run_items.append(readable_item)
                successful_inserts += 1
            else:
                # Insert into DynamoDB
                if insert_item_to_dynamodb(dynamodb_client, table_name, dynamodb_item):
                    successful_inserts += 1
                else:
                    failed_inserts += 1

        # Save dry-run items to file
        if dry_run and dry_run_items and dry_run_output_dir:
            # Create a safe filename from the pipeline_id
            safe_pipeline_id = pipeline_id.replace('/', '_').replace('\\', '_')
            rules_output_file = dry_run_output_dir / f"rules_{safe_pipeline_id}.json"
            with open(rules_output_file, 'w') as f:
                json.dump(dry_run_items, f, indent=2)
            print(f"Dry-run rules saved to: {rules_output_file}\n")
        
        return {
            "success": True,
            "total_rules": len(rules_data),
            "successful_inserts": successful_inserts,
            "failed_inserts": failed_inserts,
            "rule_guid": rule_guid
        }
        
    except FileNotFoundError:
        return {"success": False, "error": f"Rules file '{rules_file}' not found"}
    except json.JSONDecodeError as e:
        return {"success": False, "error": f"Invalid JSON in rules file: {str(e)}"}
    except Exception as e:
        return {"success": False, "error": f"Error during rule insertion: {str(e)}"}

def main():
    parser = argparse.ArgumentParser(description="Insert download rules into DynamoDB table")
    parser.add_argument("--env-id", required=True, help="Environment ID")
    parser.add_argument("--client-id", required=True, help="Client ID")
    parser.add_argument("--pipeline-id", required=True, help="Pipeline ID")
    parser.add_argument("--input-dir", required=True, help="Directory containing configuration files (expects download_rules.json)")
    parser.add_argument("--table-name", default="spec-download-rule", help="DynamoDB table name (default: spec-download-rule)")
    parser.add_argument("--region", default="us-east-1", help="AWS region (default: us-east-1)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be inserted without actually inserting")

    args = parser.parse_args()

    print(f"Starting DynamoDB rule insertion...")
    print(f"Table: {args.table_name}")
    print(f"Environment ID: {args.env_id}")
    print(f"Client ID: {args.client_id}")
    print(f"Pipeline ID: {args.pipeline_id}")
    print(f"Input Directory: {args.input_dir}")
    print(f"Region: {args.region}")

    if args.dry_run:
        print("DRY RUN MODE: No items will be inserted")

    result = insert_download_rules(
        env_id=args.env_id,
        client_id=args.client_id,
        pipeline_id=args.pipeline_id,
        input_dir=args.input_dir,
        table_name=args.table_name,
        region=args.region,
        dry_run=args.dry_run
    )
    
    if result["success"]:
        # Output summary
        print("\n=== INSERTION SUMMARY ===")
        print(f"Total rules processed: {result['total_rules']}")
        print(f"Successful inserts: {result['successful_inserts']}")
        if not args.dry_run:
            print(f"Failed inserts: {result['failed_inserts']}")
        print(f"Rule GUID: {result['rule_guid']}")
        print(f"Environment ID: {args.env_id}")
        print(f"Client ID: {args.client_id}")
        print(f"Pipeline ID: {args.pipeline_id}")
        
        if result['failed_inserts'] > 0 and not args.dry_run:
            print(f"\nWarning: {result['failed_inserts']} rules failed to insert")
            sys.exit(1)
        
        print("\nScript completed successfully.")
    else:
        print(f"Error: {result['error']}")
        sys.exit(1)

if __name__ == "__main__":
    main()