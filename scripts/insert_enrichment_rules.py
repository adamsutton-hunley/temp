#!/usr/bin/env python3
"""
insert_enrichment_rules.py
Load enrichment rules into the spec-enrichment-rule DynamoDB table.
Supports standalone execution and invocation from deploy_master.py.
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, List

import boto3
from botocore.exceptions import ClientError


def _resolve_env_id(raw_env_id: Any, environment_id_map: Optional[Dict[str, str]]) -> str:
    """Return the environment_id, allowing shorthand keys (e.g., 'prod') to be mapped."""
    env_id = str(raw_env_id)
    if environment_id_map and env_id in environment_id_map:
        return environment_id_map[env_id]
    return env_id


def _resolve_client_id(raw_client_id: Any, default_client_id: Optional[str]) -> Optional[str]:
    """Choose client_id from the item or fall back to the generated client_id."""
    if raw_client_id is None or raw_client_id == "":
        return default_client_id
    return str(raw_client_id)


def _coerce_version(raw_version: Any) -> str:
    """Ensure the version is a numeric string for DynamoDB."""
    try:
        return str(int(raw_version))
    except Exception as exc:
        raise ValueError(f"version '{raw_version}' is not a number") from exc


def _build_dynamo_item(
    raw_item: Dict[str, Any],
    environment_id_map: Optional[Dict[str, str]],
    default_client_id: Optional[str],
) -> Tuple[Dict[str, Dict[str, str]], Dict[str, Any]]:
    """Normalize an enrichment rule item and return (dynamodb_item, readable_item)."""
    if not isinstance(raw_item, dict):
        raise ValueError("item must be a JSON object")

    # Accept both Dynamo-typed input and plain JSON.
    env_id_value = raw_item.get("environment_id")
    if isinstance(env_id_value, dict) and "S" in env_id_value:
        env_id_value = env_id_value["S"]
    if env_id_value is None:
        raise ValueError("environment_id is required")
    env_id_value = _resolve_env_id(env_id_value, environment_id_map)

    version_value = raw_item.get("version")
    if isinstance(version_value, dict) and "N" in version_value:
        version_value = version_value["N"]
    if version_value is None:
        raise ValueError("version is required")
    version_value = _coerce_version(version_value)

    client_id_value = raw_item.get("client_id")
    if isinstance(client_id_value, dict) and "S" in client_id_value:
        client_id_value = client_id_value["S"]
    client_id_value = _resolve_client_id(client_id_value, default_client_id)

    rules_json_value = raw_item.get("rules_json")
    if isinstance(rules_json_value, dict) and "S" in rules_json_value:
        rules_json_value = rules_json_value["S"]
    if rules_json_value is None:
        raise ValueError("rules_json is required")

    dynamodb_item: Dict[str, Dict[str, str]] = {
        "environment_id": {"S": env_id_value},
        "version": {"N": version_value},
        "rules_json": {"S": str(rules_json_value)},
    }

    readable_item: Dict[str, Any] = {
        "environment_id": env_id_value,
        "version": int(version_value),
        "rules_json": rules_json_value,
    }

    if client_id_value:
        dynamodb_item["client_id"] = {"S": client_id_value}
        readable_item["client_id"] = client_id_value

    return dynamodb_item, readable_item


def _insert_item_to_dynamodb(dynamodb_client, table_name: str, item: Dict[str, Dict[str, str]]) -> bool:
    """Insert item into DynamoDB."""
    try:
        dynamodb_client.put_item(TableName=table_name, Item=item)
        print(f"Successfully inserted enrichment rule env_id={item['environment_id']['S']} version={item['version']['N']}")
        return True
    except ClientError as err:
        print(f"Error inserting env_id={item['environment_id']['S']} version={item['version']['N']}: {err.response['Error']['Message']}")
        return False
    except Exception as exc:
        print(f"Unexpected error inserting env_id={item['environment_id']['S']} version={item['version']['N']}: {str(exc)}")
        return False


def insert_enrichment_rules(
    input_dir: str,
    table_name: str,
    region: str,
    dry_run: bool = False,
    dry_run_output_dir: Optional[Path] = None,
    environment_id_map: Optional[Dict[str, str]] = None,
    client_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Insert enrichment rules into DynamoDB."""
    input_dir_path = Path(input_dir)
    rules_file = input_dir_path / "enrichment_rules.json"

    if not rules_file.exists():
        return {"success": False, "error": f"Rules file '{rules_file}' does not exist"}

    try:
        if not dry_run:
            dynamodb_client = boto3.client("dynamodb", region_name=region)
            dynamodb_client.describe_table(TableName=table_name)
            print(f"Connected to DynamoDB table: {table_name}")
        else:
            dynamodb_client = None
    except ClientError as err:
        if err.response["Error"]["Code"] == "ResourceNotFoundException":
            return {"success": False, "error": f"DynamoDB table '{table_name}' does not exist"}
        return {"success": False, "error": f"Unable to connect to DynamoDB: {err.response['Error']['Message']}"}
    except Exception as exc:
        return {"success": False, "error": f"Unable to connect to DynamoDB: {str(exc)}"}

    try:
        with open(rules_file, "r") as file_handle:
            rules_data = json.load(file_handle)

        if not isinstance(rules_data, list):
            return {"success": False, "error": "Rules file must contain a JSON array"}

        print(f"Found {len(rules_data)} enrichment rule item(s) in file")

        dynamo_items: List[Dict[str, Dict[str, str]]] = []
        readable_items: List[Dict[str, Any]] = []

        for idx, raw_item in enumerate(rules_data):
            dynamo_item, readable_item = _build_dynamo_item(raw_item, environment_id_map, client_id)
            dynamo_items.append(dynamo_item)
            readable_items.append(readable_item)

        successful = 0
        failed = 0

        for dynamo_item, readable_item in zip(dynamo_items, readable_items):
            env_id = readable_item["environment_id"]
            version = readable_item["version"]

            if dry_run:
                print(f"Would insert enrichment rule env_id={env_id} version={version} (rules_json length={len(str(readable_item['rules_json']))})")
                successful += 1
                continue

            if _insert_item_to_dynamodb(dynamodb_client, table_name, dynamo_item):
                successful += 1
            else:
                failed += 1

        if dry_run and readable_items and dry_run_output_dir:
            output_file = dry_run_output_dir / "enrichment_rules.json"
            with open(output_file, "w") as file_handle:
                json.dump(readable_items, file_handle, indent=2)
            print(f"Dry-run enrichment rules saved to: {output_file}")

        return {
            "success": failed == 0,
            "total_rules": len(readable_items),
            "successful_inserts": successful,
            "failed_inserts": failed,
        }
    except FileNotFoundError:
        return {"success": False, "error": f"Rules file '{rules_file}' not found"}
    except json.JSONDecodeError as err:
        return {"success": False, "error": f"Invalid JSON in rules file: {str(err)}"}
    except ValueError as err:
        return {"success": False, "error": str(err)}
    except Exception as exc:
        return {"success": False, "error": f"Error during enrichment rule insertion: {str(exc)}"}


def main():
    parser = argparse.ArgumentParser(description="Insert enrichment rules into DynamoDB")
    parser.add_argument("--input-dir", required=True, help="Directory containing enrichment_rules.json")
    parser.add_argument("--table-name", default="spec-enrichment-rule", help="DynamoDB table name (default: spec-enrichment-rule)")
    parser.add_argument("--region", default="us-east-1", help="AWS region (default: us-east-1)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be inserted without writing to DynamoDB")
    parser.add_argument("--client-id", help="Override client_id to use when the file does not specify one")

    args = parser.parse_args()

    print("Starting enrichment rule insertion...")
    print(f"Table: {args.table_name}")
    print(f"Input Directory: {args.input_dir}")
    print(f"Region: {args.region}")
    if args.dry_run:
        print("DRY RUN MODE: No items will be inserted")

    result = insert_enrichment_rules(
        input_dir=args.input_dir,
        table_name=args.table_name,
        region=args.region,
        dry_run=args.dry_run,
        client_id=args.client_id,
    )

    if result["success"]:
        print("\n=== ENRICHMENT INSERT SUMMARY ===")
        print(f"Total items processed: {result['total_rules']}")
        print(f"Successful inserts: {result['successful_inserts']}")
        if not args.dry_run:
            print(f"Failed inserts: {result['failed_inserts']}")
        print("\nScript completed successfully.")
    else:
        print(f"Error: {result['error']}")
        sys.exit(1)


if __name__ == "__main__":
    main()
