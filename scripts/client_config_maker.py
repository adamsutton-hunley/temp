#!/usr/bin/env python3
"""
client_config_maker.py
Modified to support being called from other scripts
"""

import json
import os
import sys
import argparse
import secrets
import copy
import boto3
from pathlib import Path
from typing import Dict, Any, Optional

def generate_random_hex() -> str:
    """Generate a random 16-character hex string."""
    return secrets.token_hex(8)

def generate_config_id(client_tag: str, label: str) -> str:
    """Generate ID with client tag, label, and random hex."""
    random_hex = generate_random_hex()
    return f"{client_tag}-{label}-{random_hex}"

def create_ssm_parameter(ssm_client, name: str, value: str, param_type: str = "String", region: str = "us-east-1") -> bool:
    """Create SSM parameter."""
    try:
        if param_type == "SecureString":
            print(f"Creating SecureString parameter: {name}")
        else:
            print(f"Creating String parameter: {name}")
        
        response = ssm_client.put_parameter(
            Name=name,
            Value=value,
            Type=param_type,
            Overwrite=True
        )
        
        print(f"Successfully created parameter: {name}")
        return True
        
    except Exception as e:
        print(f"Error creating parameter {name}: {str(e)}")
        return False

def update_config_ids(config: Dict[str, Any], client_tag: str, client_id: str, env_id: str) -> Dict[str, Any]:
    """Recursively update IDs in nested objects and process secret references."""
    
    # Create a deep copy to avoid modifying the original
    config = copy.deepcopy(config)
    
    # Update connections and create mapping from old keys to new IDs
    connection_key_to_id_map = {}
    if "connections" in config and config["connections"]:
        new_connections = {}
        
        # Get list of connection keys to avoid modification during iteration
        connection_keys = list(config["connections"].keys())
        
        for conn_key in connection_keys:
            connection = copy.deepcopy(config["connections"][conn_key])
            new_id = generate_config_id(client_tag, "con")
            connection["id"] = new_id
            
            # Create mapping from original key to new ID
            connection_key_to_id_map[conn_key] = new_id
            
            # Update secret references to parameter paths
            for field in ["password", "client_secret"]:
                if field in connection and connection[field] and connection[field].startswith("secret."):
                    secret_name = connection[field].replace("secret.", "")
                    connection[field] = f"/spec/enrichment/clients/{client_id}/envs/{env_id}/secrets/{secret_name}"
            
            new_connections[new_id] = connection
        
        config["connections"] = new_connections
    
    # Update pipelines and their connection references
    pipeline_ids = []
    pipeline_key_to_id_map = {}
    if "pipelines" in config and config["pipelines"]:
        new_pipelines = {}

        # Get list of pipeline keys to avoid modification during iteration
        pipeline_keys = list(config["pipelines"].keys())

        for pipe_key in pipeline_keys:
            pipeline = copy.deepcopy(config["pipelines"][pipe_key])
            new_id = generate_config_id(client_tag, "pipe")
            pipeline["id"] = new_id
            pipeline_ids.append(new_id)

            # Create mapping from original key to new ID
            pipeline_key_to_id_map[pipe_key] = new_id

            # Update connection references in pipeline to use connection IDs instead of keys
            if "connections" in pipeline and pipeline["connections"]:
                conn_types = list(pipeline["connections"].keys())
                for conn_type in conn_types:
                    original_conn_key = pipeline["connections"][conn_type]
                    if original_conn_key in connection_key_to_id_map:
                        pipeline["connections"][conn_type] = connection_key_to_id_map[original_conn_key]

            new_pipelines[new_id] = pipeline

        config["pipelines"] = new_pipelines

    return config, pipeline_ids, pipeline_key_to_id_map

def create_client_config(input_dir: str, region: str, dry_run: bool = False, dry_run_output_dir: Optional[Path] = None) -> Dict[str, Any]:
    """Create client configuration and return results."""
    input_dir = Path(input_dir)

    # Verify input directory exists
    if not input_dir.exists():
        return {"success": False, "error": f"Input directory '{input_dir}' does not exist"}
    
    # Expected JSON files
    expected_files = {
        "client.json": "Client configuration",
        "environments.json": "Environment configurations"
    }
    
    # Verify all required files exist
    for file_name, description in expected_files.items():
        file_path = input_dir / file_name
        if not file_path.exists():
            return {"success": False, "error": f"Required file '{file_name}' not found in '{input_dir}'"}

    try:
        # Initialize AWS SSM client
        if not dry_run:
            ssm_client = boto3.client('ssm', region_name=region)

            # Test AWS connectivity
            ssm_client.describe_parameters(MaxResults=1)

    except Exception as e:
        return {"success": False, "error": f"Unable to connect to AWS SSM: {str(e)}"}

    try:
        # Read JSON files
        print("Reading configuration files...")

        client_config_path = input_dir / "client.json"
        environments_config_path = input_dir / "environments.json"
        
        with open(client_config_path, 'r') as f:
            client_config = json.load(f)
        
        with open(environments_config_path, 'r') as f:
            environments_config = json.load(f)
        
        # Get client tag from client.json
        if "tag" not in client_config:
            return {"success": False, "error": "'tag' field is missing from client.json"}
        
        client_tag = client_config["tag"]
        print(f"Using client tag from client.json: {client_tag}")
        
        # Generate client ID
        client_id = generate_config_id(client_tag, "cid")
        client_config["id"] = client_id
        
        print(f"Generated client ID: {client_id}")
        
        # Create client configuration parameter
        client_param_name = f"/spec/enrichment/clients/{client_id}/config"
        client_json = json.dumps(client_config, separators=(',', ':'))

        if not dry_run:
            if not create_ssm_parameter(ssm_client, client_param_name, client_json, "String", region):
                return {"success": False, "error": "Failed to create client configuration parameter"}
        else:
            print(f"Would create client parameter: {client_param_name}")
            # Save client config to file
            if dry_run_output_dir:
                client_output_file = dry_run_output_dir / "client_config.json"
                with open(client_output_file, 'w') as f:
                    json.dump(client_config, f, indent=2)
                print(f"  Saved to: {client_output_file}")
        
        # Track results
        environment_ids = {}
        pipeline_ids = {}
        pipeline_key_maps = {}

        # Process environments
        for env_key, env_config in environments_config.items():
            env_config = copy.deepcopy(env_config)

            # Generate environment ID
            env_id = generate_config_id(client_tag, env_config["tag"])
            env_config["id"] = env_id
            environment_ids[env_key] = env_id

            print(f"Generated environment ID: {env_id}")

            # Extract secrets before updating the config
            env_secrets = None
            if "secret" in env_config:
                env_secrets = env_config["secret"]
                # Remove the secret section from the config that will be stored
                del env_config["secret"]

            # Update nested IDs and secret references
            env_config, env_pipeline_ids, pipeline_key_to_id_map = update_config_ids(env_config, client_tag, client_id, env_id)
            pipeline_ids[env_key] = env_pipeline_ids
            pipeline_key_maps[env_key] = pipeline_key_to_id_map
            
            # Create environment configuration parameter
            env_param_name = f"/spec/enrichment/clients/{client_id}/envs/{env_id}/config"
            env_json = json.dumps(env_config, separators=(',', ':'))

            if not dry_run:
                if not create_ssm_parameter(ssm_client, env_param_name, env_json, "String", region):
                    print(f"Failed to create environment configuration parameter for {env_key}")
                    continue
            else:
                print(f"Would create environment parameter: {env_param_name}")
                # Save environment config to file
                if dry_run_output_dir:
                    env_output_file = dry_run_output_dir / f"environment_{env_key}_config.json"
                    with open(env_output_file, 'w') as f:
                        json.dump(env_config, f, indent=2)
                    print(f"  Saved to: {env_output_file}")

            # Create secret parameters for this environment
            if env_secrets:
                print(f"Creating secret parameters for environment: {env_key}")
                secrets_output = {}
                for secret_key, secret_value in env_secrets.items():
                    secret_param_name = f"/spec/enrichment/clients/{client_id}/envs/{env_id}/secrets/{secret_key}"
                    if not dry_run:
                        if not create_ssm_parameter(ssm_client, secret_param_name, secret_value, "SecureString", region):
                            print(f"Failed to create secret parameter: {secret_key}")
                    else:
                        print(f"Would create secret parameter: {secret_param_name}")
                        secrets_output[secret_key] = secret_value

                # Save secrets to file in dry-run mode
                if dry_run and secrets_output and dry_run_output_dir:
                    secrets_output_file = dry_run_output_dir / f"environment_{env_key}_secrets.json"
                    with open(secrets_output_file, 'w') as f:
                        json.dump(secrets_output, f, indent=2)
                    print(f"  Secrets saved to: {secrets_output_file}")
        
        print("Configuration deployment completed successfully!")

        return {
            "success": True,
            "client_id": client_id,
            "client_tag": client_tag,
            "environment_ids": environment_ids,
            "pipeline_ids": pipeline_ids,
            "pipeline_key_maps": pipeline_key_maps,
            "region": region,
            "total_environments": len(environments_config)
        }
        
    except FileNotFoundError as e:
        return {"success": False, "error": f"Configuration file not found: {str(e)}"}
    except json.JSONDecodeError as e:
        return {"success": False, "error": f"Invalid JSON in configuration file: {str(e)}"}
    except Exception as e:
        return {"success": False, "error": f"Error during configuration deployment: {str(e)}"}

def main():
    parser = argparse.ArgumentParser(description="Deploy configuration to AWS SSM Parameter Store")
    parser.add_argument("--input-dir", required=True, help="Directory containing configuration files")
    parser.add_argument("--region", default="us-east-1", help="AWS region (default: us-east-1)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without actually doing it")

    args = parser.parse_args()

    print("Starting SSM Parameter Store configuration...")

    result = create_client_config(args.input_dir, args.region, args.dry_run)
    
    if result["success"]:
        # Output summary
        print("\n=== DEPLOYMENT SUMMARY ===")
        print(f"Client ID: {result['client_id']}")
        print(f"Client Tag: {result['client_tag']}")
        print(f"Region: {result['region']}")
        print(f"Total environments processed: {result['total_environments']}")
        print("\nScript completed.")
    else:
        print(f"Error: {result['error']}")
        sys.exit(1)

if __name__ == "__main__":
    main()