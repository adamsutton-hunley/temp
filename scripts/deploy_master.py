#!/usr/bin/env python3
"""
Master Deployment Script
This script orchestrates the deployment of client configurations, download rules, and enrichment rules.
"""

import json
import sys
import argparse
import subprocess
from pathlib import Path
from typing import Dict, Any, List, Tuple
from datetime import datetime

def run_client_config_maker(input_dir: str, region: str, dry_run: bool = False, dry_run_output_dir: Path = None) -> Tuple[bool, Dict[str, Any]]:
    """Run the client configuration maker script and return results."""
    print("=" * 60)
    print("STEP 1: Creating client configuration in SSM Parameter Store")
    print("=" * 60)

    try:
        # Import and run the client config maker
        sys.path.insert(0, str(Path(__file__).parent))
        from client_config_maker import create_client_config

        result = create_client_config(input_dir, region, dry_run, dry_run_output_dir)
        
        if result["success"]:
            print(f"\n✓ Client configuration created successfully!")
            print(f"  Client ID: {result['client_id']}")
            print(f"  Client Tag: {result['client_tag']}")
            print(f"  Environments: {', '.join(result['environment_ids'].keys())}")
            return True, result
        else:
            print(f"\n✗ Client configuration failed: {result.get('error', 'Unknown error')}")
            return False, result
            
    except Exception as e:
        print(f"\n✗ Error running client configuration maker: {str(e)}")
        return False, {"error": str(e)}

def run_download_rules_insertion(client_result: Dict[str, Any], input_dir: str, table_name: str, region: str, dry_run: bool = False, dry_run_output_dir: Path = None) -> bool:
    """Run the download rules insertion script for each environment."""
    print("\n" + "=" * 60)
    print("STEP 2: Inserting download rules into DynamoDB")
    print("=" * 60)

    try:
        # Import and run the download rules insertion
        sys.path.insert(0, str(Path(__file__).parent))
        from insert_download_rules import insert_download_rules

        client_id = client_result["client_id"]
        environment_ids = client_result["environment_ids"]
        pipeline_ids = client_result["pipeline_ids"]
        pipeline_key_maps = client_result["pipeline_key_maps"]

        overall_success = True

        for env_key, env_id in environment_ids.items():
            print(f"\nProcessing environment: {env_key}")
            print(f"Environment ID: {env_id}")

            # Get pipeline IDs and key mapping for this environment
            env_pipeline_ids = pipeline_ids.get(env_key, [])
            pipeline_key_to_id = pipeline_key_maps.get(env_key, {})

            if not env_pipeline_ids:
                print(f"Warning: No pipelines found for environment {env_key}")
                continue

            # Create reverse mapping from pipeline_id to pipeline_key
            id_to_key_map = {v: k for k, v in pipeline_key_to_id.items()}

            # Insert rules for each pipeline in this environment
            for pipeline_id in env_pipeline_ids:
                # Get the original pipeline key (e.g., "cc_pipeline", "dodge_pipeline")
                pipeline_key = id_to_key_map.get(pipeline_id)
                print(f"\nInserting rules for pipeline: {pipeline_id} (key: {pipeline_key})")

                result = insert_download_rules(
                    env_id=env_id,
                    client_id=client_id,
                    pipeline_id=pipeline_id,
                    input_dir=input_dir,
                    table_name=table_name,
                    region=region,
                    dry_run=dry_run,
                    dry_run_output_dir=dry_run_output_dir,
                    pipeline_key=pipeline_key
                )
                
                if result["success"]:
                    print(f"✓ Rules inserted successfully for pipeline {pipeline_id}")
                    print(f"  Rules processed: {result['total_rules']}")
                    print(f"  Successful inserts: {result['successful_inserts']}")
                    if not dry_run and result['failed_inserts'] > 0:
                        print(f"  Failed inserts: {result['failed_inserts']}")
                else:
                    print(f"✗ Failed to insert rules for pipeline {pipeline_id}: {result.get('error', 'Unknown error')}")
                    overall_success = False
        
        return overall_success
        
    except Exception as e:
        print(f"\n✗ Error running download rules insertion: {str(e)}")
        return False

def run_enrichment_rules_insertion(client_result: Dict[str, Any], input_dir: str, table_name: str, region: str, dry_run: bool = False, dry_run_output_dir: Path = None) -> Tuple[bool, Dict[str, Any]]:
    """Run the enrichment rules insertion script."""
    print("\n" + "=" * 60)
    print("STEP 3: Inserting enrichment rules into DynamoDB")
    print("=" * 60)

    try:
        sys.path.insert(0, str(Path(__file__).parent))
        from insert_enrichment_rules import insert_enrichment_rules

        result = insert_enrichment_rules(
            input_dir=input_dir,
            table_name=table_name,
            region=region,
            dry_run=dry_run,
            dry_run_output_dir=dry_run_output_dir,
            environment_id_map=client_result.get("environment_ids"),
            client_id=client_result.get("client_id")
        )

        if result["success"]:
            print(f"✓ Enrichment rules processed successfully")
            print(f"  Items processed: {result['total_rules']}")
            print(f"  Successful inserts: {result['successful_inserts']}")
            if not dry_run and result['failed_inserts'] > 0:
                print(f"  Failed inserts: {result['failed_inserts']}")
            return True, result
        else:
            print(f"✗ Failed to insert enrichment rules: {result.get('error', 'Unknown error')}")
            return False, result
    except Exception as e:
        print(f"\n✗ Error running enrichment rules insertion: {str(e)}")
        return False, {"error": str(e)}

def main():
    parser = argparse.ArgumentParser(description="Deploy client configuration, download rules, and enrichment rules")

    # Create mutually exclusive group for input directory options
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--input-dir", help="Full path to directory containing configuration files")
    input_group.add_argument("--input", help="Name of subfolder in ./input directory (e.g., 'newcustomer' for ./input/newcustomer)")

    parser.add_argument("--table-name", default="spec-download-rule", help="DynamoDB table name (default: spec-download-rule)")
    parser.add_argument("--enrichment-table-name", default="spec-enrichment-rule", help="DynamoDB table name for enrichment rules (default: spec-enrichment-rule)")
    parser.add_argument("--region", default="us-east-1", help="AWS region (default: us-east-1)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without actually doing it")
    parser.add_argument("--skip-rules", action="store_true", help="Skip download rules insertion (only create client config)")
    parser.add_argument("--skip-enrichment-rules", action="store_true", help="Skip enrichment rules insertion")

    args = parser.parse_args()

    # Determine the input directory
    if args.input:
        # Find project root (go up from scripts directory)
        script_dir = Path(__file__).parent
        project_root = script_dir.parent
        input_dir = project_root / "input" / args.input
    else:
        input_dir = Path(args.input_dir)

    print("🚀 Starting Master Deployment Script")
    print(f"Input Directory: {input_dir}")
    print(f"DynamoDB Table: {args.table_name}")
    print(f"Enrichment Rule Table: {args.enrichment_table_name}")
    print(f"Region: {args.region}")

    # Create timestamped dry-run output directory if in dry-run mode
    dry_run_output_dir = None
    if args.dry_run:
        print("🧪 DRY RUN MODE: No changes will be made")

        # Find project root (go up from scripts directory)
        script_dir = Path(__file__).parent
        project_root = script_dir.parent

        # Create timestamped subdirectory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dry_run_output_dir = project_root / ".dev/dry_run_output" / timestamp
        dry_run_output_dir.mkdir(parents=True, exist_ok=True)

        print(f"📁 Dry-run outputs will be saved to: {dry_run_output_dir}")

    # Verify input directory exists
    if not input_dir.exists():
        print(f"❌ Error: Input directory '{input_dir}' does not exist")
        sys.exit(1)

    # Step 1: Create client configuration
    success, client_result = run_client_config_maker(
        input_dir=str(input_dir),
        region=args.region,
        dry_run=args.dry_run,
        dry_run_output_dir=dry_run_output_dir
    )

    if not success:
        print("❌ Client configuration creation failed. Stopping deployment.")
        sys.exit(1)

    # Step 2: Insert download rules (unless skipped)
    if not args.skip_rules:
        # Verify rules file exists
        rules_file = input_dir / "download_rules.json"
        if not rules_file.exists():
            print(f"❌ Error: Rules file '{rules_file}' does not exist")
            sys.exit(1)

        rules_success = run_download_rules_insertion(
            client_result=client_result,
            input_dir=str(input_dir),
            table_name=args.table_name,
            region=args.region,
            dry_run=args.dry_run,
            dry_run_output_dir=dry_run_output_dir
        )
        
        if not rules_success:
            print("⚠️  Download rules insertion completed with some failures")
    else:
        print("\n⏭️  Skipping download rules insertion (--skip-rules specified)")

    # Step 3: Insert enrichment rules (unless skipped)
    enrichment_result = None
    if not args.skip_enrichment_rules:
        enrichment_file = input_dir / "enrichment_rules.json"
        if not enrichment_file.exists():
            print(f"❌ Error: Enrichment rules file '{enrichment_file}' does not exist")
            sys.exit(1)

        enrichment_success, enrichment_result = run_enrichment_rules_insertion(
            client_result=client_result,
            input_dir=str(input_dir),
            table_name=args.enrichment_table_name,
            region=args.region,
            dry_run=args.dry_run,
            dry_run_output_dir=dry_run_output_dir
        )

        if not enrichment_success:
            print("⚠️  Enrichment rules insertion completed with some failures")
    else:
        print("\n⏭️  Skipping enrichment rules insertion (--skip-enrichment-rules specified)")
    
    # Final summary
    print("\n" + "=" * 60)

    summ_title = "🎉 DRY RUN SUMMARY" if args.dry_run else "🎉 DEPLOYMENT SUMMARY"
    print(summ_title)
    print("=" * 60)
    print(f"Client ID: {client_result['client_id']}")
    print(f"Client Tag: {client_result['client_tag']}")
    print(f"Region: {args.region}")
    print(f"Environments processed: {len(client_result['environment_ids'])}")

    for env_key, env_id in client_result['environment_ids'].items():
        print(f"  {env_key}: {env_id}")

    if not args.skip_rules:
        total_pipelines = sum(len(pipes) for pipes in client_result['pipeline_ids'].values())
        print(f"Total pipelines with rules: {total_pipelines}")
    
    if not args.skip_enrichment_rules and enrichment_result is not None:
        print(f"Enrichment rule items processed: {enrichment_result['total_rules']}")

    if args.dry_run:
        print("\n📁 DRY RUN OUTPUT FILES:")
        print(f"All generated configurations saved to: {dry_run_output_dir}")
        print("Review these files before running without --dry-run")
        if enrichment_result and enrichment_result.get("total_rules"):
            print("Enrichment rules saved for review in dry run output")

    end_msg = "\n✅ Dry run completed!" if args.dry_run else "\n✅ Deployment completed!"
    print(end_msg)

if __name__ == "__main__":
    main()
