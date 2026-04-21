#!/usr/bin/env python3
"""
Script to set up ksqlDB streams and tables for inventory availability calculation.

PREREQUISITE: A ksqlDB cluster must be created manually in Confluent Cloud before running this script.

This script creates the necessary ksqlDB objects (streams and tables) to process
inventory transactions within an existing ksqlDB cluster.
"""

import os
import sys
import time
import requests
from dotenv import load_dotenv


def execute_ksql_statement(endpoint: str, api_key: str, api_secret: str, statement: str):
    """
    Execute a ksqlDB statement.
    
    Args:
        endpoint: ksqlDB endpoint URL
        api_key: ksqlDB API key
        api_secret: ksqlDB API secret
        statement: ksqlDB statement to execute
    
    Returns:
        dict: Response from ksqlDB
    """
    url = f"{endpoint}/ksql"
    
    headers = {
        "Content-Type": "application/vnd.ksql.v1+json; charset=utf-8"
    }
    
    payload = {
        "ksql": statement,
        "streamsProperties": {}
    }
    
    try:
        response = requests.post(
            url,
            json=payload,
            headers=headers,
            auth=(api_key, api_secret),
            timeout=30
        )
        
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.RequestException as e:
        print(f"✗ Error executing ksqlDB statement: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response: {e.response.text}")
        return None


def check_stream_exists(endpoint: str, api_key: str, api_secret: str, stream_name: str):
    """Check if a stream or table exists."""
    statement = "SHOW STREAMS;"
    result = execute_ksql_statement(endpoint, api_key, api_secret, statement)
    
    if result:
        for item in result:
            if 'streams' in item:
                for stream in item['streams']:
                    if stream['name'].upper() == stream_name.upper():
                        return True
    
    # Also check tables
    statement = "SHOW TABLES;"
    result = execute_ksql_statement(endpoint, api_key, api_secret, statement)
    
    if result:
        for item in result:
            if 'tables' in item:
                for table in item['tables']:
                    if table['name'].upper() == stream_name.upper():
                        return True
    
    return False


def setup_ksqldb_streams():
    """Set up ksqlDB streams and tables for inventory processing."""
    
    # Load environment variables
    load_dotenv()
    
    # Get ksqlDB configuration
    ksqldb_endpoint = os.getenv('KSQLDB_ENDPOINT')
    ksqldb_api_key = os.getenv('KSQLDB_API_KEY')
    ksqldb_api_secret = os.getenv('KSQLDB_API_SECRET')
    student_id = os.getenv('STUDENT_ID', '')
    topic_name = os.getenv('TOPIC_NAME', 'inventory.transactions')
    
    # Validate configuration
    if not all([ksqldb_endpoint, ksqldb_api_key, ksqldb_api_secret]):
        print("✗ Error: Missing ksqlDB configuration!")
        print("Please ensure the following environment variables are set:")
        print("  - KSQLDB_ENDPOINT")
        print("  - KSQLDB_API_KEY")
        print("  - KSQLDB_API_SECRET")
        print("\nYou can set these in the .env file")
        return False
    
    if not student_id:
        print("⚠️  Warning: STUDENT_ID not set in .env file!")
        print("   Run 'python3 init_student_env.py' to generate a unique ID")
        print("   This is important to avoid naming collisions in multi-student scenarios")
        print()
    
    # Generate unique names using student_id
    stream_name = f"inventory_transactions_{student_id}" if student_id else "inventory_transactions"
    table_name = f"inventory_availability_{student_id}" if student_id else "inventory_availability"
    derived_topic = f"inventory.availability.{student_id}" if student_id else "inventory.availability"
    
    print("=" * 60)
    print("Setting up ksqlDB Streams for Inventory Availability")
    print("=" * 60)
    print()
    print("NOTE: This script assumes a ksqlDB cluster already exists in Confluent Cloud.")
    print("      It will create streams and tables within that existing cluster.")
    if student_id:
        print(f"      Using Student ID: {student_id}")
    print()
    
    # Step 1: Create inventory_transactions stream
    print(f"Step 1: Creating {stream_name} stream...")
    
    stream_name_upper = stream_name.upper()
    if check_stream_exists(ksqldb_endpoint, ksqldb_api_key, ksqldb_api_secret, stream_name_upper):
        print(f"⚠ Stream '{stream_name_upper}' already exists, skipping creation.")
    else:
        stream_statement = f"""
        CREATE STREAM {stream_name} (
            sku VARCHAR,
            branch VARCHAR,
            quantity INT,
            transaction_type VARCHAR
        ) WITH (
            KAFKA_TOPIC='{topic_name}',
            KEY_FORMAT='KAFKA',
            VALUE_FORMAT='JSON'
        );
        """
        
        result = execute_ksql_statement(
            ksqldb_endpoint,
            ksqldb_api_key,
            ksqldb_api_secret,
            stream_statement
        )
        
        if result:
            print(f"✓ Stream '{stream_name}' created successfully!")
        else:
            print(f"✗ Failed to create stream '{stream_name}'")
            return False
    
    print()
    
    # Step 2: Create inventory_availability table
    print(f"Step 2: Creating {table_name} table...")
    
    table_name_upper = table_name.upper()
    if check_stream_exists(ksqldb_endpoint, ksqldb_api_key, ksqldb_api_secret, table_name_upper):
        print(f"⚠ Table '{table_name_upper}' already exists, skipping creation.")
    else:
        table_statement = f"""
        CREATE TABLE {table_name} WITH (
          KAFKA_TOPIC='{derived_topic}',
          KEY_FORMAT='JSON',
          VALUE_FORMAT='JSON',
          PARTITIONS=6
        ) AS
        SELECT
          sku,
          branch,
          SUM(quantity) AS available_quantity
        FROM {stream_name}
        GROUP BY sku, branch
        EMIT CHANGES;
        """
        
        result = execute_ksql_statement(
            ksqldb_endpoint,
            ksqldb_api_key,
            ksqldb_api_secret,
            table_statement
        )
        
        if result:
            print(f"✓ Table '{table_name}' created successfully!")
            print("  This table will automatically calculate available quantities")
            print("  by summing all transactions grouped by SKU and branch.")
        else:
            print(f"✗ Failed to create table '{table_name}'")
            return False
    
    print()
    print("=" * 60)
    print("✓ ksqlDB Setup Complete!")
    print("=" * 60)
    print()
    print("The following objects have been created:")
    print(f"  1. Stream: {stream_name}")
    print(f"     - Reads from: {topic_name} topic")
    print("     - Fields: sku, branch, quantity, transaction_type")
    print()
    print(f"  2. Table: {table_name}")
    print(f"     - Writes to: {derived_topic} topic")
    print("     - Fields: sku, branch, available_quantity")
    print("     - Calculation: SUM(quantity) grouped by sku and branch")
    print()
    print("Next steps:")
    print(f"  - Publish messages to {topic_name} topic")
    print("  - Query inventory.availability to see calculated results")
    print()
    
    return True


def main():
    """Main function."""
    success = setup_ksqldb_streams()
    
    if success:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()

# Made with Bob