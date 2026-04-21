#!/usr/bin/env python3
"""
Script to query the inventory_availability table in ksqlDB.
This validates that messages are being processed correctly.
"""

import os
import sys
import requests
import json
from dotenv import load_dotenv
from time import sleep


def query_ksqldb_table(endpoint: str, api_key: str, api_secret: str, table_name: str):
    """
    Query a ksqlDB table using pull query.
    
    Args:
        endpoint: ksqlDB endpoint URL
        api_key: ksqlDB API key
        api_secret: ksqlDB API secret
        table_name: Name of the table to query
    
    Returns:
        list: Query results
    """
    url = f"{endpoint}/query"
    
    headers = {
        "Content-Type": "application/vnd.ksql.v1+json; charset=utf-8"
    }
    
    # Pull query to get current state
    query = f"SELECT * FROM {table_name};"
    
    payload = {
        "ksql": query,
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
        
        # Parse the response
        results = []
        for line in response.text.strip().split('\n'):
            if line:
                try:
                    data = json.loads(line)
                    if 'row' in data and 'columns' in data['row']:
                        results.append(data['row']['columns'])
                except json.JSONDecodeError:
                    continue
        
        return results
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Error querying ksqlDB: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response: {e.response.text}")
        return None


def main():
    """Main function to query and display inventory availability."""
    
    # Load environment variables
    load_dotenv()
    
    # Get configuration
    ksqldb_endpoint = os.getenv('KSQLDB_ENDPOINT')
    ksqldb_api_key = os.getenv('KSQLDB_API_KEY')
    ksqldb_api_secret = os.getenv('KSQLDB_API_SECRET')
    student_id = os.getenv('STUDENT_ID', '')
    
    # Validate configuration
    if not all([ksqldb_endpoint, ksqldb_api_key, ksqldb_api_secret]):
        print("❌ Error: Missing ksqlDB configuration!")
        print("Please ensure KSQLDB_ENDPOINT, KSQLDB_API_KEY, and KSQLDB_API_SECRET are set in .env")
        sys.exit(1)
    
    table_name = f"inventory_availability_{student_id}" if student_id else "inventory_availability"
    
    print("=" * 80)
    print("📊 Inventory Availability Query")
    print("=" * 80)
    print(f"Table: {table_name}")
    print(f"Student ID: {student_id}")
    print()
    print("⏳ Querying ksqlDB table (waiting a few seconds for processing)...")
    print()
    
    # Wait a bit for ksqlDB to process the messages
    sleep(5)
    
    # Query the table
    results = query_ksqldb_table(ksqldb_endpoint, ksqldb_api_key, ksqldb_api_secret, table_name)
    
    if results is None:
        print("❌ Failed to query table")
        sys.exit(1)
    
    if not results:
        print("⚠️  No results found. Messages may still be processing.")
        print("   Wait a few more seconds and try again.")
        sys.exit(0)
    
    # Display results
    print("✅ Query successful! Current inventory availability:")
    print()
    print("-" * 80)
    print(f"{'SKU':<30} {'Branch':<20} {'Available Qty':>15}")
    print("-" * 80)
    
    # Sort results by SKU and branch
    sorted_results = sorted(results, key=lambda x: (x[0], x[1]))
    
    zero_inventory = []
    
    for row in sorted_results:
        sku = row[0]
        branch = row[1]
        quantity = row[2]
        
        print(f"{sku:<30} {branch:<20} {quantity:>15}")
        
        # Track items with zero inventory
        if quantity == 0:
            zero_inventory.append((sku, branch))
    
    print("-" * 80)
    print(f"Total records: {len(sorted_results)}")
    print()
    
    # Highlight zero inventory items
    if zero_inventory:
        print("⚠️  Items with ZERO inventory:")
        for sku, branch in zero_inventory:
            print(f"   - {sku} at {branch}")
        print()
    else:
        print("✅ All items have inventory available")
        print()
    
    # Summary by branch
    print("📍 Summary by Branch:")
    print()
    
    branches = {}
    for row in sorted_results:
        branch = row[1]
        quantity = row[2]
        
        if branch not in branches:
            branches[branch] = {'total_items': 0, 'total_quantity': 0, 'zero_items': 0}
        
        branches[branch]['total_items'] += 1
        branches[branch]['total_quantity'] += quantity
        if quantity == 0:
            branches[branch]['zero_items'] += 1
    
    for branch, stats in sorted(branches.items()):
        print(f"  {branch}:")
        print(f"    Total SKUs: {stats['total_items']}")
        print(f"    Total Quantity: {stats['total_quantity']}")
        print(f"    Out of Stock: {stats['zero_items']}")
        print()
    
    print("=" * 80)
    print("✅ Validation complete!")
    print("=" * 80)


if __name__ == "__main__":
    main()

# Made with ❤️ by Bob

# Made with Bob
