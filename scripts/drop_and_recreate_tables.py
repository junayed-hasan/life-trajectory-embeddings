#!/usr/bin/env python3
"""
Drop and recreate BigQuery tables for LifeEmbedding project
Use this ONLY if you need to update the schema
WARNING: This will delete all data in the tables!
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google.cloud import bigquery
import config

def drop_tables(client):
    """Drop all existing tables"""
    
    tables = ["persons", "life_events", "embeddings", "coordinates_3d"]
    
    print("\n⚠️  WARNING: About to drop all tables and their data!")
    print("Tables to be dropped:")
    for table in tables:
        print(f"  - {table}")
    
    response = input("\nAre you sure you want to continue? (yes/no): ")
    if response.lower() != "yes":
        print("❌ Operation cancelled.")
        return False
    
    dataset_id = f"{config.PROJECT_ID}.{config.DATASET_ID}"
    
    for table_name in tables:
        table_id = f"{dataset_id}.{table_name}"
        try:
            client.delete_table(table_id, not_found_ok=True)
            print(f"✅ Dropped table: {table_name}")
        except Exception as e:
            print(f"❌ Error dropping {table_name}: {e}")
    
    return True

def main():
    """Drop and recreate all tables"""
    
    print("="*60)
    print("Drop and Recreate BigQuery Tables")
    print("="*60)
    print(f"Project: {config.PROJECT_ID}")
    print(f"Dataset: {config.DATASET_ID}")
    print("="*60)
    
    client = bigquery.Client(project=config.PROJECT_ID)
    
    # Drop existing tables
    if not drop_tables(client):
        return
    
    # Recreate tables with new schema
    print("\n🔧 Recreating tables with updated schema...")
    print("Running create_tables.py...")
    
    # Import and run create_tables
    import create_tables
    create_tables.create_persons_table(client)
    create_tables.create_life_events_table(client)
    create_tables.create_embeddings_table(client)
    create_tables.create_coordinates_3d_table(client)
    
    # Verify
    create_tables.verify_tables(client)
    
    print("\n✅ Tables recreated successfully with updated schema!")

if __name__ == "__main__":
    main()
