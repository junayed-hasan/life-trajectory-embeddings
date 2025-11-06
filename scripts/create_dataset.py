#!/usr/bin/env python3
"""
Create BigQuery dataset for LifeEmbedding project
"""

import sys
import os

# Add parent directory to path to import config
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google.cloud import bigquery
import config

def create_dataset():
    """Create the main BigQuery dataset"""
    
    client = bigquery.Client(project=config.PROJECT_ID)
    
    # Construct dataset ID
    dataset_id = f"{config.PROJECT_ID}.{config.DATASET_ID}"
    
    # Check if dataset already exists
    try:
        client.get_dataset(dataset_id)
        print(f"✅ Dataset {config.DATASET_ID} already exists")
        return
    except Exception:
        pass  # Dataset doesn't exist, proceed to create
    
    # Create dataset
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = config.DATASET_LOCATION
    dataset.description = "LifeEmbedding project data - persons, events, embeddings, and coordinates"
    
    # Set default table expiration (optional - set to None for no expiration)
    # dataset.default_table_expiration_ms = None
    
    print(f"🔧 Creating dataset {config.DATASET_ID} in {config.DATASET_LOCATION}...")
    
    dataset = client.create_dataset(dataset, timeout=30)
    
    print(f"✅ Dataset {dataset.dataset_id} created successfully!")
    print(f"   Location: {dataset.location}")
    print(f"   Full ID: {dataset.full_dataset_id}")
    
    return dataset

def verify_dataset():
    """Verify dataset exists and show details"""
    
    client = bigquery.Client(project=config.PROJECT_ID)
    dataset_id = f"{config.PROJECT_ID}.{config.DATASET_ID}"
    
    try:
        dataset = client.get_dataset(dataset_id)
        print(f"\n📊 Dataset Details:")
        print(f"   Dataset ID: {dataset.dataset_id}")
        print(f"   Location: {dataset.location}")
        print(f"   Description: {dataset.description}")
        print(f"   Created: {dataset.created}")
        print(f"   Modified: {dataset.modified}")
        
        # List tables in dataset
        tables = list(client.list_tables(dataset_id))
        if tables:
            print(f"\n📋 Tables in dataset:")
            for table in tables:
                print(f"   - {table.table_id}")
        else:
            print(f"\n📋 No tables in dataset yet (expected for new dataset)")
        
        return True
    except Exception as e:
        print(f"❌ Error verifying dataset: {e}")
        return False

if __name__ == "__main__":
    print("="*60)
    print("Creating BigQuery Dataset for LifeEmbedding")
    print("="*60)
    
    create_dataset()
    verify_dataset()
    
    print("\n✅ Step 2.1 Complete!")
