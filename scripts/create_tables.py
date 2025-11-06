#!/usr/bin/env python3
"""
Create all BigQuery tables for LifeEmbedding project
Tables: persons, life_events, embeddings, coordinates_3d
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google.cloud import bigquery
import config

def create_persons_table(client):
    """Create the persons table"""
    
    table_id = f"{config.PROJECT_ID}.{config.DATASET_ID}.persons"
    
    schema = [
        bigquery.SchemaField("person_id", "STRING", mode="REQUIRED", description="Unique identifier for person"),
        bigquery.SchemaField("wikidata_id", "STRING", mode="REQUIRED", description="Wikidata QID (e.g., Q937)"),
        bigquery.SchemaField("name", "STRING", mode="REQUIRED", description="Full name of the person"),
        bigquery.SchemaField("description", "STRING", mode="NULLABLE", description="Short description/bio"),
        bigquery.SchemaField("occupation", "STRING", mode="REPEATED", description="List of occupations"),
        bigquery.SchemaField("field_of_work", "STRING", mode="REPEATED", description="Fields of work"),
        bigquery.SchemaField("citizenship", "STRING", mode="REPEATED", description="Countries of citizenship"),
        bigquery.SchemaField("languages", "STRING", mode="REPEATED", description="Languages spoken/written/signed"),
        bigquery.SchemaField("birth_date", "DATE", mode="NULLABLE", description="Date of birth"),
        bigquery.SchemaField("death_date", "DATE", mode="NULLABLE", description="Date of death (null if living)"),
        bigquery.SchemaField("birth_place", "STRING", mode="NULLABLE", description="Place of birth"),
        bigquery.SchemaField("death_place", "STRING", mode="NULLABLE", description="Place of death"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED", description="Record creation timestamp"),
    ]
    
    table = bigquery.Table(table_id, schema=schema)
    table.description = "Biographical information for all persons in the system"
    
    try:
        table = client.create_table(table)
        print(f"✅ Created table: persons")
        print(f"   Columns: {len(schema)}")
        return table
    except Exception as e:
        if "Already Exists" in str(e):
            print(f"✅ Table persons already exists")
        else:
            print(f"❌ Error creating persons table: {e}")
            raise

def create_life_events_table(client):
    """Create the life_events table"""
    
    table_id = f"{config.PROJECT_ID}.{config.DATASET_ID}.life_events"
    
    schema = [
        bigquery.SchemaField("event_id", "STRING", mode="REQUIRED", description="Unique identifier for event"),
        bigquery.SchemaField("person_id", "STRING", mode="REQUIRED", description="Reference to persons.person_id"),
        bigquery.SchemaField("event_type", "STRING", mode="REQUIRED", description="Type: education, employment, residence, award, etc."),
        bigquery.SchemaField("event_title", "STRING", mode="REQUIRED", description="Title/name of the event"),
        bigquery.SchemaField("event_description", "STRING", mode="NULLABLE", description="Detailed description"),
        bigquery.SchemaField("start_date", "DATE", mode="NULLABLE", description="Event start date"),
        bigquery.SchemaField("end_date", "DATE", mode="NULLABLE", description="Event end date (null for ongoing/point events)"),
        bigquery.SchemaField("point_in_time", "DATE", mode="NULLABLE", description="Point in time for instant events (awards, ceremonies)"),
        bigquery.SchemaField("location", "STRING", mode="NULLABLE", description="Location where event occurred"),
        bigquery.SchemaField("organization", "STRING", mode="NULLABLE", description="Associated organization/institution"),
        bigquery.SchemaField("role_or_degree", "STRING", mode="NULLABLE", description="Role, position, or academic degree"),
        bigquery.SchemaField("field_or_major", "STRING", mode="NULLABLE", description="Field of study, major, or area of work"),
        bigquery.SchemaField("sport", "STRING", mode="NULLABLE", description="Sport (for athletic events)"),
        bigquery.SchemaField("instrument", "STRING", mode="NULLABLE", description="Musical instrument (for musicians)"),
        bigquery.SchemaField("source", "STRING", mode="NULLABLE", description="Data source (e.g., Wikidata property)"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED", description="Record creation timestamp"),
    ]
    
    table = bigquery.Table(table_id, schema=schema)
    table.description = "Life events for all persons (education, employment, awards, etc.)"
    
    try:
        table = client.create_table(table)
        print(f"✅ Created table: life_events")
        print(f"   Columns: {len(schema)}")
        return table
    except Exception as e:
        if "Already Exists" in str(e):
            print(f"✅ Table life_events already exists")
        else:
            print(f"❌ Error creating life_events table: {e}")
            raise

def create_embeddings_table(client):
    """Create the embeddings table"""
    
    table_id = f"{config.PROJECT_ID}.{config.DATASET_ID}.embeddings"
    
    schema = [
        bigquery.SchemaField("person_id", "STRING", mode="REQUIRED", description="Reference to persons.person_id"),
        bigquery.SchemaField("embedding_vector", "FLOAT64", mode="REPEATED", description="High-dimensional embedding vector"),
        bigquery.SchemaField("embedding_model", "STRING", mode="REQUIRED", description="Model used (e.g., text-embedding-004)"),
        bigquery.SchemaField("embedding_dim", "INT64", mode="REQUIRED", description="Dimension of embedding vector"),
        bigquery.SchemaField("embedding_text", "STRING", mode="NULLABLE", description="Text that was embedded (for reference)"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED", description="Embedding generation timestamp"),
    ]
    
    table = bigquery.Table(table_id, schema=schema)
    table.description = "High-dimensional embeddings for all persons"
    
    try:
        table = client.create_table(table)
        print(f"✅ Created table: embeddings")
        print(f"   Columns: {len(schema)}")
        return table
    except Exception as e:
        if "Already Exists" in str(e):
            print(f"✅ Table embeddings already exists")
        else:
            print(f"❌ Error creating embeddings table: {e}")
            raise

def create_coordinates_3d_table(client):
    """Create the coordinates_3d table"""
    
    table_id = f"{config.PROJECT_ID}.{config.DATASET_ID}.coordinates_3d"
    
    schema = [
        bigquery.SchemaField("person_id", "STRING", mode="REQUIRED", description="Reference to persons.person_id"),
        bigquery.SchemaField("x", "FLOAT64", mode="REQUIRED", description="X coordinate in 3D space"),
        bigquery.SchemaField("y", "FLOAT64", mode="REQUIRED", description="Y coordinate in 3D space"),
        bigquery.SchemaField("z", "FLOAT64", mode="REQUIRED", description="Z coordinate in 3D space"),
        bigquery.SchemaField("reduction_method", "STRING", mode="REQUIRED", description="Dimensionality reduction method used (e.g., PCA+UMAP)"),
        bigquery.SchemaField("cluster_id", "INT64", mode="NULLABLE", description="Cluster assignment (null if not clustered yet)"),
        bigquery.SchemaField("cluster_label", "STRING", mode="NULLABLE", description="Human-readable cluster label"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED", description="Coordinate generation timestamp"),
    ]
    
    table = bigquery.Table(table_id, schema=schema)
    table.description = "3D coordinates for visualization after dimensionality reduction"
    
    try:
        table = client.create_table(table)
        print(f"✅ Created table: coordinates_3d")
        print(f"   Columns: {len(schema)}")
        return table
    except Exception as e:
        if "Already Exists" in str(e):
            print(f"✅ Table coordinates_3d already exists")
        else:
            print(f"❌ Error creating coordinates_3d table: {e}")
            raise

def verify_tables(client):
    """Verify all tables exist and show their schemas"""
    
    dataset_id = f"{config.PROJECT_ID}.{config.DATASET_ID}"
    tables = list(client.list_tables(dataset_id))
    
    print(f"\n📊 Tables in dataset {config.DATASET_ID}:")
    print("="*60)
    
    expected_tables = ["persons", "life_events", "embeddings", "coordinates_3d"]
    
    for table_name in expected_tables:
        table_id = f"{dataset_id}.{table_name}"
        try:
            table = client.get_table(table_id)
            print(f"\n✅ {table_name}")
            print(f"   Description: {table.description}")
            print(f"   Columns: {len(table.schema)}")
            print(f"   Rows: {table.num_rows}")
            print(f"   Size: {table.num_bytes / 1024:.2f} KB")
            
            # Show schema
            print(f"   Schema:")
            for field in table.schema[:5]:  # Show first 5 fields
                mode = f"[{field.mode}]" if field.mode != "NULLABLE" else ""
                print(f"      - {field.name}: {field.field_type} {mode}")
            if len(table.schema) > 5:
                print(f"      ... and {len(table.schema) - 5} more fields")
                
        except Exception as e:
            print(f"❌ {table_name}: Not found or error - {e}")
    
    print("\n" + "="*60)

def main():
    """Create all tables"""
    
    print("="*60)
    print("Creating BigQuery Tables for LifeEmbedding")
    print("="*60)
    print(f"Project: {config.PROJECT_ID}")
    print(f"Dataset: {config.DATASET_ID}")
    print(f"Location: {config.DATASET_LOCATION}")
    print("="*60)
    
    client = bigquery.Client(project=config.PROJECT_ID)
    
    # Create all tables
    print("\n🔧 Creating tables...")
    print("Note: If tables already exist, they will not be modified.")
    print("To update schema, you must drop and recreate tables.")
    create_persons_table(client)
    create_life_events_table(client)
    create_embeddings_table(client)
    create_coordinates_3d_table(client)
    
    # Verify
    verify_tables(client)
    
    print("\n✅ Step 2.2 Complete! All tables created successfully.")

if __name__ == "__main__":
    main()
