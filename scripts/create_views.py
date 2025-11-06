#!/usr/bin/env python3
"""
Create BigQuery views for LifeEmbedding project
Views: v_complete_profiles, v_visualization_data
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google.cloud import bigquery
import config

def create_complete_profiles_view(client):
    """
    Create view that joins persons with their life events
    Useful for: data exploration, event analysis, timeline generation
    """
    
    view_id = f"{config.PROJECT_ID}.{config.DATASET_ID}.v_complete_profiles"
    
    view_query = f"""
    SELECT 
        p.person_id,
        p.wikidata_id,
        p.name,
        p.description,
        p.occupation,
        p.field_of_work,
        p.birth_date,
        p.death_date,
        p.birth_place,
        p.death_place,
        
        -- Aggregate life events
        ARRAY_AGG(
            STRUCT(
                e.event_id,
                e.event_type,
                e.event_title,
                e.event_description,
                e.start_date,
                e.end_date,
                e.location,
                e.organization
            ) 
            ORDER BY e.start_date ASC
        ) AS life_events,
        
        -- Event statistics
        COUNT(e.event_id) AS total_events,
        COUNTIF(e.event_type = 'education') AS education_events,
        COUNTIF(e.event_type = 'employment') AS employment_events,
        COUNTIF(e.event_type = 'award') AS award_events,
        
        -- Date range
        MIN(e.start_date) AS earliest_event_date,
        MAX(COALESCE(e.end_date, e.start_date)) AS latest_event_date,
        
        p.created_at
        
    FROM 
        `{config.PROJECT_ID}.{config.DATASET_ID}.persons` p
    LEFT JOIN 
        `{config.PROJECT_ID}.{config.DATASET_ID}.life_events` e
    ON 
        p.person_id = e.person_id
    GROUP BY 
        p.person_id, p.wikidata_id, p.name, p.description,
        p.occupation, p.field_of_work, p.birth_date, p.death_date,
        p.birth_place, p.death_place, p.created_at
    """
    
    view = bigquery.Table(view_id)
    view.view_query = view_query
    view.description = "Complete profiles with aggregated life events for each person"
    
    try:
        view = client.create_table(view)
        print(f"✅ Created view: v_complete_profiles")
        print(f"   Description: {view.description}")
        return view
    except Exception as e:
        if "Already Exists" in str(e):
            print(f"✅ View v_complete_profiles already exists")
            # Update the view
            view = client.update_table(view, ["view_query"])
            print(f"   Updated existing view")
        else:
            print(f"❌ Error creating v_complete_profiles view: {e}")
            raise

def create_visualization_data_view(client):
    """
    Create view that joins persons, coordinates, and embeddings
    Useful for: frontend visualization, similarity search, cluster analysis
    """
    
    view_id = f"{config.PROJECT_ID}.{config.DATASET_ID}.v_visualization_data"
    
    view_query = f"""
    SELECT 
        p.person_id,
        p.wikidata_id,
        p.name,
        p.description,
        p.occupation,
        p.field_of_work,
        p.birth_date,
        p.death_date,
        
        -- 3D coordinates
        c.x,
        c.y,
        c.z,
        c.cluster_id,
        c.cluster_label,
        c.reduction_method,
        
        -- Embedding metadata
        emb.embedding_model,
        emb.embedding_dim,
        
        -- Computed fields
        CASE 
            WHEN p.death_date IS NULL THEN 'Living'
            ELSE 'Deceased'
        END AS status,
        
        DATE_DIFF(
            COALESCE(p.death_date, CURRENT_DATE()), 
            p.birth_date, 
            YEAR
        ) AS age_or_age_at_death,
        
        -- Get primary occupation (first in array)
        ARRAY_LENGTH(p.occupation) AS occupation_count,
        IF(ARRAY_LENGTH(p.occupation) > 0, p.occupation[OFFSET(0)], NULL) AS primary_occupation,
        
        -- Timestamps
        p.created_at AS person_created_at,
        c.created_at AS coordinates_created_at,
        emb.created_at AS embedding_created_at
        
    FROM 
        `{config.PROJECT_ID}.{config.DATASET_ID}.persons` p
    LEFT JOIN 
        `{config.PROJECT_ID}.{config.DATASET_ID}.coordinates_3d` c
    ON 
        p.person_id = c.person_id
    LEFT JOIN 
        `{config.PROJECT_ID}.{config.DATASET_ID}.embeddings` emb
    ON 
        p.person_id = emb.person_id
    WHERE
        c.person_id IS NOT NULL  -- Only include persons with coordinates
    """
    
    view = bigquery.Table(view_id)
    view.view_query = view_query
    view.description = "Visualization-ready data with persons, coordinates, and metadata"
    
    try:
        view = client.create_table(view)
        print(f"✅ Created view: v_visualization_data")
        print(f"   Description: {view.description}")
        return view
    except Exception as e:
        if "Already Exists" in str(e):
            print(f"✅ View v_visualization_data already exists")
            # Update the view
            view = client.update_table(view, ["view_query"])
            print(f"   Updated existing view")
        else:
            print(f"❌ Error creating v_visualization_data view: {e}")
            raise

def create_event_timeline_view(client):
    """
    Bonus view: Create a timeline view for temporal analysis
    Useful for: trajectory analysis, career progression study
    """
    
    view_id = f"{config.PROJECT_ID}.{config.DATASET_ID}.v_event_timeline"
    
    view_query = f"""
    SELECT 
        e.event_id,
        e.person_id,
        p.name AS person_name,
        p.occupation,
        e.event_type,
        e.event_title,
        e.event_description,
        e.start_date,
        e.end_date,
        e.location,
        e.organization,
        
        -- Temporal features
        EXTRACT(YEAR FROM e.start_date) AS start_year,
        EXTRACT(YEAR FROM e.end_date) AS end_year,
        DATE_DIFF(e.end_date, e.start_date, DAY) AS duration_days,
        
        -- Age at event (if birth date available)
        DATE_DIFF(e.start_date, p.birth_date, YEAR) AS age_at_event,
        
        -- Event ordering per person
        ROW_NUMBER() OVER (PARTITION BY e.person_id ORDER BY e.start_date) AS event_sequence,
        
        e.created_at
        
    FROM 
        `{config.PROJECT_ID}.{config.DATASET_ID}.life_events` e
    INNER JOIN 
        `{config.PROJECT_ID}.{config.DATASET_ID}.persons` p
    ON 
        e.person_id = p.person_id
    WHERE 
        e.start_date IS NOT NULL
    ORDER BY 
        e.person_id, e.start_date
    """
    
    view = bigquery.Table(view_id)
    view.view_query = view_query
    view.description = "Timeline view of all events with temporal features and sequencing"
    
    try:
        view = client.create_table(view)
        print(f"✅ Created view: v_event_timeline")
        print(f"   Description: {view.description}")
        return view
    except Exception as e:
        if "Already Exists" in str(e):
            print(f"✅ View v_event_timeline already exists")
            # Update the view
            view = client.update_table(view, ["view_query"])
            print(f"   Updated existing view")
        else:
            print(f"❌ Error creating v_event_timeline view: {e}")
            raise

def verify_views(client):
    """Verify all views exist and test with sample queries"""
    
    dataset_id = f"{config.PROJECT_ID}.{config.DATASET_ID}"
    
    print(f"\n📊 Views in dataset {config.DATASET_ID}:")
    print("="*60)
    
    views = ["v_complete_profiles", "v_visualization_data", "v_event_timeline"]
    
    for view_name in views:
        view_id = f"{dataset_id}.{view_name}"
        try:
            view = client.get_table(view_id)
            print(f"\n✅ {view_name}")
            print(f"   Description: {view.description}")
            print(f"   Type: {view.table_type}")
            
            # Try to preview the view (will be empty now, but validates query syntax)
            query = f"SELECT * FROM `{view_id}` LIMIT 1"
            try:
                result = client.query(query).result()
                print(f"   Query validation: ✅ View query is valid")
            except Exception as e:
                print(f"   Query validation: ⚠️  {str(e)[:100]}")
                
        except Exception as e:
            print(f"❌ {view_name}: Not found or error - {e}")
    
    print("\n" + "="*60)

def create_sample_test_queries(client):
    """Create a file with sample queries for testing the views"""
    
    queries = f"""
-- Sample Queries for LifeEmbedding Views
-- Copy these to BigQuery console to test after data is loaded

-- 1. Get complete profile for a person (replace with actual person_id)
SELECT 
    name, 
    description, 
    occupation,
    total_events,
    education_events,
    employment_events,
    award_events
FROM `{config.PROJECT_ID}.{config.DATASET_ID}.v_complete_profiles`
LIMIT 10;

-- 2. Get visualization data for all persons with coordinates
SELECT 
    person_id,
    name,
    primary_occupation,
    x, y, z,
    cluster_id,
    cluster_label,
    age_or_age_at_death
FROM `{config.PROJECT_ID}.{config.DATASET_ID}.v_visualization_data`
ORDER BY name
LIMIT 10;

-- 3. Get event timeline for analysis
SELECT 
    person_name,
    event_type,
    event_title,
    start_year,
    age_at_event,
    event_sequence
FROM `{config.PROJECT_ID}.{config.DATASET_ID}.v_event_timeline`
ORDER BY person_name, event_sequence
LIMIT 20;

-- 4. Cluster analysis (after clustering is done)
SELECT 
    cluster_id,
    cluster_label,
    COUNT(*) as person_count,
    ARRAY_AGG(DISTINCT primary_occupation IGNORE NULLS) as occupations_in_cluster
FROM `{config.PROJECT_ID}.{config.DATASET_ID}.v_visualization_data`
WHERE cluster_id IS NOT NULL
GROUP BY cluster_id, cluster_label
ORDER BY person_count DESC;

-- 5. Event type distribution
SELECT 
    event_type,
    COUNT(*) as event_count,
    COUNT(DISTINCT person_id) as person_count
FROM `{config.PROJECT_ID}.{config.DATASET_ID}.v_event_timeline`
GROUP BY event_type
ORDER BY event_count DESC;
"""
    
    # Write to file
    output_file = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "docs",
        "sample_queries.sql"
    )
    
    with open(output_file, "w") as f:
        f.write(queries)
    
    print(f"\n📝 Sample queries saved to: docs/sample_queries.sql")

def main():
    """Create all views"""
    
    print("="*60)
    print("Creating BigQuery Views for LifeEmbedding")
    print("="*60)
    print(f"Project: {config.PROJECT_ID}")
    print(f"Dataset: {config.DATASET_ID}")
    print("="*60)
    
    client = bigquery.Client(project=config.PROJECT_ID)
    
    # Create all views
    print("\n🔧 Creating views...")
    create_complete_profiles_view(client)
    create_visualization_data_view(client)
    create_event_timeline_view(client)
    
    # Verify
    verify_views(client)
    
    # Create sample queries
    create_sample_test_queries(client)
    
    print("\n✅ Step 2.3 Complete! All views created successfully.")
    print("\n📌 Next: Views will be empty until we load data in Phase 3.")

if __name__ == "__main__":
    main()
