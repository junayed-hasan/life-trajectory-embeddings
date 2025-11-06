#!/usr/bin/env python3
"""
Test Backend API Endpoints
"""

import requests
import json
from datetime import date

# Base URL (change if running on different host/port)
BASE_URL = "http://localhost:8080"


def test_health():
    """Test health endpoint"""
    print("Testing /api/health...")
    response = requests.get(f"{BASE_URL}/api/health")
    print(f"  Status: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"  API Status: {data['status']}")
        print(f"  Services: {data['services']}")
    print()


def test_get_persons():
    """Test get persons endpoint"""
    print("Testing /api/v1/persons...")
    response = requests.get(f"{BASE_URL}/api/v1/persons?limit=5")
    print(f"  Status: {response.status_code}")
    if response.status_code == 200:
        persons = response.json()
        print(f"  Returned {len(persons)} persons")
        if persons:
            print(f"  First person: {persons[0]['name']}")
    print()


def test_get_visualization():
    """Test visualization data endpoint"""
    print("Testing /api/v1/visualization...")
    response = requests.get(f"{BASE_URL}/api/v1/visualization")
    print(f"  Status: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"  Total persons: {data['metadata']['total_persons']}")
        print(f"  Num clusters: {data['metadata']['num_clusters']}")
    print()


def test_get_clusters():
    """Test clusters endpoint"""
    print("Testing /api/v1/clusters...")
    response = requests.get(f"{BASE_URL}/api/v1/clusters")
    print(f"  Status: {response.status_code}")
    if response.status_code == 200:
        clusters = response.json()
        print(f"  Found {len(clusters)} clusters")
        for cluster in clusters[:3]:
            print(f"  - Cluster {cluster['cluster_id']}: {cluster['cluster_label']} ({cluster['person_count']} persons)")
    print()


def test_generate_embedding():
    """Test user embedding generation"""
    print("Testing /api/v1/generate-embedding...")
    
    payload = {
        "name": "Test User",
        "description": "Computer scientist and educator",
        "life_events": [
            {
                "event_type": "education",
                "event_title": "PhD in Computer Science",
                "event_description": "Machine Learning research",
                "start_date": "2015-09-01",
                "end_date": "2020-05-15",
                "organization": "Stanford University",
                "location": "Stanford, CA"
            },
            {
                "event_type": "employment",
                "event_title": "Assistant Professor",
                "start_date": "2020-08-01",
                "organization": "UC Berkeley",
                "location": "Berkeley, CA"
            },
            {
                "event_type": "award",
                "event_title": "Best Paper Award",
                "start_date": "2021-06-15",
                "organization": "ICML 2021"
            }
        ]
    }
    
    response = requests.post(f"{BASE_URL}/api/v1/generate-embedding", json=payload)
    print(f"  Status: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"  User coordinates: ({data['user_coordinates']['x']:.2f}, {data['user_coordinates']['y']:.2f}, {data['user_coordinates']['z']:.2f})")
        print(f"  Nearest cluster: {data['nearest_cluster']['cluster_label']}")
        print(f"  Similar persons:")
        for i, person in enumerate(data['similar_persons'][:5], 1):
            print(f"    {i}. {person['name']} (similarity: {person['similarity_score']:.2f})")
        print(f"  Processing time: {data['processing_time_ms']:.2f} ms")
    elif response.status_code == 503:
        print("  ⚠️  Service unavailable - PCA/UMAP models not loaded")
        print("  This is expected if dim_reduction.py hasn't been run yet")
    print()


def main():
    """Run all tests"""
    print("="*60)
    print("LIFEEMBEDDING BACKEND API TESTS")
    print("="*60)
    print()
    
    try:
        # Basic tests (should always work)
        test_health()
        test_get_persons()
        test_get_visualization()
        test_get_clusters()
        
        # Advanced test (requires PCA/UMAP models)
        test_generate_embedding()
        
        print("="*60)
        print("✓ TESTS COMPLETE")
        print("="*60)
        
    except requests.exceptions.ConnectionError:
        print("✗ Error: Could not connect to API")
        print("  Make sure the server is running:")
        print("  uvicorn main:app --reload --host 0.0.0.0 --port 8080")
    except Exception as e:
        print(f"✗ Error: {e}")


if __name__ == "__main__":
    main()
