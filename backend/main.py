"""
LifeEmbedding Backend API
FastAPI application for serving life trajectory embeddings and visualization data
"""

import sys
import os
import time
import pickle
import json
from typing import List, Optional
from datetime import datetime

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import numpy as np

# Local imports
from models import (
    UserEmbeddingRequest, UserEmbeddingResponse, PersonSummary, PersonDetail,
    VisualizationData, VisualizationPerson, ClusterInfo, SimilarPerson,
    Coordinate3D, HealthResponse
)
from database import Database
from embeddings import EmbeddingService

# Add parent directory for config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

# Initialize FastAPI app
app = FastAPI(
    title="LifeEmbedding API",
    description="API for visualizing and comparing life trajectories as embeddings",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify exact origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize services
db = Database()
embedding_service = EmbeddingService()

# Load PCA/UMAP models at startup
_models_loaded = False
_pca_model = None
_umap_model = None


@app.on_event("startup")
async def startup_event():
    """Load PCA/UMAP models and cache data at startup"""
    global _models_loaded, _pca_model, _umap_model
    
    print("Loading reduction models from file...")
    
    # Try to load from local coordinates file
    coords_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 
                               'data', 'processed', 'coordinates_3d.json')
    
    if os.path.exists(coords_file):
        print(f"  Found coordinates file: {coords_file}")
        # Models will be loaded lazily when needed
        # For now, just verify the file exists
        _models_loaded = True
    else:
        print(f"  Warning: Coordinates file not found at {coords_file}")
        print(f"  User embedding generation will not be available")


@app.get("/", tags=["root"])
async def root():
    """Root endpoint"""
    return {
        "message": "LifeEmbedding API",
        "version": "1.0.0",
        "docs": "/api/docs"
    }


@app.get("/api/health", response_model=HealthResponse, tags=["health"])
async def health_check():
    """Health check endpoint"""
    # Test BigQuery connection
    try:
        persons = db.get_all_persons(limit=1)
        bigquery_status = "healthy"
    except Exception as e:
        bigquery_status = f"unhealthy: {str(e)}"
    
    return HealthResponse(
        status="healthy",
        timestamp=datetime.utcnow(),
        version="1.0.0",
        services={
            "bigquery": bigquery_status,
            "vertex_ai": "configured",
            "reduction_models": "loaded" if _models_loaded else "not loaded"
        }
    )


@app.get("/api/v1/persons", response_model=List[PersonSummary], tags=["persons"])
async def get_persons(
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    cluster_id: Optional[int] = Query(None, description="Filter by cluster ID")
):
    """
    Get list of all persons with basic information
    
    - **limit**: Maximum number of results (1-1000)
    - **offset**: Pagination offset
    - **cluster_id**: Optional filter by cluster
    """
    try:
        if cluster_id is not None:
            persons = db.get_persons_by_cluster(cluster_id)
            persons = persons[offset:offset+limit]
        else:
            persons = db.get_all_persons(limit=limit, offset=offset)
        
        # Convert to response model
        result = []
        for p in persons:
            result.append(PersonSummary(
                person_id=p['person_id'],
                name=p['name'],
                description=p.get('description'),
                occupation=p.get('occupation', []),
                cluster_id=p.get('cluster_id'),
                cluster_label=p.get('cluster_label'),
                coordinates=Coordinate3D(**p['coordinates']) if p.get('coordinates') else None
            ))
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching persons: {str(e)}")


@app.get("/api/v1/person/{person_id}", response_model=PersonDetail, tags=["persons"])
async def get_person(person_id: str):
    """
    Get detailed information for a specific person
    
    - **person_id**: Unique person identifier
    """
    try:
        person = db.get_person_by_id(person_id)
        
        if not person:
            raise HTTPException(status_code=404, detail=f"Person {person_id} not found")
        
        return PersonDetail(
            person_id=person['person_id'],
            wikidata_id=person['wikidata_id'],
            name=person['name'],
            description=person.get('description'),
            occupation=person.get('occupation', []),
            field_of_work=person.get('field_of_work', []),
            birth_date=person.get('birth_date'),
            death_date=person.get('death_date'),
            birth_place=person.get('birth_place'),
            death_place=person.get('death_place'),
            coordinates=Coordinate3D(**person['coordinates']) if person.get('coordinates') else None,
            cluster_id=person.get('cluster_id'),
            cluster_label=person.get('cluster_label'),
            total_events=person['total_events'],
            event_types=person['event_types']
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching person: {str(e)}")


@app.get("/api/v1/visualization", response_model=VisualizationData, tags=["visualization"])
async def get_visualization_data():
    """
    Get complete dataset for 3D visualization
    
    Returns all persons with 3D coordinates and cluster assignments
    """
    try:
        persons = db.get_visualization_data()
        
        # Convert to response model
        viz_persons = [
            VisualizationPerson(**p) for p in persons
        ]
        
        # Get metadata
        clusters = db.get_clusters_info()
        
        metadata = {
            "total_persons": len(viz_persons),
            "num_clusters": len(clusters),
            "reduction_method": "PCA(50D) + UMAP(3D)",
            "timestamp": datetime.utcnow().isoformat()
        }
        
        return VisualizationData(
            persons=viz_persons,
            metadata=metadata
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching visualization data: {str(e)}")


@app.get("/api/v1/clusters", response_model=List[ClusterInfo], tags=["clusters"])
async def get_clusters():
    """
    Get information about all clusters
    
    Returns cluster statistics, top occupations, and centroid coordinates
    """
    try:
        clusters = db.get_clusters_info()
        
        # Convert to response model
        result = []
        for c in clusters:
            result.append(ClusterInfo(
                cluster_id=c['cluster_id'],
                cluster_label=c['cluster_label'],
                person_count=c['person_count'],
                top_occupations=c['top_occupations'],
                avg_coordinates=Coordinate3D(**c['avg_coordinates'])
            ))
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching clusters: {str(e)}")


@app.get("/api/v1/cluster/{cluster_id}", response_model=ClusterInfo, tags=["clusters"])
async def get_cluster(cluster_id: int):
    """
    Get information about a specific cluster
    
    - **cluster_id**: Cluster identifier (0-14)
    """
    try:
        clusters = db.get_clusters_info()
        
        cluster = next((c for c in clusters if c['cluster_id'] == cluster_id), None)
        
        if not cluster:
            raise HTTPException(status_code=404, detail=f"Cluster {cluster_id} not found")
        
        return ClusterInfo(
            cluster_id=cluster['cluster_id'],
            cluster_label=cluster['cluster_label'],
            person_count=cluster['person_count'],
            top_occupations=cluster['top_occupations'],
            avg_coordinates=Coordinate3D(**cluster['avg_coordinates'])
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching cluster: {str(e)}")


@app.post("/api/v1/generate-embedding", response_model=UserEmbeddingResponse, tags=["embedding"])
async def generate_user_embedding(request: UserEmbeddingRequest):
    """
    Generate embedding for user's life events and find similar persons
    
    This endpoint:
    1. Creates a narrative from user's life events
    2. Generates 768D embedding using Vertex AI
    3. Projects to 3D coordinates using PCA + UMAP
    4. Finds nearest cluster
    5. Identifies most similar historical persons
    
    **Note:** User data is NOT stored in the database (ephemeral for demo)
    """
    if not _models_loaded:
        raise HTTPException(
            status_code=503,
            detail="Reduction models not loaded. Cannot generate user embeddings."
        )
    
    start_time = time.time()
    
    try:
        # Step 1: Create narrative from events
        narrative = embedding_service.create_narrative_from_events(
            [event.dict() for event in request.life_events],
            name=request.name,
            description=request.description
        )
        
        # Step 2: Generate 768D embedding
        embedding_768d = embedding_service.generate_embedding(narrative)
        
        # Step 3: Load models if not already loaded
        if embedding_service.pca_model is None:
            # Load from saved dim_reduction run
            # For simplicity, we'll regenerate from database
            # In production, save PCA/UMAP models to disk
            raise HTTPException(
                status_code=503,
                detail="PCA/UMAP models not available. Run dim_reduction.py first."
            )
        
        # Step 4: Project to 3D
        coordinates_3d = embedding_service.project_to_3d(embedding_768d)
        
        # Step 5: Find nearest cluster
        clusters = db.get_clusters_info()
        nearest_cluster = embedding_service.find_nearest_cluster(coordinates_3d, clusters)
        
        # Step 6: Find similar persons
        all_person_ids, all_coordinates = db.get_all_coordinates()
        similar_persons_data = embedding_service.find_similar_persons(
            coordinates_3d, all_person_ids, all_coordinates, top_k=10
        )
        
        # Get detailed info for similar persons
        similar_persons = []
        for person_id, distance in similar_persons_data:
            person = db.get_person_by_id(person_id)
            if person:
                # Calculate similarity score (inverse of distance, normalized)
                max_distance = 20.0  # Approximate max distance in 3D space
                similarity_score = max(0.0, 1.0 - (distance / max_distance))
                
                similar_persons.append(SimilarPerson(
                    person_id=person['person_id'],
                    name=person['name'],
                    description=person.get('description'),
                    occupation=person.get('occupation', []),
                    distance=distance,
                    similarity_score=similarity_score,
                    cluster_id=person.get('cluster_id'),
                    cluster_label=person.get('cluster_label')
                ))
        
        processing_time = (time.time() - start_time) * 1000  # Convert to ms
        
        return UserEmbeddingResponse(
            user_coordinates=Coordinate3D(
                x=float(coordinates_3d[0]),
                y=float(coordinates_3d[1]),
                z=float(coordinates_3d[2])
            ),
            nearest_cluster=ClusterInfo(
                cluster_id=nearest_cluster['cluster_id'],
                cluster_label=nearest_cluster['cluster_label'],
                person_count=nearest_cluster['person_count'],
                top_occupations=nearest_cluster['top_occupations'],
                avg_coordinates=Coordinate3D(**nearest_cluster['avg_coordinates'])
            ),
            similar_persons=similar_persons,
            narrative_text=narrative,
            embedding_dimension=768,
            processing_time_ms=processing_time
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating embedding: {str(e)}")


# Error handlers
@app.exception_handler(404)
async def not_found_handler(request, exc):
    return JSONResponse(
        status_code=404,
        content={"detail": "Resource not found"}
    )


@app.exception_handler(500)
async def internal_error_handler(request, exc):
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"}
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
