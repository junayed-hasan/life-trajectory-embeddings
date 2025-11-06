"""
Pydantic Models for LifeEmbedding API
"""

from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import date, datetime


class LifeEvent(BaseModel):
    """Single life event input from user"""
    event_type: str = Field(..., description="Type: education, employment, award, etc.")
    event_title: str = Field(..., description="Title/name of the event")
    event_description: Optional[str] = Field(None, description="Detailed description")
    start_date: Optional[date] = Field(None, description="Event start date")
    end_date: Optional[date] = Field(None, description="Event end date")
    organization: Optional[str] = Field(None, description="Associated organization")
    location: Optional[str] = Field(None, description="Event location")
    
    class Config:
        json_schema_extra = {
            "example": {
                "event_type": "education",
                "event_title": "PhD in Computer Science",
                "event_description": "Specialized in Machine Learning",
                "start_date": "2015-09-01",
                "end_date": "2020-05-15",
                "organization": "Stanford University",
                "location": "Stanford, CA"
            }
        }


class UserEmbeddingRequest(BaseModel):
    """Request to generate embedding for user's life events"""
    name: Optional[str] = Field(None, description="User's name (optional)")
    description: Optional[str] = Field(None, description="Brief bio (optional)")
    life_events: List[LifeEvent] = Field(..., min_length=1, description="List of life events")
    
    class Config:
        json_schema_extra = {
            "example": {
                "name": "John Doe",
                "description": "Software engineer and researcher",
                "life_events": [
                    {
                        "event_type": "education",
                        "event_title": "BS in Computer Science",
                        "start_date": "2010-09-01",
                        "end_date": "2014-05-15",
                        "organization": "MIT"
                    },
                    {
                        "event_type": "employment",
                        "event_title": "Software Engineer",
                        "start_date": "2014-06-01",
                        "organization": "Google"
                    }
                ]
            }
        }


class Coordinate3D(BaseModel):
    """3D coordinates for a person"""
    x: float
    y: float
    z: float


class PersonSummary(BaseModel):
    """Summary information for a person"""
    person_id: str
    name: str
    description: Optional[str]
    occupation: List[str]
    cluster_id: Optional[int]
    cluster_label: Optional[str]
    coordinates: Optional[Coordinate3D]


class PersonDetail(BaseModel):
    """Detailed information for a person"""
    person_id: str
    wikidata_id: str
    name: str
    description: Optional[str]
    occupation: List[str]
    field_of_work: List[str]
    birth_date: Optional[date]
    death_date: Optional[date]
    birth_place: Optional[str]
    death_place: Optional[str]
    coordinates: Optional[Coordinate3D]
    cluster_id: Optional[int]
    cluster_label: Optional[str]
    total_events: int
    event_types: dict


class VisualizationPerson(BaseModel):
    """Person data optimized for 3D visualization"""
    person_id: str
    name: str
    description: Optional[str]
    occupation: List[str]
    x: float
    y: float
    z: float
    cluster_id: int
    cluster_label: str


class VisualizationData(BaseModel):
    """Complete dataset for 3D visualization"""
    persons: List[VisualizationPerson]
    metadata: dict


class ClusterInfo(BaseModel):
    """Information about a cluster"""
    cluster_id: int
    cluster_label: str
    person_count: int
    top_occupations: List[dict]
    avg_coordinates: Coordinate3D


class SimilarPerson(BaseModel):
    """Similar person with distance metric"""
    person_id: str
    name: str
    description: Optional[str]
    occupation: List[str]
    distance: float
    similarity_score: float
    cluster_id: int
    cluster_label: str


class UserEmbeddingResponse(BaseModel):
    """Response after generating user embedding"""
    user_coordinates: Coordinate3D
    nearest_cluster: ClusterInfo
    similar_persons: List[SimilarPerson]
    narrative_text: str
    embedding_dimension: int
    processing_time_ms: float


class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    timestamp: datetime
    version: str
    services: dict
