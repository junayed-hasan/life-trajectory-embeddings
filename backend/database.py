"""
BigQuery Database Connection and Query Functions
"""

import sys
import os
from typing import List, Dict, Optional, Tuple
from google.cloud import bigquery
import numpy as np

# Add parent directory to path for config import
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config


class Database:
    """Handle BigQuery database operations"""
    
    def __init__(self):
        """Initialize BigQuery client"""
        self.client = bigquery.Client(project=config.PROJECT_ID)
        self.dataset_id = config.DATASET_ID
        
    def get_all_persons(self, limit: int = 1000, offset: int = 0) -> List[Dict]:
        """
        Get all persons with basic info
        
        Args:
            limit: Maximum number of results
            offset: Offset for pagination
            
        Returns:
            List of person dictionaries
        """
        query = f"""
        SELECT 
            p.person_id,
            p.name,
            p.description,
            p.occupation,
            p.field_of_work,
            p.birth_date,
            p.death_date,
            c.x,
            c.y,
            c.z,
            c.cluster_id,
            c.cluster_label
        FROM `{config.PROJECT_ID}.{self.dataset_id}.persons` p
        LEFT JOIN `{config.PROJECT_ID}.{self.dataset_id}.coordinates_3d` c
        ON p.person_id = c.person_id
        ORDER BY p.name
        LIMIT {limit}
        OFFSET {offset}
        """
        
        results = self.client.query(query).result()
        
        persons = []
        for row in results:
            persons.append({
                'person_id': row.person_id,
                'name': row.name,
                'description': row.description,
                'occupation': row.occupation or [],
                'field_of_work': row.field_of_work or [],
                'birth_date': row.birth_date,
                'death_date': row.death_date,
                'coordinates': {
                    'x': float(row.x) if row.x is not None else None,
                    'y': float(row.y) if row.y is not None else None,
                    'z': float(row.z) if row.z is not None else None
                } if row.x is not None else None,
                'cluster_id': row.cluster_id,
                'cluster_label': row.cluster_label
            })
        
        return persons
    
    def get_person_by_id(self, person_id: str) -> Optional[Dict]:
        """
        Get detailed information for a specific person
        
        Args:
            person_id: Unique person identifier
            
        Returns:
            Person dictionary or None
        """
        query = f"""
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
            c.x,
            c.y,
            c.z,
            c.cluster_id,
            c.cluster_label,
            COUNT(e.event_id) as total_events
        FROM `{config.PROJECT_ID}.{self.dataset_id}.persons` p
        LEFT JOIN `{config.PROJECT_ID}.{self.dataset_id}.coordinates_3d` c
        ON p.person_id = c.person_id
        LEFT JOIN `{config.PROJECT_ID}.{self.dataset_id}.life_events` e
        ON p.person_id = e.person_id
        WHERE p.person_id = @person_id
        GROUP BY p.person_id, p.wikidata_id, p.name, p.description,
                 p.occupation, p.field_of_work, p.birth_date, p.death_date,
                 p.birth_place, p.death_place, c.x, c.y, c.z, c.cluster_id, c.cluster_label
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("person_id", "STRING", person_id)
            ]
        )
        
        results = list(self.client.query(query, job_config=job_config).result())
        
        if not results:
            return None
        
        row = results[0]
        
        # Get event type breakdown
        event_query = f"""
        SELECT event_type, COUNT(*) as count
        FROM `{config.PROJECT_ID}.{self.dataset_id}.life_events`
        WHERE person_id = @person_id
        GROUP BY event_type
        """
        
        event_results = self.client.query(event_query, job_config=job_config).result()
        event_types = {r.event_type: r.count for r in event_results}
        
        return {
            'person_id': row.person_id,
            'wikidata_id': row.wikidata_id,
            'name': row.name,
            'description': row.description,
            'occupation': row.occupation or [],
            'field_of_work': row.field_of_work or [],
            'birth_date': row.birth_date,
            'death_date': row.death_date,
            'birth_place': row.birth_place,
            'death_place': row.death_place,
            'coordinates': {
                'x': float(row.x) if row.x is not None else None,
                'y': float(row.y) if row.y is not None else None,
                'z': float(row.z) if row.z is not None else None
            } if row.x is not None else None,
            'cluster_id': row.cluster_id,
            'cluster_label': row.cluster_label,
            'total_events': row.total_events,
            'event_types': event_types
        }
    
    def get_visualization_data(self) -> List[Dict]:
        """
        Get all persons with 3D coordinates for visualization
        
        Returns:
            List of persons with coordinates
        """
        query = f"""
        SELECT 
            p.person_id,
            p.name,
            p.description,
            p.occupation,
            c.x,
            c.y,
            c.z,
            c.cluster_id,
            c.cluster_label
        FROM `{config.PROJECT_ID}.{self.dataset_id}.persons` p
        INNER JOIN `{config.PROJECT_ID}.{self.dataset_id}.coordinates_3d` c
        ON p.person_id = c.person_id
        ORDER BY p.name
        """
        
        results = self.client.query(query).result()
        
        persons = []
        for row in results:
            persons.append({
                'person_id': row.person_id,
                'name': row.name,
                'description': row.description,
                'occupation': row.occupation or [],
                'x': float(row.x),
                'y': float(row.y),
                'z': float(row.z),
                'cluster_id': int(row.cluster_id),
                'cluster_label': row.cluster_label
            })
        
        return persons
    
    def get_clusters_info(self) -> List[Dict]:
        """
        Get information about all clusters
        
        Returns:
            List of cluster information
        """
        query = f"""
        SELECT 
            c.cluster_id,
            c.cluster_label,
            COUNT(*) as person_count,
            AVG(c.x) as avg_x,
            AVG(c.y) as avg_y,
            AVG(c.z) as avg_z
        FROM `{config.PROJECT_ID}.{self.dataset_id}.coordinates_3d` c
        GROUP BY c.cluster_id, c.cluster_label
        ORDER BY c.cluster_id
        """
        
        results = self.client.query(query).result()
        
        clusters = []
        for row in results:
            cluster_id = int(row.cluster_id)
            
            # Get top occupations for this cluster
            occ_query = f"""
            SELECT occupation, COUNT(*) as count
            FROM `{config.PROJECT_ID}.{self.dataset_id}.persons` p
            INNER JOIN `{config.PROJECT_ID}.{self.dataset_id}.coordinates_3d` c
            ON p.person_id = c.person_id
            CROSS JOIN UNNEST(p.occupation) as occupation
            WHERE c.cluster_id = @cluster_id
            GROUP BY occupation
            ORDER BY count DESC
            LIMIT 5
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("cluster_id", "INT64", cluster_id)
                ]
            )
            
            occ_results = list(self.client.query(occ_query, job_config=job_config).result())
            top_occupations = [{'occupation': r.occupation, 'count': r.count} for r in occ_results]
            
            clusters.append({
                'cluster_id': cluster_id,
                'cluster_label': row.cluster_label,
                'person_count': row.person_count,
                'top_occupations': top_occupations,
                'avg_coordinates': {
                    'x': float(row.avg_x),
                    'y': float(row.avg_y),
                    'z': float(row.avg_z)
                }
            })
        
        return clusters
    
    def get_persons_by_cluster(self, cluster_id: int) -> List[Dict]:
        """
        Get all persons in a specific cluster
        
        Args:
            cluster_id: Cluster identifier
            
        Returns:
            List of persons in cluster
        """
        query = f"""
        SELECT 
            p.person_id,
            p.name,
            p.description,
            p.occupation,
            c.x,
            c.y,
            c.z
        FROM `{config.PROJECT_ID}.{self.dataset_id}.persons` p
        INNER JOIN `{config.PROJECT_ID}.{self.dataset_id}.coordinates_3d` c
        ON p.person_id = c.person_id
        WHERE c.cluster_id = @cluster_id
        ORDER BY p.name
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("cluster_id", "INT64", cluster_id)
            ]
        )
        
        results = self.client.query(query, job_config=job_config).result()
        
        persons = []
        for row in results:
            persons.append({
                'person_id': row.person_id,
                'name': row.name,
                'description': row.description,
                'occupation': row.occupation or [],
                'coordinates': {
                    'x': float(row.x),
                    'y': float(row.y),
                    'z': float(row.z)
                }
            })
        
        return persons
    
    def get_all_embeddings(self) -> Tuple[List[str], np.ndarray]:
        """
        Get all embeddings from database
        
        Returns:
            Tuple of (person_ids, embedding_matrix)
        """
        query = f"""
        SELECT person_id, embedding_vector
        FROM `{config.PROJECT_ID}.{self.dataset_id}.embeddings`
        ORDER BY person_id
        """
        
        results = self.client.query(query).result()
        
        person_ids = []
        embeddings = []
        
        for row in results:
            person_ids.append(row.person_id)
            embeddings.append(row.embedding_vector)
        
        return person_ids, np.array(embeddings)
    
    def get_all_coordinates(self) -> Tuple[List[str], np.ndarray]:
        """
        Get all 3D coordinates from database
        
        Returns:
            Tuple of (person_ids, coordinates_matrix)
        """
        query = f"""
        SELECT person_id, x, y, z
        FROM `{config.PROJECT_ID}.{self.dataset_id}.coordinates_3d`
        ORDER BY person_id
        """
        
        results = self.client.query(query).result()
        
        person_ids = []
        coordinates = []
        
        for row in results:
            person_ids.append(row.person_id)
            coordinates.append([float(row.x), float(row.y), float(row.z)])
        
        return person_ids, np.array(coordinates)
