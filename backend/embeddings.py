"""
Embedding Generation Service for User Input
"""

import sys
import os
from typing import List, Tuple
import numpy as np
from datetime import datetime
from vertexai.language_models import TextEmbeddingModel
import vertexai

# Scientific libraries
from sklearn.decomposition import PCA
import umap

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config


class EmbeddingService:
    """Generate embeddings for user-provided life events"""
    
    def __init__(self):
        """Initialize Vertex AI and load PCA/UMAP models"""
        # Initialize Vertex AI
        vertexai.init(project=config.PROJECT_ID, location=config.VERTEX_AI_REGION)
        self.embedding_model = TextEmbeddingModel.from_pretrained(config.EMBEDDING_MODEL)
        
        # Will load PCA/UMAP models from saved state or retrain
        self.pca_model = None
        self.umap_model = None
        self._models_loaded = False
        
    def load_reduction_models(self, pca_model, umap_model):
        """
        Load pre-trained PCA and UMAP models
        
        Args:
            pca_model: Fitted PCA model
            umap_model: Fitted UMAP model
        """
        self.pca_model = pca_model
        self.umap_model = umap_model
        self._models_loaded = True
        
    def create_narrative_from_events(self, events: List[dict], name: str = None, description: str = None) -> str:
        """
        Create a narrative text from life events (similar to event_text_processor.py)
        
        Args:
            events: List of life event dictionaries
            name: User's name (optional)
            description: User's description (optional)
            
        Returns:
            Narrative text string
        """
        narrative_parts = []
        
        # Add biography if provided
        if name or description:
            bio = f"{name or 'This person'}"
            if description:
                bio += f" is {description}."
            else:
                bio += "."
            narrative_parts.append(bio)
        
        # Group events by type
        event_types = {}
        for event in events:
            event_type = event.get('event_type', 'other')
            if event_type not in event_types:
                event_types[event_type] = []
            event_types[event_type].append(event)
        
        # Create narratives for each type
        
        # Education
        if 'education' in event_types:
            edu_events = sorted(event_types['education'], 
                              key=lambda e: e.get('start_date') or datetime.min.date())
            edu_parts = []
            for event in edu_events:
                title = event.get('event_title', '')
                org = event.get('organization', '')
                if title and org:
                    edu_parts.append(f"{title} from {org}")
                elif title:
                    edu_parts.append(title)
            
            if edu_parts:
                if len(edu_parts) == 1:
                    narrative_parts.append(f"Studied {edu_parts[0]}.")
                else:
                    edu_str = ", ".join(edu_parts[:-1]) + f" and {edu_parts[-1]}"
                    narrative_parts.append(f"Educational background includes {edu_str}.")
        
        # Employment
        if 'employment' in event_types:
            emp_events = sorted(event_types['employment'],
                               key=lambda e: e.get('start_date') or datetime.min.date())
            emp_parts = []
            for event in emp_events:
                title = event.get('event_title', '')
                org = event.get('organization', '')
                if title and org:
                    emp_parts.append(f"{title} at {org}")
                elif org:
                    emp_parts.append(f"worked at {org}")
                elif title:
                    emp_parts.append(title)
            
            if emp_parts:
                if len(emp_parts) <= 3:
                    career_str = ", ".join(emp_parts)
                    narrative_parts.append(f"Career includes {career_str}.")
                else:
                    career_str = ", ".join(emp_parts[:3]) + f" and {len(emp_parts)-3} other positions"
                    narrative_parts.append(f"Career includes {career_str}.")
        
        # Awards
        if 'award' in event_types:
            award_events = event_types['award']
            if len(award_events) <= 5:
                award_names = [e.get('event_title', '') for e in award_events if e.get('event_title')]
                if award_names:
                    awards_str = ", ".join(award_names)
                    narrative_parts.append(f"Received awards: {awards_str}.")
            else:
                narrative_parts.append(f"Received {len(award_events)} awards and honors.")
        
        # Other events
        for event_type in event_types:
            if event_type not in ['education', 'employment', 'award']:
                events_of_type = event_types[event_type]
                if events_of_type:
                    narrative_parts.append(f"Notable {event_type} events: {len(events_of_type)}.")
        
        # Join all parts
        narrative = " ".join(narrative_parts)
        
        # Fallback if no narrative generated
        if not narrative.strip():
            narrative = "This person has a diverse background with various life experiences."
        
        return narrative
    
    def generate_embedding(self, text: str) -> np.ndarray:
        """
        Generate 768-dimensional embedding for text
        
        Args:
            text: Input text
            
        Returns:
            768D embedding vector
        """
        # Call Vertex AI
        embeddings = self.embedding_model.get_embeddings([text])
        embedding_vector = np.array(embeddings[0].values)
        
        return embedding_vector
    
    def project_to_3d(self, embedding_768d: np.ndarray) -> np.ndarray:
        """
        Project 768D embedding to 3D coordinates using pre-trained PCA/UMAP
        
        Args:
            embedding_768d: 768-dimensional embedding
            
        Returns:
            3D coordinates [x, y, z]
        """
        if not self._models_loaded:
            raise RuntimeError("PCA/UMAP models not loaded. Call load_reduction_models() first.")
        
        # PCA: 768D -> 50D
        embedding_50d = self.pca_model.transform(embedding_768d.reshape(1, -1))
        
        # UMAP: 50D -> 3D
        coordinates_3d = self.umap_model.transform(embedding_50d)
        
        return coordinates_3d[0]
    
    def find_nearest_cluster(self, coordinates_3d: np.ndarray, clusters_info: List[dict]) -> dict:
        """
        Find the nearest cluster to given 3D coordinates
        
        Args:
            coordinates_3d: [x, y, z] coordinates
            clusters_info: List of cluster information dicts
            
        Returns:
            Nearest cluster dict
        """
        min_distance = float('inf')
        nearest_cluster = None
        
        for cluster in clusters_info:
            cluster_center = np.array([
                cluster['avg_coordinates']['x'],
                cluster['avg_coordinates']['y'],
                cluster['avg_coordinates']['z']
            ])
            
            distance = np.linalg.norm(coordinates_3d - cluster_center)
            
            if distance < min_distance:
                min_distance = distance
                nearest_cluster = cluster
        
        return nearest_cluster
    
    def find_similar_persons(self, coordinates_3d: np.ndarray, 
                            all_person_ids: List[str], 
                            all_coordinates: np.ndarray,
                            top_k: int = 10) -> List[Tuple[str, float]]:
        """
        Find k most similar persons by 3D distance
        
        Args:
            coordinates_3d: User's [x, y, z] coordinates
            all_person_ids: List of all person IDs
            all_coordinates: Matrix of all 3D coordinates (N x 3)
            top_k: Number of similar persons to return
            
        Returns:
            List of (person_id, distance) tuples
        """
        # Calculate distances to all persons
        distances = np.linalg.norm(all_coordinates - coordinates_3d, axis=1)
        
        # Get top k indices
        top_k_indices = np.argsort(distances)[:top_k]
        
        # Return person IDs and distances
        similar = [(all_person_ids[i], float(distances[i])) for i in top_k_indices]
        
        return similar
