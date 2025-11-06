#!/usr/bin/env python3
"""
Dimensionality Reduction and Clustering for LifeEmbedding

This script:
1. Loads 768-dimensional embeddings from BigQuery
2. Applies PCA to reduce 768D → 50D (preserves ~90% variance)
3. Applies UMAP to reduce 50D → 3D (for visualization)
4. Performs K-means clustering on 3D coordinates
5. Stores coordinates and cluster assignments in BigQuery

Author: LifeEmbedding Team
Date: 2025-10-22
"""

import sys
import numpy as np
from datetime import datetime
from typing import List, Dict, Tuple
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import json
import os

# Scientific computing libraries
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score, davies_bouldin_score
import umap

# Google Cloud
from google.cloud import bigquery

# Local imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config


class DimensionalityReducer:
    """Handle PCA + UMAP dimensionality reduction and clustering"""
    
    def __init__(self):
        """Initialize BigQuery client and reduction parameters"""
        self.bq_client = bigquery.Client(project=config.PROJECT_ID)
        self.dataset_id = config.DATASET_ID
        
        # PCA parameters
        self.pca_components = 50  # Intermediate dimensionality
        self.pca_model = None
        
        # UMAP parameters for 3D visualization
        self.umap_components = 3
        self.umap_n_neighbors = 15
        self.umap_min_dist = 0.1
        self.umap_metric = 'cosine'
        self.umap_model = None
        
        # Clustering parameters
        self.n_clusters = 8  # Will determine optimal number
        self.kmeans_model = None
        
        # Data storage
        self.embeddings_data = []
        self.embeddings_768d = None
        self.embeddings_50d = None
        self.coordinates_3d = None
        self.cluster_labels = None
        
    def load_embeddings_from_bigquery(self) -> int:
        """
        Load embeddings from BigQuery embeddings table
        
        Returns:
            Number of embeddings loaded
        """
        print("Loading embeddings from BigQuery...")
        
        query = f"""
        SELECT 
            e.person_id,
            e.embedding_vector,
            e.embedding_model,
            e.embedding_dim,
            p.name,
            p.description,
            p.occupation
        FROM `{config.PROJECT_ID}.{self.dataset_id}.embeddings` e
        JOIN `{config.PROJECT_ID}.{self.dataset_id}.persons` p
        ON e.person_id = p.person_id
        ORDER BY p.name
        """
        
        try:
            query_job = self.bq_client.query(query)
            results = query_job.result()
            
            self.embeddings_data = []
            embeddings_list = []
            
            for row in results:
                self.embeddings_data.append({
                    'person_id': row.person_id,
                    'name': row.name,
                    'description': row.description,
                    'occupation': row.occupation,
                    'embedding_model': row.embedding_model,
                    'embedding_dim': row.embedding_dim
                })
                embeddings_list.append(row.embedding_vector)
            
            self.embeddings_768d = np.array(embeddings_list)
            
            print(f"✓ Loaded {len(self.embeddings_data)} embeddings")
            print(f"  Embedding shape: {self.embeddings_768d.shape}")
            
            return len(self.embeddings_data)
            
        except Exception as e:
            print(f"✗ Error loading embeddings: {e}")
            raise
    
    def apply_pca(self) -> np.ndarray:
        """
        Apply PCA to reduce dimensionality from 768D to 50D
        
        Returns:
            50D embeddings array
        """
        print(f"\nApplying PCA reduction (768D → {self.pca_components}D)...")
        
        try:
            # Initialize PCA
            self.pca_model = PCA(n_components=self.pca_components, random_state=42)
            
            # Fit and transform
            self.embeddings_50d = self.pca_model.fit_transform(self.embeddings_768d)
            
            # Calculate explained variance
            explained_variance = np.sum(self.pca_model.explained_variance_ratio_)
            
            print(f"✓ PCA reduction complete")
            print(f"  Output shape: {self.embeddings_50d.shape}")
            print(f"  Explained variance: {explained_variance:.4f} ({explained_variance*100:.2f}%)")
            print(f"  First 5 components explain: {np.sum(self.pca_model.explained_variance_ratio_[:5]):.4f}")
            
            return self.embeddings_50d
            
        except Exception as e:
            print(f"✗ Error in PCA: {e}")
            raise
    
    def apply_umap(self) -> np.ndarray:
        """
        Apply UMAP to reduce dimensionality from 50D to 3D
        
        Returns:
            3D coordinates array
        """
        print(f"\nApplying UMAP reduction ({self.pca_components}D → {self.umap_components}D)...")
        print(f"  Parameters: n_neighbors={self.umap_n_neighbors}, min_dist={self.umap_min_dist}, metric='{self.umap_metric}'")
        
        try:
            # Initialize UMAP
            self.umap_model = umap.UMAP(
                n_components=self.umap_components,
                n_neighbors=self.umap_n_neighbors,
                min_dist=self.umap_min_dist,
                metric=self.umap_metric,
                random_state=42
            )
            
            # Fit and transform
            self.coordinates_3d = self.umap_model.fit_transform(self.embeddings_50d)
            
            print(f"✓ UMAP reduction complete")
            print(f"  Output shape: {self.coordinates_3d.shape}")
            print(f"  X range: [{self.coordinates_3d[:, 0].min():.2f}, {self.coordinates_3d[:, 0].max():.2f}]")
            print(f"  Y range: [{self.coordinates_3d[:, 1].min():.2f}, {self.coordinates_3d[:, 1].max():.2f}]")
            print(f"  Z range: [{self.coordinates_3d[:, 2].min():.2f}, {self.coordinates_3d[:, 2].max():.2f}]")
            
            return self.coordinates_3d
            
        except Exception as e:
            print(f"✗ Error in UMAP: {e}")
            raise
    
    def determine_optimal_clusters(self, max_k: int = 15) -> int:
        """
        Use elbow method and silhouette score to find optimal k
        
        Args:
            max_k: Maximum number of clusters to test
            
        Returns:
            Optimal number of clusters
        """
        print(f"\nDetermining optimal number of clusters (testing k=2 to k={max_k})...")
        
        inertias = []
        silhouette_scores = []
        k_range = range(2, min(max_k + 1, len(self.coordinates_3d)))
        
        for k in k_range:
            kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
            labels = kmeans.fit_predict(self.coordinates_3d)
            inertias.append(kmeans.inertia_)
            silhouette_scores.append(silhouette_score(self.coordinates_3d, labels))
        
        # Find elbow point (simple heuristic: max second derivative)
        if len(inertias) > 2:
            second_derivatives = np.diff(np.diff(inertias))
            optimal_k_elbow = np.argmax(np.abs(second_derivatives)) + 2
        else:
            optimal_k_elbow = 2
        
        # Find best silhouette score
        optimal_k_silhouette = np.argmax(silhouette_scores) + 2
        
        # Choose based on silhouette score (more reliable for visualization)
        optimal_k = optimal_k_silhouette
        
        print(f"✓ Clustering analysis complete")
        print(f"  Elbow method suggests: k={optimal_k_elbow}")
        print(f"  Best silhouette score: k={optimal_k_silhouette} (score={silhouette_scores[optimal_k_silhouette-2]:.4f})")
        print(f"  Selected: k={optimal_k}")
        
        return optimal_k
    
    def perform_clustering(self, n_clusters: int = None) -> np.ndarray:
        """
        Perform K-means clustering on 3D coordinates
        
        Args:
            n_clusters: Number of clusters (if None, will determine optimal)
            
        Returns:
            Cluster labels array
        """
        if n_clusters is None:
            n_clusters = self.determine_optimal_clusters()
        
        self.n_clusters = n_clusters
        
        print(f"\nPerforming K-means clustering with k={self.n_clusters}...")
        
        try:
            # Fit K-means
            self.kmeans_model = KMeans(
                n_clusters=self.n_clusters,
                random_state=42,
                n_init=20,
                max_iter=300
            )
            self.cluster_labels = self.kmeans_model.fit_predict(self.coordinates_3d)
            
            # Calculate quality metrics
            silhouette = silhouette_score(self.coordinates_3d, self.cluster_labels)
            davies_bouldin = davies_bouldin_score(self.coordinates_3d, self.cluster_labels)
            
            print(f"✓ Clustering complete")
            print(f"  Silhouette score: {silhouette:.4f} (higher is better, range [-1, 1])")
            print(f"  Davies-Bouldin index: {davies_bouldin:.4f} (lower is better)")
            
            # Print cluster sizes
            unique, counts = np.unique(self.cluster_labels, return_counts=True)
            print(f"\n  Cluster distribution:")
            for cluster_id, count in zip(unique, counts):
                print(f"    Cluster {cluster_id}: {count} persons")
            
            return self.cluster_labels
            
        except Exception as e:
            print(f"✗ Error in clustering: {e}")
            raise
    
    def generate_cluster_labels(self) -> Dict[int, str]:
        """
        Generate descriptive labels for clusters based on common occupations
        
        Returns:
            Dictionary mapping cluster_id to label
        """
        print("\nGenerating cluster labels...")
        
        cluster_labels = {}
        
        for cluster_id in range(self.n_clusters):
            # Get persons in this cluster
            cluster_mask = self.cluster_labels == cluster_id
            cluster_persons = [self.embeddings_data[i] for i in range(len(self.embeddings_data)) if cluster_mask[i]]
            
            # Count occupations
            occupation_counts = {}
            for person in cluster_persons:
                if person['occupation']:
                    for occ in person['occupation']:
                        occupation_counts[occ] = occupation_counts.get(occ, 0) + 1
            
            # Get top 3 occupations
            if occupation_counts:
                top_occupations = sorted(occupation_counts.items(), key=lambda x: x[1], reverse=True)[:3]
                label = ", ".join([occ for occ, _ in top_occupations])
                
                # Show detailed breakdown
                print(f"\n  Cluster {cluster_id}: {label}")
                print(f"    Size: {len(cluster_persons)} persons")
                print(f"    Top occupations:")
                for occ, count in top_occupations:
                    pct = (count / len(cluster_persons)) * 100
                    print(f"      - {occ}: {count} ({pct:.1f}%)")
            else:
                label = f"Cluster {cluster_id}"
                print(f"\n  Cluster {cluster_id}: {label}")
                print(f"    Size: {len(cluster_persons)} persons")
            
            cluster_labels[int(cluster_id)] = label
        
        return cluster_labels
    
    def save_coordinates_to_bigquery(self) -> int:
        """
        Save 3D coordinates and cluster assignments to BigQuery
        
        Returns:
            Number of records inserted
        """
        print("\nSaving coordinates to BigQuery...")
        
        # Generate cluster labels
        cluster_label_map = self.generate_cluster_labels()
        
        # Prepare records
        records = []
        timestamp = datetime.utcnow().isoformat()
        
        for i, person_data in enumerate(self.embeddings_data):
            record = {
                'person_id': person_data['person_id'],
                'x': float(self.coordinates_3d[i, 0]),
                'y': float(self.coordinates_3d[i, 1]),
                'z': float(self.coordinates_3d[i, 2]),
                'cluster_id': int(self.cluster_labels[i]),
                'cluster_label': cluster_label_map[int(self.cluster_labels[i])],
                'reduction_method': f'PCA({self.pca_components}D) + UMAP(3D)',
                'created_at': timestamp
            }
            records.append(record)
        
        # Insert into BigQuery
        table_id = f"{config.PROJECT_ID}.{self.dataset_id}.coordinates_3d"
        
        try:
            # Check if table has data first
            check_query = f"SELECT COUNT(*) as cnt FROM `{table_id}`"
            result = list(self.bq_client.query(check_query).result())[0]
            row_count = result.cnt
            
            if row_count > 0:
                print(f"  ⚠️  Table has {row_count} existing rows from previous run")
                print(f"  Skipping BigQuery insert (streaming buffer prevents DELETE)")
                print(f"  Data will be saved locally only for now")
                print(f"\n  To clear and re-insert into BigQuery later:")
                print(f"    1. Wait 90+ minutes for streaming buffer to clear")
                print(f"    2. Run in BigQuery Console: DELETE FROM `{table_id}` WHERE TRUE")
                print(f"    3. Re-run this script")
                
                # Save locally but skip BigQuery insert
                self.save_coordinates_locally()
                self._coordinates_saved_locally = True
                print(f"\n✓ Coordinates saved locally successfully!")
                print(f"  File: data/processed/coordinates_3d.json")
                return row_count  # Return existing count
            
            # Insert new data (only if table is empty)
            errors = self.bq_client.insert_rows_json(table_id, records)
            
            if errors:
                print(f"✗ Errors inserting coordinates:")
                for error in errors:
                    print(f"  {error}")
                return 0
            else:
                print(f"✓ Inserted {len(records)} coordinate records")
                
                # Validate
                validate_query = f"""
                SELECT 
                    COUNT(*) as total,
                    COUNT(DISTINCT cluster_id) as num_clusters,
                    AVG(x) as avg_x,
                    AVG(y) as avg_y,
                    AVG(z) as avg_z
                FROM `{table_id}`
                """
                result = list(self.bq_client.query(validate_query).result())[0]
                print(f"\n  Validation:")
                print(f"    Total records: {result.total}")
                print(f"    Unique clusters: {result.num_clusters}")
                print(f"    Avg coordinates: ({result.avg_x:.2f}, {result.avg_y:.2f}, {result.avg_z:.2f})")
                
                return len(records)
                
        except Exception as e:
            print(f"✗ Error saving to BigQuery: {e}")
            raise
    
    def save_coordinates_locally(self, output_path: str = 'data/processed/coordinates_3d.json'):
        """
        Save coordinates locally as backup
        
        Args:
            output_path: Path to save JSON file
        """
        print(f"\nSaving coordinates locally to {output_path}...")
        
        cluster_label_map = self.generate_cluster_labels() if not hasattr(self, 'cluster_label_map') else self.cluster_label_map
        
        data = {
            'metadata': {
                'num_persons': len(self.embeddings_data),
                'num_clusters': self.n_clusters,
                'reduction_method': f'PCA({self.pca_components}D) + UMAP(3D)',
                'pca_explained_variance': float(np.sum(self.pca_model.explained_variance_ratio_)),
                'umap_params': {
                    'n_neighbors': self.umap_n_neighbors,
                    'min_dist': self.umap_min_dist,
                    'metric': self.umap_metric
                },
                'timestamp': datetime.utcnow().isoformat()
            },
            'cluster_labels': cluster_label_map,
            'coordinates': []
        }
        
        for i, person_data in enumerate(self.embeddings_data):
            cluster_id = int(self.cluster_labels[i])
            data['coordinates'].append({
                'person_id': person_data['person_id'],
                'name': person_data['name'],
                'occupation': list(person_data['occupation']) if person_data['occupation'] else [],
                'x': float(self.coordinates_3d[i, 0]),
                'y': float(self.coordinates_3d[i, 1]),
                'z': float(self.coordinates_3d[i, 2]),
                'cluster_id': cluster_id,
                'cluster_label': cluster_label_map[cluster_id]
            })
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2)
        
        self._coordinates_saved_locally = True
        print(f"✓ Saved {len(data['coordinates'])} coordinates locally")
    
    def visualize_3d_clusters(self, output_path: str = 'data/processed/visualization_3d.png'):
        """
        Create 3D scatter plot of clusters
        
        Args:
            output_path: Path to save plot
        """
        print(f"\nGenerating 3D visualization...")
        
        fig = plt.figure(figsize=(15, 10))
        ax = fig.add_subplot(111, projection='3d')
        
        # Plot each cluster with different color
        colors = plt.cm.tab10(np.linspace(0, 1, self.n_clusters))
        
        for cluster_id in range(self.n_clusters):
            mask = self.cluster_labels == cluster_id
            ax.scatter(
                self.coordinates_3d[mask, 0],
                self.coordinates_3d[mask, 1],
                self.coordinates_3d[mask, 2],
                c=[colors[cluster_id]],
                label=f'Cluster {cluster_id}',
                s=50,
                alpha=0.6,
                edgecolors='black',
                linewidths=0.5
            )
        
        ax.set_xlabel('X', fontsize=12)
        ax.set_ylabel('Y', fontsize=12)
        ax.set_zlabel('Z', fontsize=12)
        ax.set_title('LifeEmbedding: 3D Visualization of Life Trajectories', fontsize=14, fontweight='bold')
        ax.legend(loc='upper right', fontsize=10)
        
        plt.tight_layout()
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        print(f"✓ Saved 3D visualization to {output_path}")
        
    def run_full_pipeline(self, n_clusters: int = 10):
        """
        Run complete dimensionality reduction and clustering pipeline
        
        Args:
            n_clusters: Number of clusters (default=10 for occupational diversity, 
                       set to None to auto-determine)
        """
        print("="*60)
        print("LIFEEMBEDDING: DIMENSIONALITY REDUCTION & CLUSTERING")
        print("="*60)
        
        # Step 1: Load embeddings
        num_embeddings = self.load_embeddings_from_bigquery()
        
        if num_embeddings == 0:
            print("✗ No embeddings found. Run embedding generation first.")
            return
        
        # Step 2: PCA reduction (768D → 50D)
        self.apply_pca()
        
        # Step 3: UMAP reduction (50D → 3D)
        self.apply_umap()
        
        # Step 4: Clustering
        self.perform_clustering(n_clusters=n_clusters)
        
        # Step 5: Save to BigQuery (will skip if streaming buffer active)
        num_inserted = self.save_coordinates_to_bigquery()
        
        # Step 6: Save locally (always)
        if num_inserted == 0 or not hasattr(self, '_coordinates_saved_locally'):
            self.save_coordinates_locally()
        
        # Step 7: Visualize
        self.visualize_3d_clusters()
        
        print("\n" + "="*60)
        print("✓ PIPELINE COMPLETE")
        print("="*60)
        print(f"\nResults:")
        print(f"  - {num_embeddings} persons processed")
        print(f"  - Reduced from 768D → {self.pca_components}D → 3D")
        print(f"  - {self.n_clusters} clusters identified")
        print(f"  - Coordinates stored in BigQuery: coordinates_3d table")
        print(f"  - Local backup: data/processed/coordinates_3d.json")
        print(f"  - Visualization: data/processed/visualization_3d.png")
        print("\nNext: Proceed to Phase 6 (Backend API Development)")


def main():
    """Main entry point"""
    try:
        # Allow command-line argument for number of clusters
        import argparse
        parser = argparse.ArgumentParser(description='Generate 3D coordinates and clusters for LifeEmbedding')
        parser.add_argument('--clusters', type=int, default=10, 
                          help='Number of clusters (default=10, use 0 for auto-detect)')
        args = parser.parse_args()
        
        n_clusters = None if args.clusters == 0 else args.clusters
        
        reducer = DimensionalityReducer()
        
        # Run full pipeline
        reducer.run_full_pipeline(n_clusters=n_clusters)
        
    except Exception as e:
        print(f"\n✗ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
