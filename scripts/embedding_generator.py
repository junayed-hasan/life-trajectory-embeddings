#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Embedding Generator
Generates embeddings using Vertex AI and stores them in BigQuery
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google.cloud import bigquery
from vertexai.language_models import TextEmbeddingModel
import vertexai
from typing import List, Tuple, Dict, Any
import logging
from datetime import datetime
import time
import json
import config

# Import our event text processor
from event_text_processor import EventTextProcessor

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'{config.LOGS_DIR}/embedding_generator.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class EmbeddingGenerator:
    """Generates embeddings using Vertex AI text-embedding-004 model"""
    
    def __init__(self):
        """Initialize Vertex AI and BigQuery clients"""
        
        # Initialize Vertex AI
        vertexai.init(project=config.PROJECT_ID, location=config.VERTEX_AI_REGION)
        self.embedding_model = TextEmbeddingModel.from_pretrained(config.EMBEDDING_MODEL)
        
        # Initialize BigQuery
        self.bq_client = bigquery.Client(project=config.PROJECT_ID)
        self.dataset_id = config.DATASET_ID
        self.embeddings_table = f"{config.PROJECT_ID}.{self.dataset_id}.embeddings"
        
        logger.info(f"Initialized EmbeddingGenerator")
        logger.info(f"  Vertex AI region: {config.VERTEX_AI_REGION}")
        logger.info(f"  Model: {config.EMBEDDING_MODEL}")
        logger.info(f"  Target table: {self.embeddings_table}")
    
    def generate_embedding(self, text: str) -> List[float]:
        """
        Generate embedding for a single text
        
        Args:
            text: Input text to embed
        
        Returns:
            List of floats representing the embedding vector
        """
        
        try:
            # Truncate text if too long (Vertex AI limit is ~20k tokens)
            if len(text) > 10000:
                text = text[:10000]
                logger.warning(f"Text truncated to 10000 characters")
            
            # Get embedding
            embeddings = self.embedding_model.get_embeddings([text])
            
            # Extract the vector
            vector = embeddings[0].values
            
            return vector
            
        except Exception as e:
            logger.error(f"Error generating embedding: {e}")
            raise
    
    def generate_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        """
        Generate embeddings for multiple texts in batch
        
        Args:
            texts: List of texts to embed (max 5 for Vertex AI)
        
        Returns:
            List of embedding vectors
        """
        
        try:
            # Vertex AI recommends max 5 texts per batch
            if len(texts) > 5:
                logger.warning(f"Batch size {len(texts)} exceeds recommended limit of 5")
            
            # Truncate long texts
            truncated_texts = []
            for text in texts:
                if len(text) > 10000:
                    truncated_texts.append(text[:10000])
                else:
                    truncated_texts.append(text)
            
            # Get embeddings (no task_type parameter for older SDK)
            embeddings = self.embedding_model.get_embeddings(truncated_texts)
            
            # Extract vectors
            vectors = [emb.values for emb in embeddings]
            
            return vectors
            
        except Exception as e:
            logger.error(f"Error generating batch embeddings: {e}")
            raise
    
    def load_processed_narratives(self, filepath: str) -> List[Tuple[str, str, str, str]]:
        """
        Load pre-processed narratives from JSON file
        
        Returns:
            List of tuples: (person_id, name, narrative_text, metadata_json)
        """
        logger.info(f"Loading pre-processed narratives from {filepath}...")
        
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        results = []
        for item in data:
            person_id = item['person_id']
            name = item['name']
            narrative = item['narrative']
            metadata = str(item['metadata'])
            results.append((person_id, name, narrative, metadata))
        
        logger.info(f"✓ Loaded {len(results)} pre-processed narratives")
        return results
    
    def process_and_embed_all(self, batch_size: int = 5, use_cached: bool = True) -> List[Dict[str, Any]]:
        """
        Process all persons, generate narratives, and create embeddings
        
        Args:
            batch_size: Number of texts to embed per API call (max 5 for Vertex AI)
            use_cached: If True, load pre-processed narratives from file if available
        
        Returns:
            List of embedding records ready for BigQuery insertion
        """
        
        logger.info("="*60)
        logger.info("GENERATING EMBEDDINGS FOR ALL PERSONS")
        logger.info("="*60)
        
        # Try to load cached narratives first
        narratives_file = os.path.join(config.DATA_DIR, "processed", "person_narratives.json")
        
        if use_cached and os.path.exists(narratives_file):
            logger.info(f"Found cached narratives at {narratives_file}")
            person_data = self.load_processed_narratives(narratives_file)
        else:
            logger.info("Processing narratives from BigQuery (no cache found or use_cached=False)")
            processor = EventTextProcessor()
            person_data = processor.process_all_persons()
        
        logger.info(f"\nGenerating embeddings for {len(person_data)} persons...")
        logger.info(f"Batch size: {batch_size}")
        
        embedding_records = []
        total_api_calls = 0
        
        # Process in batches
        for i in range(0, len(person_data), batch_size):
            batch = person_data[i:i+batch_size]
            
            # Extract texts and metadata
            batch_texts = [narrative for _, _, narrative, _ in batch]
            batch_person_ids = [person_id for person_id, _, _, _ in batch]
            batch_names = [name for _, name, _, _ in batch]
            
            try:
                # Generate embeddings for batch
                vectors = self.generate_embeddings_batch(batch_texts)
                total_api_calls += 1
                
                # Create records for BigQuery (match the actual table schema)
                for j, vector in enumerate(vectors):
                    person_id = batch_person_ids[j]
                    name = batch_names[j]
                    narrative = batch_texts[j]
                    
                    record = {
                        'person_id': person_id,
                        'embedding_vector': vector,
                        'embedding_model': config.EMBEDDING_MODEL,  # Changed from model_name
                        'embedding_dim': len(vector),  # Changed from embedding_dimension
                        'embedding_text': narrative[:1000],  # Changed from source_text
                        'created_at': datetime.utcnow().isoformat()
                    }
                    
                    embedding_records.append(record)
                
                # Progress logging
                if (i // batch_size + 1) % 10 == 0:
                    logger.info(f"  Processed {i+len(batch)}/{len(person_data)} persons ({total_api_calls} API calls)")
                
                # Rate limiting (Vertex AI has generous limits, but be safe)
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Error processing batch at index {i}: {e}")
                # Continue with next batch
                continue
        
        logger.info(f"\n✓ Generated {len(embedding_records)} embeddings")
        logger.info(f"  Total API calls: {total_api_calls}")
        logger.info(f"  Embedding dimension: {embedding_records[0]['embedding_dim'] if embedding_records else 'N/A'}")
        
        # Save embeddings locally
        self._save_embeddings_locally(embedding_records)
        
        return embedding_records
    
    def _save_embeddings_locally(self, embedding_records: List[Dict[str, Any]]):
        """Save embeddings to local JSON file as backup"""
        
        embeddings_file = os.path.join(config.DATA_DIR, "embeddings", "person_embeddings.json")
        
        logger.info(f"\nSaving embeddings locally to {embeddings_file}...")
        
        with open(embeddings_file, 'w', encoding='utf-8') as f:
            json.dump(embedding_records, f, indent=2, ensure_ascii=False)
        
        logger.info(f"✓ Saved {len(embedding_records)} embeddings locally")
    
    def load_embeddings_from_local(self, filepath: str) -> List[Dict[str, Any]]:
        """Load embeddings from local JSON file"""
        
        logger.info(f"Loading embeddings from {filepath}...")
        
        with open(filepath, 'r', encoding='utf-8') as f:
            embedding_records = json.load(f)
        
        logger.info(f"✓ Loaded {len(embedding_records)} embeddings from local file")
        return embedding_records
    
    def insert_embeddings_to_bigquery(self, embedding_records: List[Dict[str, Any]], batch_size: int = 100):
        """
        Insert embedding records into BigQuery
        
        Args:
            embedding_records: List of embedding records
            batch_size: Number of records per insert batch
        """
        
        logger.info(f"\nInserting {len(embedding_records)} embeddings into BigQuery...")
        
        total_inserted = 0
        failed_inserts = 0
        
        for i in range(0, len(embedding_records), batch_size):
            batch = embedding_records[i:i+batch_size]
            
            try:
                errors = self.bq_client.insert_rows_json(self.embeddings_table, batch)
                
                if errors:
                    logger.error(f"Errors inserting batch {i//batch_size + 1}: {errors}")
                    failed_inserts += len(batch)
                else:
                    total_inserted += len(batch)
                    
                if (i // batch_size + 1) % 5 == 0:
                    logger.info(f"  Inserted {total_inserted} embeddings so far...")
                    
            except Exception as e:
                logger.error(f"Error inserting batch at index {i}: {e}")
                failed_inserts += len(batch)
        
        logger.info(f"\n✓ Insertion complete")
        logger.info(f"  Successfully inserted: {total_inserted}")
        logger.info(f"  Failed: {failed_inserts}")
        
        return total_inserted, failed_inserts
    
    def validate_embeddings(self):
        """Validate embeddings in BigQuery"""
        
        logger.info("\n" + "="*60)
        logger.info("VALIDATING EMBEDDINGS")
        logger.info("="*60)
        
        queries = {
            "Total embeddings": f"""
                SELECT COUNT(*) as count 
                FROM `{self.embeddings_table}`
            """,
            "Embedding dimensions": f"""
                SELECT 
                    embedding_dim,
                    COUNT(*) as count
                FROM `{self.embeddings_table}`
                GROUP BY embedding_dim
            """,
            "Persons with embeddings": f"""
                SELECT COUNT(DISTINCT person_id) as count
                FROM `{self.embeddings_table}`
            """,
            "Persons without embeddings": f"""
                SELECT COUNT(*) as count
                FROM `{config.PROJECT_ID}.{self.dataset_id}.persons` p
                LEFT JOIN `{self.embeddings_table}` e ON p.person_id = e.person_id
                WHERE e.person_id IS NULL
            """,
            "Sample embedding stats": f"""
                SELECT 
                    person_id,
                    embedding_dim,
                    LENGTH(embedding_text) as text_length,
                    created_at
                FROM `{self.embeddings_table}`
                LIMIT 5
            """
        }
        
        for query_name, query in queries.items():
            logger.info(f"\n{query_name}:")
            try:
                result = self.bq_client.query(query).result()
                for row in result:
                    logger.info(f"  {dict(row)}")
            except Exception as e:
                logger.error(f"Error running query: {e}")


def main():
    """Main execution"""
    
    logger.info("="*60)
    logger.info("EMBEDDING GENERATION PIPELINE")
    logger.info("="*60)
    
    # Initialize generator
    generator = EmbeddingGenerator()
    
    # Check if embeddings already exist locally
    embeddings_file = os.path.join(config.DATA_DIR, "embeddings", "person_embeddings.json")
    
    if os.path.exists(embeddings_file):
        logger.info(f"\n✓ Found existing embeddings at {embeddings_file}")
        logger.info("Loading from local file instead of regenerating...")
        embedding_records = generator.load_embeddings_from_local(embeddings_file)
    else:
        logger.info("\nNo local embeddings found. Generating new embeddings...")
        # Generate embeddings for all persons
        embedding_records = generator.process_and_embed_all(batch_size=5)
    
    if not embedding_records:
        logger.error("No embeddings available. Exiting.")
        return
    
    # Insert into BigQuery
    logger.info("\nInserting embeddings into BigQuery...")
    total_inserted, failed = generator.insert_embeddings_to_bigquery(
        embedding_records, 
        batch_size=100
    )
    
    # Validate
    if total_inserted > 0:
        generator.validate_embeddings()
    
    # Summary
    logger.info("\n" + "="*60)
    logger.info("EMBEDDING GENERATION COMPLETE")
    logger.info("="*60)
    logger.info(f"Total persons processed: {len(embedding_records)}")
    logger.info(f"Embeddings inserted: {total_inserted}")
    logger.info(f"Failed insertions: {failed}")
    logger.info(f"Model used: {config.EMBEDDING_MODEL}")
    logger.info(f"Embedding dimension: {embedding_records[0]['embedding_dim']}")
    logger.info("="*60)
    
    logger.info(f"\n✓ Check logs at: {config.LOGS_DIR}/embedding_generator.log")


if __name__ == "__main__":
    main()
