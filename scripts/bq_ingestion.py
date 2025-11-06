#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BigQuery Data Ingestion Module
Loads cleaned Wikidata crawl data into BigQuery tables
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import uuid
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
import logging
from datetime import datetime
from typing import List, Dict, Any
import config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'{config.LOGS_DIR}/bq_ingestion.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class BigQueryIngestor:
    """Handles data ingestion into BigQuery tables"""
    
    def __init__(self):
        """Initialize BigQuery client and table references"""
        self.client = bigquery.Client(project=config.PROJECT_ID)
        self.dataset_id = config.DATASET_ID
        
        # Table references
        self.persons_table = f"{config.PROJECT_ID}.{self.dataset_id}.persons"
        self.life_events_table = f"{config.PROJECT_ID}.{self.dataset_id}.life_events"
        
        logger.info(f"Initialized BigQuery client for project: {config.PROJECT_ID}")
        logger.info(f"Target dataset: {self.dataset_id}")
    
    def load_cleaned_data(self, filepath: str) -> List[Dict[str, Any]]:
        """Load cleaned JSON data"""
        logger.info(f"Loading cleaned data from {filepath}...")
        
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        logger.info(f"Loaded {len(data)} person records")
        return data
    
    def transform_person_data(self, person: Dict[str, Any]) -> Dict[str, Any]:
        """Transform person data to match BigQuery schema"""
        
        # Generate UUID for person_id
        person_id = str(uuid.uuid4())
        
        # Transform to BigQuery schema
        bq_person = {
            'person_id': person_id,
            'wikidata_id': person.get('wikidata_id'),
            'name': person.get('name'),
            'description': person.get('description'),
            'occupation': person.get('occupation', []),
            'field_of_work': person.get('field_of_work', []),
            'citizenship': person.get('citizenship', []),
            'languages': person.get('languages', []),
            'birth_date': person.get('birth_date'),
            'death_date': person.get('death_date'),
            'birth_place': person.get('birth_place'),
            'death_place': person.get('death_place'),
            'created_at': datetime.utcnow().isoformat()
        }
        
        return bq_person, person_id
    
    def transform_life_events(self, person: Dict[str, Any], person_id: str) -> List[Dict[str, Any]]:
        """Transform life events to match BigQuery schema"""
        
        bq_events = []
        
        for event in person.get('life_events', []):
            # Generate UUID for event_id
            event_id = str(uuid.uuid4())
            
            bq_event = {
                'event_id': event_id,
                'person_id': person_id,
                'event_type': event.get('event_type'),
                'event_title': event.get('event_title'),
                'event_description': event.get('event_description'),
                'start_date': event.get('start_date'),
                'end_date': event.get('end_date'),
                'point_in_time': event.get('point_in_time'),
                'location': event.get('location'),
                'organization': event.get('organization'),
                'role_or_degree': event.get('role_or_degree'),
                'field_or_major': event.get('field_or_major'),
                'sport': event.get('sport'),
                'instrument': event.get('instrument'),
                'source': event.get('source'),
                'created_at': datetime.utcnow().isoformat()
            }
            
            bq_events.append(bq_event)
        
        return bq_events
    
    def insert_persons_batch(self, persons: List[Dict[str, Any]]) -> int:
        """Insert persons data in batch"""
        
        logger.info(f"Inserting {len(persons)} persons into {self.persons_table}...")
        
        try:
            errors = self.client.insert_rows_json(self.persons_table, persons)
            
            if errors:
                logger.error(f"Errors inserting persons: {errors}")
                return 0
            
            logger.info(f"✓ Successfully inserted {len(persons)} persons")
            return len(persons)
            
        except GoogleCloudError as e:
            logger.error(f"BigQuery error inserting persons: {e}")
            return 0
    
    def insert_events_batch(self, events: List[Dict[str, Any]]) -> int:
        """Insert life events data in batch"""
        
        logger.info(f"Inserting {len(events)} life events into {self.life_events_table}...")
        
        try:
            errors = self.client.insert_rows_json(self.life_events_table, events)
            
            if errors:
                logger.error(f"Errors inserting events: {errors}")
                return 0
            
            logger.info(f"✓ Successfully inserted {len(events)} life events")
            return len(events)
            
        except GoogleCloudError as e:
            logger.error(f"BigQuery error inserting events: {e}")
            return 0
    
    def ingest_data(self, data: List[Dict[str, Any]], batch_size: int = 100):
        """Main ingestion process with batching"""
        
        logger.info("="*60)
        logger.info("STARTING DATA INGESTION")
        logger.info("="*60)
        
        total_persons = 0
        total_events = 0
        failed_persons = 0
        
        persons_batch = []
        events_batch = []
        
        for i, person in enumerate(data, 1):
            try:
                # Transform person data
                bq_person, person_id = self.transform_person_data(person)
                persons_batch.append(bq_person)
                
                # Transform life events
                bq_events = self.transform_life_events(person, person_id)
                events_batch.extend(bq_events)
                
                # Insert in batches
                if len(persons_batch) >= batch_size:
                    success = self.insert_persons_batch(persons_batch)
                    total_persons += success
                    if success == 0:
                        failed_persons += len(persons_batch)
                    persons_batch = []
                
                if len(events_batch) >= batch_size * 10:  # Larger batch for events
                    success = self.insert_events_batch(events_batch)
                    total_events += success
                    events_batch = []
                
                # Progress logging
                if i % 50 == 0:
                    logger.info(f"Progress: {i}/{len(data)} persons processed")
                    
            except Exception as e:
                logger.error(f"Error processing person {person.get('name', 'Unknown')}: {e}")
                failed_persons += 1
        
        # Insert remaining batches
        if persons_batch:
            success = self.insert_persons_batch(persons_batch)
            total_persons += success
            if success == 0:
                failed_persons += len(persons_batch)
        
        if events_batch:
            success = self.insert_events_batch(events_batch)
            total_events += success
        
        # Summary
        logger.info("\n" + "="*60)
        logger.info("INGESTION COMPLETE")
        logger.info("="*60)
        logger.info(f"Total persons inserted: {total_persons}")
        logger.info(f"Total events inserted: {total_events}")
        logger.info(f"Failed persons: {failed_persons}")
        logger.info("="*60)
        
        return total_persons, total_events, failed_persons
    
    def validate_ingestion(self):
        """Validate data after ingestion"""
        
        logger.info("\n" + "="*60)
        logger.info("VALIDATING INGESTION")
        logger.info("="*60)
        
        queries = {
            "Persons count": f"SELECT COUNT(*) as count FROM `{self.persons_table}`",
            "Events count": f"SELECT COUNT(*) as count FROM `{self.life_events_table}`",
            "Events per person": f"""
                SELECT 
                    COUNT(DISTINCT person_id) as persons,
                    COUNT(*) as events,
                    ROUND(COUNT(*) / COUNT(DISTINCT person_id), 2) as avg_events_per_person
                FROM `{self.life_events_table}`
            """,
            "Event type breakdown": f"""
                SELECT 
                    event_type,
                    COUNT(*) as count
                FROM `{self.life_events_table}`
                GROUP BY event_type
                ORDER BY count DESC
                LIMIT 10
            """,
            "Persons without events": f"""
                SELECT COUNT(*) as count
                FROM `{self.persons_table}` p
                LEFT JOIN `{self.life_events_table}` e ON p.person_id = e.person_id
                WHERE e.event_id IS NULL
            """,
            "Occupation distribution": f"""
                SELECT 
                    occupation,
                    COUNT(*) as count
                FROM `{self.persons_table}`,
                UNNEST(occupation) as occupation
                GROUP BY occupation
                ORDER BY count DESC
                LIMIT 10
            """
        }
        
        for query_name, query in queries.items():
            logger.info(f"\n{query_name}:")
            try:
                result = self.client.query(query).result()
                
                for row in result:
                    row_dict = dict(row)
                    logger.info(f"  {row_dict}")
                    
            except GoogleCloudError as e:
                logger.error(f"Error running validation query: {e}")
        
        logger.info("\n" + "="*60)


def main():
    """Main execution"""
    
    # Configuration
    input_file = os.path.join(config.DATA_DIR, "processed", "wikidata_people_cleaned.json")
    
    # Check if file exists
    if not os.path.exists(input_file):
        logger.error(f"Cleaned data file not found: {input_file}")
        logger.error("Please run eda_and_cleaning.py first")
        return
    
    # Initialize ingestor
    ingestor = BigQueryIngestor()
    
    # Load cleaned data
    data = ingestor.load_cleaned_data(input_file)
    
    # Confirm before ingestion
    logger.info(f"\nReady to ingest {len(data)} persons into BigQuery")
    logger.info(f"Target dataset: {config.DATASET_ID}")
    logger.info(f"Tables: persons, life_events")
    
    # Ingest data
    total_persons, total_events, failed = ingestor.ingest_data(data, batch_size=100)
    
    # Validate
    if total_persons > 0:
        ingestor.validate_ingestion()
    else:
        logger.error("No data was ingested. Check errors above.")
    
    logger.info("\n✓ Ingestion process complete")
    logger.info(f"Check logs at: {config.LOGS_DIR}/bq_ingestion.log")


if __name__ == "__main__":
    main()
