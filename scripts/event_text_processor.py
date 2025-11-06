#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Event Text Processor
Fetches life events from BigQuery and prepares narrative text for embedding generation
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google.cloud import bigquery
from typing import List, Dict, Any, Tuple
import logging
from datetime import datetime
import json
import config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'{config.LOGS_DIR}/event_text_processor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class EventTextProcessor:
    """Processes life events into narrative text for embeddings"""
    
    def __init__(self):
        """Initialize BigQuery client"""
        self.client = bigquery.Client(project=config.PROJECT_ID)
        self.dataset_id = config.DATASET_ID
        logger.info(f"Initialized EventTextProcessor for project: {config.PROJECT_ID}")
    
    def fetch_all_persons(self) -> List[Dict[str, Any]]:
        """Fetch all persons from BigQuery"""
        
        query = f"""
        SELECT 
            person_id,
            wikidata_id,
            name,
            description,
            occupation,
            field_of_work,
            birth_date,
            death_date,
            birth_place,
            death_place
        FROM `{config.PROJECT_ID}.{self.dataset_id}.persons`
        ORDER BY name
        """
        
        logger.info("Fetching all persons from BigQuery...")
        result = self.client.query(query).result()
        
        persons = []
        for row in result:
            persons.append(dict(row))
        
        logger.info(f"Fetched {len(persons)} persons")
        return persons
    
    def fetch_life_events_for_person(self, person_id: str) -> List[Dict[str, Any]]:
        """Fetch all life events for a specific person, sorted chronologically"""
        
        query = f"""
        SELECT 
            event_id,
            event_type,
            event_title,
            event_description,
            start_date,
            end_date,
            point_in_time,
            location,
            organization,
            role_or_degree,
            field_or_major
        FROM `{config.PROJECT_ID}.{self.dataset_id}.life_events`
        WHERE person_id = @person_id
        ORDER BY 
            COALESCE(start_date, point_in_time, end_date, '9999-12-31') ASC
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("person_id", "STRING", person_id)
            ]
        )
        
        result = self.client.query(query, job_config=job_config).result()
        
        events = []
        for row in result:
            events.append(dict(row))
        
        return events
    
    def format_date(self, date_value) -> str:
        """
        Format date for narrative (e.g., '1973-01-01' -> 'January 1973')
        Handles both string and datetime.date objects from BigQuery
        """
        if not date_value:
            return ""
        
        try:
            # Handle datetime.date objects from BigQuery
            if hasattr(date_value, 'year'):
                year = str(date_value.year)
                month = date_value.month if date_value.month != 1 else None
            else:
                # Handle string dates
                parts = str(date_value).split('-')
                year = parts[0]
                month = int(parts[1]) if len(parts) > 1 and parts[1] != '01' else None
            
            month_names = {
                1: 'January', 2: 'February', 3: 'March', 4: 'April',
                5: 'May', 6: 'June', 7: 'July', 8: 'August',
                9: 'September', 10: 'October', 11: 'November', 12: 'December'
            }
            
            if month and month in month_names:
                return f"{month_names[month]} {year}"
            return year
        except Exception as e:
            logger.warning(f"Error formatting date {date_value}: {e}")
            return str(date_value)
    
    def is_event_valid_for_narrative(self, event: Dict[str, Any]) -> bool:
        """
        Check if event has enough information to create a meaningful narrative
        
        Valid event must have at least ONE of:
        - organization (for work/education)
        - event_title (for awards/works)
        - location (for residence/work location)
        
        AND at least one temporal field (start_date, end_date, or point_in_time)
        """
        
        has_content = any([
            event.get('organization'),
            event.get('event_title'),
            event.get('location')
        ])
        
        has_temporal = any([
            event.get('start_date'),
            event.get('end_date'),
            event.get('point_in_time')
        ])
        
        return has_content and has_temporal
    
    def create_event_narrative(self, event: Dict[str, Any]) -> str:
        """
        Create a natural language narrative for a single event
        Returns empty string if event lacks sufficient information
        
        Example outputs:
        - "In 1973, studied physics at The Queen's College, Oxford, earning a degree in physics"
        - "From 1976 to 2005, worked at Bell Labs as a researcher"
        - "In 1985, received the Nobel Prize in Physics"
        """
        
        # Skip if not enough information
        if not self.is_event_valid_for_narrative(event):
            return ""
        
        parts = []
        
        # Temporal information
        start_date = event.get('start_date')
        end_date = event.get('end_date')
        point_in_time = event.get('point_in_time')
        
        if start_date and end_date:
            parts.append(f"From {self.format_date(start_date)} to {self.format_date(end_date)}")
        elif start_date:
            parts.append(f"Starting in {self.format_date(start_date)}")
        elif point_in_time:
            parts.append(f"In {self.format_date(point_in_time)}")
        elif end_date:
            parts.append(f"Until {self.format_date(end_date)}")
        
        # Event type verb mapping
        event_type = event.get('event_type', '').lower()
        
        # Main content
        organization = event.get('organization')
        event_title = event.get('event_title')
        location = event.get('location')
        role_or_degree = event.get('role_or_degree')
        field_or_major = event.get('field_or_major')
        
        # Build the narrative based on event type and available data
        if event_type == 'education':
            if not organization and not location:
                return ""  # Skip if no school/location
            
            if field_or_major:
                parts.append(f"studied {field_or_major}")
            else:
                parts.append("studied")
            
            if organization:
                parts.append(f"at {organization}")
            elif location:
                parts.append(f"in {location}")
            
            if role_or_degree:
                parts.append(f"earning a {role_or_degree}")
                
        elif event_type in ['employment', 'position']:
            if not organization and not location:
                return ""  # Skip if no employer/location
            
            if role_or_degree:
                parts.append(f"worked as {role_or_degree}")
            else:
                parts.append("worked")
            
            if organization:
                parts.append(f"at {organization}")
            elif location:
                parts.append(f"in {location}")
            
            if field_or_major and not role_or_degree:
                parts.append(f"in {field_or_major}")
                
        elif event_type == 'work_location':
            if not location and not organization:
                return ""  # Skip if no location
            
            parts.append("worked in")
            if location:
                parts.append(location)
            elif organization:
                parts.append(f"the {organization} area")
                
        elif event_type == 'residence':
            if not location and not organization:
                return ""  # Skip if no location
            
            parts.append("lived in")
            if location:
                parts.append(location)
            elif organization:
                parts.append(organization)
                
        elif event_type == 'award':
            if not event_title and not organization:
                return ""  # Skip if no award name
            
            parts.append("received")
            if event_title:
                parts.append(event_title)
            elif organization:
                parts.append(f"an award from {organization}")
            
            if location and 'in ' not in ' '.join(parts):
                parts.append(f"in {location}")
                
        elif event_type == 'notable_work':
            if not event_title and not organization:
                return ""  # Skip if no work name
            
            parts.append("created")
            if event_title:
                parts.append(event_title)
            elif organization:
                parts.append(organization)
                
        elif event_type in ['significant_event', 'participant_in']:
            if not event_title and not organization:
                return ""  # Skip if no event name
            
            parts.append("participated in")
            if event_title:
                parts.append(event_title)
            elif organization:
                parts.append(organization)
            
            if location and 'in ' not in ' '.join(parts):
                parts.append(f"in {location}")
                
        elif event_type == 'sports_team':
            if not organization:
                return ""  # Skip if no team name
            
            parts.append(f"played for {organization}")
            
            if role_or_degree:
                parts.append(f"as {role_or_degree}")
        
        else:
            # Generic format for unknown event types
            if organization:
                parts.append(f"was associated with {organization}")
            elif event_title:
                parts.append(f"was involved in {event_title}")
            elif location:
                parts.append(f"was active in {location}")
            else:
                return ""  # Skip if nothing meaningful
        
        # Join parts into narrative
        narrative = ' '.join(parts)
        
        # Clean up extra spaces
        narrative = ' '.join(narrative.split())
        
        # Ensure it ends with a period
        if narrative and not narrative.endswith('.'):
            narrative += '.'
        
        return narrative
    
    def create_person_biography(self, person: Dict[str, Any]) -> str:
        """
        Create a biographical paragraph from person metadata
        
        Example:
        "Tim Berners-Lee is an English computer scientist, born in London in 1955.
         He is known for inventing the World Wide Web and is a professor and researcher."
        """
        
        parts = []
        
        name = person.get('name')
        description = person.get('description')
        birth_date = person.get('birth_date')
        birth_place = person.get('birth_place')
        death_date = person.get('death_date')
        occupations = person.get('occupation', [])
        
        # Opening sentence
        if name:
            intro = f"{name} is"
            if death_date:
                intro = f"{name} was"
            
            # Add description or occupations
            if description and description != "No description available":
                parts.append(f"{intro} {description}.")
            elif occupations:
                occ_str = ', '.join(occupations[:3])  # Limit to 3 occupations
                parts.append(f"{intro} a {occ_str}.")
        
        # Birth information
        if birth_date or birth_place:
            birth_info = []
            if birth_place:
                birth_info.append(f"born in {birth_place}")
            if birth_date:
                # Handle both datetime.date objects and strings
                if hasattr(birth_date, 'year'):
                    birth_year = str(birth_date.year)
                else:
                    birth_year = str(birth_date).split('-')[0]
                    
                if birth_info:
                    birth_info.append(f"in {birth_year}")
                else:
                    birth_info.append(f"born in {birth_year}")
            
            if birth_info:
                parts.append(' '.join(birth_info) + '.')
        
        biography = ' '.join(parts)
        return biography
    
    def create_life_narrative(self, person: Dict[str, Any], events: List[Dict[str, Any]]) -> str:
        """
        Create complete life narrative combining biography and chronological events
        Groups events by type and creates more natural flowing narratives
        
        Format:
        [Biography] [Education] [Career] [Notable Works] [Awards] [Other Events]
        """
        
        narratives = []
        
        # Add biography
        bio = self.create_person_biography(person)
        if bio:
            narratives.append(bio)
        
        # Separate events by type
        education_events = []
        career_events = []
        award_events = []
        work_events = []
        residence_events = []
        other_events = []
        
        valid_events = 0
        skipped_events = 0
        
        for event in events:
            if not self.is_event_valid_for_narrative(event):
                skipped_events += 1
                continue
            
            valid_events += 1
            event_type = event.get('event_type', '').lower()
            
            if event_type == 'education':
                education_events.append(event)
            elif event_type in ['employment', 'position']:
                career_events.append(event)
            elif event_type == 'award':
                award_events.append(event)
            elif event_type in ['notable_work', 'participant_in', 'significant_event', 'sports_team']:
                work_events.append(event)
            elif event_type in ['residence', 'work_location']:
                residence_events.append(event)
            else:
                other_events.append(event)
        
        # Add education narrative
        if education_events:
            edu_narrative = self._create_education_narrative(education_events)
            if edu_narrative:
                narratives.append(edu_narrative)
        
        # Add career narrative
        if career_events:
            career_narrative = self._create_career_narrative(career_events)
            if career_narrative:
                narratives.append(career_narrative)
        
        # Add notable works
        if work_events:
            for event in work_events[:5]:  # Limit to top 5 works
                work_narrative = self.create_event_narrative(event)
                if work_narrative:
                    narratives.append(work_narrative)
        
        # Add awards (summarized if many)
        if award_events:
            award_narrative = self._create_awards_narrative(award_events)
            if award_narrative:
                narratives.append(award_narrative)
        
        # Add residence events (limit to avoid clutter)
        if residence_events:
            for event in residence_events[:3]:  # Only first 3 residences
                res_narrative = self.create_event_narrative(event)
                if res_narrative:
                    narratives.append(res_narrative)
        
        # Log if we skipped many events
        if skipped_events > len(events) * 0.5:
            logger.debug(f"Skipped {skipped_events}/{len(events)} events for {person.get('name')} due to missing information")
        
        # Combine into single narrative
        full_narrative = ' '.join(narratives)
        
        return full_narrative
    
    def _create_education_narrative(self, education_events: List[Dict[str, Any]]) -> str:
        """Create a flowing narrative for education events"""
        
        if not education_events:
            return ""
        
        # If only 1-2 education events, use individual narratives
        if len(education_events) <= 2:
            narratives = []
            for event in education_events:
                narrative = self.create_event_narrative(event)
                if narrative:
                    narratives.append(narrative)
            return ' '.join(narratives)
        
        # If many education events, create summary
        schools = []
        for event in education_events:
            org = event.get('organization')
            degree = event.get('role_or_degree')
            field = event.get('field_or_major')
            
            if org:
                school_info = org
                if degree or field:
                    details = []
                    if degree:
                        details.append(degree)
                    if field:
                        details.append(field)
                    school_info += f" ({', '.join(details)})"
                schools.append(school_info)
        
        if schools:
            if len(schools) == 1:
                return f"Studied at {schools[0]}."
            elif len(schools) == 2:
                return f"Studied at {schools[0]} and {schools[1]}."
            else:
                return f"Educated at {', '.join(schools[:3])}."
        
        return ""
    
    def _create_career_narrative(self, career_events: List[Dict[str, Any]]) -> str:
        """Create a flowing narrative for career events"""
        
        if not career_events:
            return ""
        
        # If only 1-2 positions, use individual narratives
        if len(career_events) <= 2:
            narratives = []
            for event in career_events:
                narrative = self.create_event_narrative(event)
                if narrative:
                    narratives.append(narrative)
            return ' '.join(narratives)
        
        # If many positions, create summary
        positions = []
        for event in career_events[:5]:  # Limit to 5 most significant
            org = event.get('organization')
            role = event.get('role_or_degree')
            
            if org:
                if role:
                    positions.append(f"{role} at {org}")
                else:
                    positions.append(org)
        
        if positions:
            if len(positions) == 1:
                return f"Worked as {positions[0]}."
            elif len(positions) == 2:
                return f"Career included positions at {positions[0]} and {positions[1]}."
            else:
                return f"Career included positions at {', '.join(positions[:4])}."
        
        return ""
    
    def _create_awards_narrative(self, award_events: List[Dict[str, Any]]) -> str:
        """Create a flowing narrative for awards"""
        
        if not award_events:
            return ""
        
        # If only 1-2 awards, use individual narratives
        if len(award_events) <= 2:
            narratives = []
            for event in award_events:
                narrative = self.create_event_narrative(event)
                if narrative:
                    narratives.append(narrative)
            return ' '.join(narratives)
        
        # If many awards, create summary
        awards = []
        for event in award_events[:6]:  # Limit to top 6 awards
            title = event.get('event_title')
            org = event.get('organization')
            year = event.get('point_in_time') or event.get('start_date')
            
            if title:
                if year:
                    year_str = self.format_date(year)
                    awards.append(f"{title} ({year_str})")
                else:
                    awards.append(title)
        
        if awards:
            if len(awards) <= 3:
                award_list = ', '.join(awards[:-1]) + f" and {awards[-1]}" if len(awards) > 1 else awards[0]
                return f"Received honors including {award_list}."
            else:
                return f"Received numerous honors including {', '.join(awards[:4])} among others."
        
        return ""
    
    def process_all_persons(self) -> List[Tuple[str, str, str, str]]:
        """
        Process all persons and create narratives
        
        Returns:
            List of tuples: (person_id, name, narrative_text, metadata_json)
        """
        
        logger.info("="*60)
        logger.info("PROCESSING ALL PERSONS FOR EMBEDDING")
        logger.info("="*60)
        
        persons = self.fetch_all_persons()
        results = []
        
        total_events = 0
        total_valid_events = 0
        total_skipped_events = 0
        
        for i, person in enumerate(persons, 1):
            person_id = person['person_id']
            name = person['name']
            
            # Fetch events
            events = self.fetch_life_events_for_person(person_id)
            total_events += len(events)
            
            # Count valid events
            valid_count = sum(1 for e in events if self.is_event_valid_for_narrative(e))
            skipped_count = len(events) - valid_count
            total_valid_events += valid_count
            total_skipped_events += skipped_count
            
            # Create narrative
            narrative = self.create_life_narrative(person, events)
            
            # Create metadata (for tracking)
            metadata = {
                'person_id': person_id,
                'name': name,
                'wikidata_id': person.get('wikidata_id'),
                'total_events': len(events),
                'valid_events': valid_count,
                'skipped_events': skipped_count,
                'narrative_length': len(narrative)
            }
            
            results.append((person_id, name, narrative, str(metadata)))
            
            # Progress logging
            if i % 50 == 0:
                logger.info(f"Processed {i}/{len(persons)} persons")
            
            # Log sample for first person
            if i == 1:
                logger.info(f"\nSample narrative for: {name}")
                logger.info(f"Total events: {len(events)}")
                logger.info(f"Valid events: {valid_count}")
                logger.info(f"Skipped events: {skipped_count}")
                logger.info(f"Narrative length: {len(narrative)} characters")
                logger.info(f"Preview: {narrative[:500]}...")
        
        logger.info(f"\n✓ Processed {len(results)} person narratives")
        logger.info(f"Event statistics:")
        logger.info(f"  Total events: {total_events}")
        logger.info(f"  Valid events: {total_valid_events} ({total_valid_events/total_events*100:.1f}%)")
        logger.info(f"  Skipped events: {total_skipped_events} ({total_skipped_events/total_events*100:.1f}%)")
        
        return results


def main():
    """Test the event text processor"""
    
    processor = EventTextProcessor()
    
    # Process all persons
    results = processor.process_all_persons()
    
    # Save processed narratives to file
    output_file = os.path.join(config.DATA_DIR, "processed", "person_narratives.json")
    logger.info(f"\nSaving processed narratives to {output_file}...")
    
    narratives_data = []
    for person_id, name, narrative, metadata in results:
        narratives_data.append({
            'person_id': person_id,
            'name': name,
            'narrative': narrative,
            'metadata': eval(metadata)  # Convert string back to dict
        })
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(narratives_data, f, indent=2, ensure_ascii=False)
    
    logger.info(f"✓ Saved {len(narratives_data)} narratives to {output_file}")
    
    # Show statistics
    logger.info("\n" + "="*60)
    logger.info("NARRATIVE STATISTICS")
    logger.info("="*60)
    
    narrative_lengths = [len(narrative) for _, _, narrative, _ in results]
    
    logger.info(f"Total persons: {len(results)}")
    logger.info(f"Average narrative length: {sum(narrative_lengths) / len(narrative_lengths):.0f} characters")
    logger.info(f"Min narrative length: {min(narrative_lengths)}")
    logger.info(f"Max narrative length: {max(narrative_lengths)}")
    
    # Show sample narratives
    logger.info("\n" + "="*60)
    logger.info("SAMPLE NARRATIVES")
    logger.info("="*60)
    
    for i in range(min(3, len(results))):
        person_id, name, narrative, _ = results[i]
        logger.info(f"\n{i+1}. {name}")
        logger.info(f"   {narrative[:400]}...")
    
    logger.info("\n✓ Event text processing complete")


if __name__ == "__main__":
    main()
