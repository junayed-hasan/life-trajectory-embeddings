#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enhanced Wikidata Scientist Biography Extractor with Temporal Information
Extracts biographical data with life events (education, employment, awards) including dates
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from SPARQLWrapper import SPARQLWrapper, JSON
import json
import time
from tqdm import tqdm
import logging
from datetime import datetime
import config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'{config.LOGS_DIR}/wikidata_crawler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def setup_sparql():
    """Initialize SPARQL wrapper with proper user agent"""
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
    sparql.setReturnFormat(JSON)
    sparql.addCustomHttpHeader("User-Agent", "LifeEmbedding-JHU-Project/1.0 (lifeembedding@jhu.edu)")
    return sparql

def get_person_ids(sparql, target=1000):
    """
    Get diverse person IDs from Wikidata across 50+ occupations
    Targets ~20 people per occupation for rich clustering
    """
    logger.info(f"Fetching person IDs across diverse occupations (target: {target})...")
    
    # Define 50+ diverse occupations (QIDs)
    occupations = {
        # STEM
        "Q5482740": "computer scientist",
        "Q169470": "physicist", 
        "Q170790": "mathematician",
        "Q593644": "chemist",
        "Q15978367": "biologist",
        "Q11063": "astronomer",
        "Q205375": "engineer",
        "Q901402": "data scientist",
        "Q105186": "medical doctor",
        
        # Social Sciences & Humanities
        "Q188094": "economist",
        "Q4964182": "philosopher",
        "Q36180": "writer",
        "Q6625963": "novelist",
        "Q49757": "poet",
        "Q201788": "historian",
        "Q4773904": "anthropologist",
        "Q20826540": "sociologist",
        "Q211346": "political scientist",
        "Q212980": "psychologist",
        
        # Arts & Creative
        "Q1028181": "painter",
        "Q1281618": "sculptor",
        "Q483501": "artist",
        "Q822146": "photographer",
        "Q639669": "musician",
        "Q36834": "composer",
        "Q488205": "singer-songwriter",
        "Q3282637": "film producer",
        "Q2526255": "film director",
        "Q33999": "actor",
        "Q10800557": "film actor",
        
        # Sports & Athletics
        "Q2066131": "athlete",
        "Q937857": "association football player",
        "Q5137571": "basketball player",
        "Q10871364": "baseball player",
        "Q13141064": "tennis player",
        
        # Business & Leadership
        "Q131524": "entrepreneur",
        "Q170790": "business executive",
        "Q212238": "civil servant",
        "Q82955": "politician",
        "Q15253558": "activist",
        
        # Law & Military
        "Q40348": "lawyer",
        "Q16533": "judge",
        "Q47064": "military personnel",
        
        # Education & Religion
        "Q1622272": "university teacher",
        "Q1234713": "theologian",
        
        # Media & Communication
        "Q1930187": "journalist",
        "Q947873": "television presenter",
        "Q2722764": "radio personality",
        
        # Tech & Innovation
        "Q5137571": "software developer",
        "Q1114448": "researcher",
    }
    
    person_ids = []
    occupation_counts = {}
    
    # Query ~25 people per occupation (to get ~20 complete after filtering)
    per_occupation = 25
    
    for occ_qid, occ_label in tqdm(occupations.items(), desc="Fetching occupations"):
        query = f"""
        SELECT DISTINCT ?person WHERE {{
          ?person wdt:P31 wd:Q5;                    # Human
                  wdt:P106 wd:{occ_qid}.            # Specific occupation

          # Must have English Wikipedia
          ?article schema:about ?person;
                   schema:isPartOf <https://en.wikipedia.org/>.
                   
          # Prefer people with some notable activity (but not required)
          OPTIONAL {{ ?person wdt:P166 ?award. }}
          OPTIONAL {{ ?person wdt:P69 ?education. }}
          OPTIONAL {{ ?person wdt:P108 ?employer. }}
        }}
        LIMIT {per_occupation}
        """
        
        try:
            sparql.setQuery(query)
            results = sparql.query().convert()
            
            occupation_person_ids = []
            for result in results["results"]["bindings"]:
                person_id = result["person"]["value"].split("/")[-1]
                if person_id not in person_ids:
                    person_ids.append(person_id)
                    occupation_person_ids.append(person_id)
            
            occupation_counts[occ_label] = len(occupation_person_ids)
            logger.info(f"  {occ_label}: {len(occupation_person_ids)} people")
            
            time.sleep(0.5)  # Rate limiting between occupation queries
            
        except Exception as e:
            logger.error(f"Error fetching {occ_label}: {e}")
            time.sleep(2)
    
    logger.info(f"\nTotal people found: {len(person_ids)} across {len(occupation_counts)} occupations")
    logger.info(f"Average per occupation: {len(person_ids) / len(occupation_counts):.1f}")
    
    return person_ids

def parse_wikidata_date(date_string):
    """
    Parse Wikidata date format to Python date
    Handles various formats: +1955-01-01T00:00:00Z, +1955-00-00, etc.
    """
    if not date_string:
        return None
    
    try:
        # Remove the + prefix and timezone
        date_str = date_string.replace('+', '').replace('Z', '')
        
        # Handle incomplete dates (00 for month/day)
        parts = date_str.split('T')[0].split('-')
        year = int(parts[0]) if parts[0] != '0000' else None
        month = int(parts[1]) if len(parts) > 1 and parts[1] != '00' else 1
        day = int(parts[2]) if len(parts) > 2 and parts[2] != '00' else 1
        
        if year:
            return f"{year:04d}-{month:02d}-{day:02d}"
        return None
    except Exception as e:
        logger.warning(f"Could not parse date: {date_string} - {e}")
        return None

def extract_person_basic_info(person_id, sparql):
    """Extract basic biographical information"""
    
    query = f"""
    SELECT ?name ?desc ?birthDate ?deathDate ?birthPlace ?deathPlace WHERE {{
      wd:{person_id} rdfs:label ?name.
      FILTER(LANG(?name) = "en")

      OPTIONAL {{
        wd:{person_id} schema:description ?desc.
        FILTER(LANG(?desc) = "en")
      }}
      
      OPTIONAL {{ wd:{person_id} wdt:P569 ?birthDate. }}
      OPTIONAL {{ wd:{person_id} wdt:P570 ?deathDate. }}
      OPTIONAL {{ 
        wd:{person_id} wdt:P19 ?birthPlaceEntity.
        ?birthPlaceEntity rdfs:label ?birthPlace.
        FILTER(LANG(?birthPlace) = "en")
      }}
      OPTIONAL {{ 
        wd:{person_id} wdt:P20 ?deathPlaceEntity.
        ?deathPlaceEntity rdfs:label ?deathPlace.
        FILTER(LANG(?deathPlace) = "en")
      }}
    }}
    LIMIT 1
    """

    sparql.setQuery(query)
    result = sparql.query().convert()
    
    if not result["results"]["bindings"]:
        return None

    data = result["results"]["bindings"][0]
    
    return {
        "name": data.get("name", {}).get("value"),
        "description": data.get("desc", {}).get("value"),
        "birth_date": parse_wikidata_date(data.get("birthDate", {}).get("value")),
        "death_date": parse_wikidata_date(data.get("deathDate", {}).get("value")),
        "birth_place": data.get("birthPlace", {}).get("value"),
        "death_place": data.get("deathPlace", {}).get("value"),
    }

def extract_person_categories(person_id, sparql):
    """Extract occupations, fields of work, citizenship, and languages"""
    
    query = f"""
    SELECT ?occupation ?field ?citizenship ?language WHERE {{
      wd:{person_id} wdt:P106 ?occEntity.
      ?occEntity rdfs:label ?occupation.
      FILTER(LANG(?occupation) = "en")
      
      OPTIONAL {{
        wd:{person_id} wdt:P101 ?fieldEntity.
        ?fieldEntity rdfs:label ?field.
        FILTER(LANG(?field) = "en")
      }}
      
      OPTIONAL {{
        wd:{person_id} wdt:P27 ?citizenshipEntity.
        ?citizenshipEntity rdfs:label ?citizenship.
        FILTER(LANG(?citizenship) = "en")
      }}
      
      OPTIONAL {{
        wd:{person_id} wdt:P1412 ?languageEntity.
        ?languageEntity rdfs:label ?language.
        FILTER(LANG(?language) = "en")
      }}
    }}
    """

    sparql.setQuery(query)
    result = sparql.query().convert()
    
    occupations = []
    fields = []
    citizenships = []
    languages = []
    
    for binding in result["results"]["bindings"]:
        occ = binding.get("occupation", {}).get("value")
        if occ and occ not in occupations:
            occupations.append(occ)
        
        field = binding.get("field", {}).get("value")
        if field and field not in fields:
            fields.append(field)
            
        citizenship = binding.get("citizenship", {}).get("value")
        if citizenship and citizenship not in citizenships:
            citizenships.append(citizenship)
            
        language = binding.get("language", {}).get("value")
        if language and language not in languages:
            languages.append(language)
    
    return occupations, fields, citizenships, languages

def extract_life_events(person_id, sparql):
    """
    Extract comprehensive life events with temporal and location information
    Includes: education, employment, residence, awards, participation, and domain-specific events
    """
    
    # Simplified query - query each property type separately for reliability
    events = []
    seen_events = set()
    
    # Define property mappings
    properties = {
        "P69": "education",
        "P108": "employment", 
        "P39": "position",
        "P551": "residence",
        "P937": "work_location",
        "P166": "award",
        "P800": "notable_work",
        "P793": "significant_event",
        "P1344": "participant_in",
        "P54": "sports_team",
    }
    
    for prop_id, event_type in properties.items():
        # Simple query per property
        query = f"""
        SELECT ?value ?valueLabel ?startDate ?endDate ?pointInTime 
               ?degree ?degreeLabel ?major ?majorLabel ?location ?locationLabel WHERE {{
          
          wd:{person_id} p:{prop_id} ?statement.
          ?statement ps:{prop_id} ?value.
          
          # Temporal qualifiers
          OPTIONAL {{ ?statement pq:P580 ?startDate. }}
          OPTIONAL {{ ?statement pq:P582 ?endDate. }}
          OPTIONAL {{ ?statement pq:P585 ?pointInTime. }}
          
          # Education qualifiers
          OPTIONAL {{ 
            ?statement pq:P512 ?degree.
            ?degree rdfs:label ?degreeLabel.
            FILTER(LANG(?degreeLabel) = "en")
          }}
          OPTIONAL {{ 
            ?statement pq:P812 ?major.
            ?major rdfs:label ?majorLabel.
            FILTER(LANG(?majorLabel) = "en")
          }}
          
          # Location
          OPTIONAL {{ 
            ?value wdt:P131 ?location.
            ?location rdfs:label ?locationLabel.
            FILTER(LANG(?locationLabel) = "en")
          }}
          
          SERVICE wikibase:label {{ 
            bd:serviceParam wikibase:language "en".
            ?value rdfs:label ?valueLabel.
          }}
        }}
        """
        
        try:
            sparql.setQuery(query)
            result = sparql.query().convert()
            
            for binding in result["results"]["bindings"]:
                value_label = binding.get("valueLabel", {}).get("value", "")
                if not value_label:
                    continue
                
                # Parse dates
                start_date = parse_wikidata_date(binding.get("startDate", {}).get("value"))
                end_date = parse_wikidata_date(binding.get("endDate", {}).get("value"))
                point_in_time = parse_wikidata_date(binding.get("pointInTime", {}).get("value"))
                
                # Use point_in_time as start_date if missing
                if not start_date and point_in_time:
                    start_date = point_in_time
                
                # Get qualifiers
                degree = binding.get("degreeLabel", {}).get("value")
                major = binding.get("majorLabel", {}).get("value")
                location = binding.get("locationLabel", {}).get("value")
                
                # Build event title
                title_parts = [value_label]
                if degree:
                    title_parts.append(degree)
                if major:
                    title_parts.append(f"in {major}")
                
                event_title = " - ".join(title_parts) if len(title_parts) > 1 else value_label
                
                # Create unique key
                event_key = f"{event_type}_{value_label}_{start_date}_{degree}"
                
                if event_key not in seen_events:
                    event_data = {
                        "event_type": event_type,
                        "event_title": event_title,
                        "event_description": f"{event_type} event",
                        "start_date": start_date,
                        "end_date": end_date,
                        "point_in_time": point_in_time,
                        "location": location,
                        "organization": value_label if event_type in ["education", "employment", "sports_team"] else None,
                        "role_or_degree": degree,
                        "field_or_major": major,
                        "sport": None,
                        "instrument": None,
                        "source": f"wikidata:{prop_id}"
                    }
                    
                    events.append(event_data)
                    seen_events.add(event_key)
        
        except Exception as e:
            logger.debug(f"No {event_type} events for {person_id}: {e}")
            continue
        
        time.sleep(0.1)  # Small delay between property queries
    
    return events



def extract_person_metadata(person_id, sparql):
    """Extract complete metadata for a person"""
    
    logger.info(f"Extracting metadata for {person_id}...")
    
    # Basic info
    basic_info = extract_person_basic_info(person_id, sparql)
    if not basic_info:
        logger.warning(f"No basic info found for {person_id}")
        return None
    
    time.sleep(0.5)  # Rate limiting
    
    # Categories (now includes citizenship and languages)
    occupations, fields, citizenships, languages = extract_person_categories(person_id, sparql)
    
    time.sleep(0.5)  # Rate limiting
    
    # Life events (enhanced with new properties)
    events = extract_life_events(person_id, sparql)
    
    metadata = {
        "wikidata_id": person_id,
        "name": basic_info["name"],
        "description": basic_info["description"],
        "occupation": occupations,
        "field_of_work": fields,
        "citizenship": citizenships,
        "languages": languages,
        "birth_date": basic_info["birth_date"],
        "death_date": basic_info["death_date"],
        "birth_place": basic_info["birth_place"],
        "death_place": basic_info["death_place"],
        "life_events": events,
        "created_at": datetime.utcnow().isoformat()
    }
    
    return metadata

def is_complete_profile(metadata):
    """Check if profile has minimum required fields"""
    if not metadata:
        return False
    
    required_checks = [
        metadata.get("name") is not None,
        len(metadata.get("occupation", [])) > 0,
        len(metadata.get("field_of_work", [])) > 0,
        len(metadata.get("life_events", [])) >= 3,  # At least 3 events
    ]
    return sum(required_checks) >= 3

def completeness_score(metadata):
    """Calculate completeness score for ranking"""
    if not metadata:
        return 0
    
    return sum([
        1 if metadata.get("name") else 0,
        len(metadata.get("occupation", [])),
        len(metadata.get("field_of_work", [])),
        len(metadata.get("life_events", [])),
        1 if metadata.get("birth_date") else 0,
        1 if metadata.get("birth_place") else 0,
    ])

def save_intermediate_results(data, filename="intermediate_crawl.json"):
    """Save intermediate results as backup"""
    filepath = os.path.join(config.DATA_DIR, "raw", filename)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logger.info(f"Saved intermediate results to {filepath}")

def main():
    """Main execution function"""
    
    logger.info("="*60)
    logger.info("Starting Enhanced Wikidata Biography Extraction")
    logger.info("Target: 1000 people across 50+ diverse occupations")
    logger.info("="*60)
    
    # Output file path
    output_path = os.path.join(config.DATA_DIR, "raw", "wikidata_people_1000.json")
    
    # Setup
    sparql = setup_sparql()
    
    # Get person IDs across diverse occupations
    logger.info("Step 1: Fetching person IDs across occupations...")
    person_ids = get_person_ids(sparql, target=1000)
    logger.info(f"Found {len(person_ids)} potential people")
    
    # Extract metadata
    logger.info("\nStep 2: Extracting metadata and life events...")
    logger.info("This will take 5-7 hours with proper rate limiting...")
    all_metadata = []
    failed_ids = []

    for i, person_id in enumerate(tqdm(person_ids, desc="Extracting metadata")):
        try:
            metadata = extract_person_metadata(person_id, sparql)
            if metadata:
                all_metadata.append(metadata)
                
                # Save intermediate results every 50 persons (more frequent for long runs)
                if (i + 1) % 50 == 0:
                    save_intermediate_results(all_metadata, f"intermediate_crawl_{i+1}.json")
                    logger.info(f"\n  Checkpoint: {len(all_metadata)} profiles extracted so far")
                    
            time.sleep(1.5)  # Rate limiting - be respectful to Wikidata
            
        except Exception as e:
            logger.error(f"Failed to extract {person_id}: {e}")
            failed_ids.append(person_id)
            time.sleep(3)  # Wait longer on error

    logger.info(f"\nSuccessfully extracted {len(all_metadata)} profiles")
    logger.info(f"Failed: {len(failed_ids)} profiles")

    # Filter complete profiles
    logger.info("\nStep 3: Filtering complete profiles...")
    complete_metadata = [m for m in all_metadata if is_complete_profile(m)]
    logger.info(f"Complete profiles: {len(complete_metadata)}")

    # Sort by completeness and take top 1000 (or all if less)
    target_count = min(1000, len(complete_metadata))
    if len(complete_metadata) > target_count:
        complete_metadata = sorted(complete_metadata,
                                   key=completeness_score,
                                   reverse=True)[:target_count]

    # Save to JSON
    logger.info(f"\nStep 4: Saving to {output_path}...")
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(complete_metadata, f, indent=2, ensure_ascii=False)

    # Print summary statistics
    logger.info("\n" + "="*60)
    logger.info("CRAWLING COMPLETE")
    logger.info("="*60)
    logger.info(f"Total profiles saved: {len(complete_metadata)}")
    logger.info(f"Output file: {output_path}")
    
    if complete_metadata:
        total_events = sum(len(m.get("life_events", [])) for m in complete_metadata)
        avg_events = total_events / len(complete_metadata)
        logger.info(f"Total life events: {total_events}")
        logger.info(f"Average events per person: {avg_events:.1f}")
        
        # Event type breakdown
        event_types = {}
        for person in complete_metadata:
            for event in person.get("life_events", []):
                event_type = event.get("event_type", "other")
                event_types[event_type] = event_types.get(event_type, 0) + 1
        
        logger.info(f"\nEvent type breakdown:")
        for event_type, count in sorted(event_types.items(), key=lambda x: x[1], reverse=True):
            logger.info(f"  {event_type}: {count}")
        
        # Occupation diversity
        all_occupations = set()
        for person in complete_metadata:
            all_occupations.update(person.get("occupation", []))
        logger.info(f"\nUnique occupations: {len(all_occupations)}")
    
    logger.info("="*60)
    
    return complete_metadata

if __name__ == "__main__":
    main()
