#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Exploratory Data Analysis and Data Cleaning for Wikidata Crawl
Analyzes completeness, handles NULL values, and prepares data for BigQuery ingestion
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter, defaultdict
from datetime import datetime
import logging
import config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'{config.LOGS_DIR}/eda_cleaning.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configure plotting
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)

def load_crawl_data(filepath):
    """Load the crawled JSON data"""
    logger.info(f"Loading data from {filepath}...")
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
    logger.info(f"Loaded {len(data)} person records")
    return data

def analyze_field_completeness(data):
    """Analyze completeness of each field across all persons"""
    logger.info("\n" + "="*60)
    logger.info("FIELD COMPLETENESS ANALYSIS")
    logger.info("="*60)
    
    total_persons = len(data)
    field_stats = {}
    
    # Person-level fields
    person_fields = [
        'wikidata_id', 'name', 'description', 'birth_date', 'death_date',
        'birth_place', 'death_place', 'occupation', 'field_of_work',
        'citizenship', 'languages'
    ]
    
    for field in person_fields:
        non_null_count = 0
        for person in data:
            value = person.get(field)
            # Check if value is not None and not empty (for arrays)
            if value is not None:
                if isinstance(value, list):
                    if len(value) > 0:
                        non_null_count += 1
                else:
                    non_null_count += 1
        
        completeness_pct = (non_null_count / total_persons) * 100
        field_stats[field] = {
            'non_null': non_null_count,
            'null': total_persons - non_null_count,
            'completeness_pct': completeness_pct
        }
        logger.info(f"{field:20s}: {non_null_count:4d}/{total_persons} ({completeness_pct:5.1f}% complete)")
    
    return field_stats

def analyze_life_events(data):
    """Analyze life events distribution and completeness"""
    logger.info("\n" + "="*60)
    logger.info("LIFE EVENTS ANALYSIS")
    logger.info("="*60)
    
    # Events per person
    events_per_person = [len(person.get('life_events', [])) for person in data]
    
    logger.info(f"\nEvents per person statistics:")
    logger.info(f"  Total persons: {len(data)}")
    logger.info(f"  Mean events: {sum(events_per_person) / len(events_per_person):.2f}")
    logger.info(f"  Median events: {sorted(events_per_person)[len(events_per_person)//2]}")
    logger.info(f"  Min events: {min(events_per_person)}")
    logger.info(f"  Max events: {max(events_per_person)}")
    logger.info(f"  Persons with 0 events: {sum(1 for e in events_per_person if e == 0)}")
    logger.info(f"  Persons with <3 events: {sum(1 for e in events_per_person if e < 3)}")
    
    # Event type breakdown
    event_types = Counter()
    event_field_completeness = defaultdict(lambda: {'total': 0, 'non_null': 0})
    
    total_events = 0
    for person in data:
        for event in person.get('life_events', []):
            total_events += 1
            event_type = event.get('event_type', 'unknown')
            event_types[event_type] += 1
            
            # Track field completeness within events
            for field in ['start_date', 'end_date', 'point_in_time', 'location', 
                         'organization', 'role_or_degree', 'field_or_major']:
                event_field_completeness[field]['total'] += 1
                if event.get(field) is not None:
                    event_field_completeness[field]['non_null'] += 1
    
    logger.info(f"\nTotal life events: {total_events}")
    logger.info(f"\nEvent type breakdown:")
    for event_type, count in event_types.most_common():
        pct = (count / total_events) * 100
        logger.info(f"  {event_type:20s}: {count:5d} ({pct:5.1f}%)")
    
    logger.info(f"\nEvent field completeness:")
    for field, stats in sorted(event_field_completeness.items()):
        pct = (stats['non_null'] / stats['total']) * 100 if stats['total'] > 0 else 0
        logger.info(f"  {field:20s}: {stats['non_null']:5d}/{stats['total']:5d} ({pct:5.1f}%)")
    
    return events_per_person, event_types, event_field_completeness

def analyze_temporal_coverage(data):
    """Analyze temporal coverage of dates"""
    logger.info("\n" + "="*60)
    logger.info("TEMPORAL COVERAGE ANALYSIS")
    logger.info("="*60)
    
    birth_years = []
    death_years = []
    event_years = []
    
    for person in data:
        # Birth years
        if person.get('birth_date'):
            try:
                year = int(person['birth_date'].split('-')[0])
                if 1800 <= year <= 2025:  # Sanity check
                    birth_years.append(year)
            except:
                pass
        
        # Death years
        if person.get('death_date'):
            try:
                year = int(person['death_date'].split('-')[0])
                if 1800 <= year <= 2025:
                    death_years.append(year)
            except:
                pass
        
        # Event years
        for event in person.get('life_events', []):
            for date_field in ['start_date', 'end_date', 'point_in_time']:
                date_val = event.get(date_field)
                if date_val:
                    try:
                        year = int(date_val.split('-')[0])
                        if 1800 <= year <= 2025:
                            event_years.append(year)
                    except:
                        pass
    
    if birth_years:
        logger.info(f"\nBirth years:")
        logger.info(f"  Range: {min(birth_years)} - {max(birth_years)}")
        logger.info(f"  Count: {len(birth_years)}")
    
    if death_years:
        logger.info(f"\nDeath years:")
        logger.info(f"  Range: {min(death_years)} - {max(death_years)}")
        logger.info(f"  Count: {len(death_years)}")
    
    if event_years:
        logger.info(f"\nEvent years:")
        logger.info(f"  Range: {min(event_years)} - {max(event_years)}")
        logger.info(f"  Count: {len(event_years)}")
    
    return birth_years, death_years, event_years

def analyze_occupation_diversity(data):
    """Analyze occupation and field diversity"""
    logger.info("\n" + "="*60)
    logger.info("OCCUPATION & FIELD DIVERSITY ANALYSIS")
    logger.info("="*60)
    
    all_occupations = Counter()
    all_fields = Counter()
    
    for person in data:
        for occ in person.get('occupation', []):
            all_occupations[occ] += 1
        for field in person.get('field_of_work', []):
            all_fields[field] += 1
    
    logger.info(f"\nTotal unique occupations: {len(all_occupations)}")
    logger.info(f"\nTop 20 occupations:")
    for occ, count in all_occupations.most_common(20):
        logger.info(f"  {occ:40s}: {count:4d}")
    
    logger.info(f"\nTotal unique fields of work: {len(all_fields)}")
    logger.info(f"\nTop 20 fields of work:")
    for field, count in all_fields.most_common(20):
        logger.info(f"  {field:40s}: {count:4d}")
    
    return all_occupations, all_fields

def calculate_profile_quality_score(person):
    """Calculate a quality score for a person profile (0-100)"""
    score = 0
    
    # Required fields (40 points)
    if person.get('wikidata_id'): score += 10
    if person.get('name'): score += 10
    if person.get('description'): score += 10
    if person.get('occupation') and len(person['occupation']) > 0: score += 10
    
    # Biographical fields (20 points)
    if person.get('birth_date'): score += 5
    if person.get('birth_place'): score += 5
    if person.get('field_of_work') and len(person['field_of_work']) > 0: score += 10
    
    # Life events (40 points)
    events = person.get('life_events', [])
    num_events = len(events)
    if num_events >= 10:
        score += 20
    elif num_events >= 5:
        score += 15
    elif num_events >= 3:
        score += 10
    elif num_events >= 1:
        score += 5
    
    # Event quality (20 points max)
    events_with_dates = sum(1 for e in events if e.get('start_date') or e.get('point_in_time'))
    events_with_location = sum(1 for e in events if e.get('location'))
    if num_events > 0:
        date_pct = events_with_dates / num_events
        loc_pct = events_with_location / num_events
        score += int(date_pct * 15)
        score += int(loc_pct * 5)
    
    return min(score, 100)

def identify_quality_profiles(data):
    """Identify high and low quality profiles"""
    logger.info("\n" + "="*60)
    logger.info("PROFILE QUALITY ASSESSMENT")
    logger.info("="*60)
    
    # Calculate scores
    profiles_with_scores = []
    for person in data:
        score = calculate_profile_quality_score(person)
        profiles_with_scores.append((person, score))
    
    # Sort by score
    profiles_with_scores.sort(key=lambda x: x[1], reverse=True)
    
    # Overall statistics
    scores = [score for _, score in profiles_with_scores]
    avg_score = sum(scores) / len(scores)
    
    logger.info(f"\nOverall quality statistics:")
    logger.info(f"  Mean score: {avg_score:.1f}")
    logger.info(f"  Median score: {sorted(scores)[len(scores)//2]}")
    logger.info(f"  Min score: {min(scores)}")
    logger.info(f"  Max score: {max(scores)}")
    logger.info(f"  Profiles with score >= 80: {sum(1 for s in scores if s >= 80)}")
    logger.info(f"  Profiles with score >= 60: {sum(1 for s in scores if s >= 60)}")
    logger.info(f"  Profiles with score < 40: {sum(1 for s in scores if s < 40)}")
    
    # Top 10
    logger.info(f"\nTop 10 highest quality profiles:")
    for i, (person, score) in enumerate(profiles_with_scores[:10], 1):
        logger.info(f"  {i:2d}. {person.get('name', 'Unknown'):40s} - Score: {score} - Events: {len(person.get('life_events', []))}")
    
    # Bottom 10
    logger.info(f"\nBottom 10 lowest quality profiles:")
    for i, (person, score) in enumerate(profiles_with_scores[-10:], 1):
        logger.info(f"  {i:2d}. {person.get('name', 'Unknown'):40s} - Score: {score} - Events: {len(person.get('life_events', []))}")
    
    return profiles_with_scores

def clean_and_filter_data(data, min_quality_score=40, min_events=3):
    """Clean data and filter out low-quality profiles"""
    logger.info("\n" + "="*60)
    logger.info("DATA CLEANING & FILTERING")
    logger.info("="*60)
    
    logger.info(f"\nFiltering criteria:")
    logger.info(f"  Minimum quality score: {min_quality_score}")
    logger.info(f"  Minimum life events: {min_events}")
    
    cleaned_data = []
    rejected_count = 0
    rejection_reasons = Counter()
    
    for person in data:
        # Calculate quality score
        score = calculate_profile_quality_score(person)
        
        # Check required fields
        if not person.get('wikidata_id'):
            rejection_reasons['missing_wikidata_id'] += 1
            rejected_count += 1
            continue
        
        if not person.get('name'):
            rejection_reasons['missing_name'] += 1
            rejected_count += 1
            continue
        
        # Check quality score
        if score < min_quality_score:
            rejection_reasons['low_quality_score'] += 1
            rejected_count += 1
            continue
        
        # Check minimum events
        events = person.get('life_events', [])
        if len(events) < min_events:
            rejection_reasons['insufficient_events'] += 1
            rejected_count += 1
            continue
        
        # Clean person data
        cleaned_person = clean_person_data(person)
        cleaned_data.append(cleaned_person)
    
    logger.info(f"\nFiltering results:")
    logger.info(f"  Original profiles: {len(data)}")
    logger.info(f"  Accepted profiles: {len(cleaned_data)}")
    logger.info(f"  Rejected profiles: {rejected_count}")
    logger.info(f"\nRejection reasons:")
    for reason, count in rejection_reasons.most_common():
        logger.info(f"  {reason:30s}: {count:4d}")
    
    return cleaned_data

def clean_person_data(person):
    """Clean and standardize a person's data"""
    cleaned = person.copy()
    
    # Fill missing description
    if not cleaned.get('description'):
        cleaned['description'] = "No description available"
    
    # Ensure arrays are lists (not None)
    for field in ['occupation', 'field_of_work', 'citizenship', 'languages']:
        if not cleaned.get(field):
            cleaned[field] = []
    
    # Clean life events
    cleaned_events = []
    seen_events = set()
    
    for event in person.get('life_events', []):
        # Skip events without meaningful content
        if not event.get('event_title') and not event.get('organization'):
            continue
        
        # Deduplicate (same type + title + start_date)
        event_key = (
            event.get('event_type', ''),
            event.get('event_title', ''),
            event.get('start_date', '')
        )
        if event_key in seen_events:
            continue
        seen_events.add(event_key)
        
        # Clean event data
        cleaned_event = clean_event_data(event)
        cleaned_events.append(cleaned_event)
    
    cleaned['life_events'] = cleaned_events
    
    # Add metadata
    cleaned['quality_score'] = calculate_profile_quality_score(cleaned)
    cleaned['num_life_events'] = len(cleaned_events)
    
    return cleaned

def clean_event_data(event):
    """Clean and standardize event data"""
    cleaned = event.copy()
    
    # Ensure event_description exists
    if not cleaned.get('event_description'):
        event_type = cleaned.get('event_type', 'event')
        org = cleaned.get('organization', '')
        if org:
            cleaned['event_description'] = f"{event_type} at {org}"
        else:
            cleaned['event_description'] = f"{event_type} event"
    
    # Clean text fields (strip whitespace)
    for field in ['event_title', 'event_description', 'location', 'organization']:
        if cleaned.get(field):
            cleaned[field] = cleaned[field].strip()
    
    return cleaned

def save_cleaned_data(cleaned_data, output_path):
    """Save cleaned data to JSON"""
    logger.info(f"\nSaving cleaned data to {output_path}...")
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(cleaned_data, f, indent=2, ensure_ascii=False)
    logger.info(f"Saved {len(cleaned_data)} cleaned profiles")

def generate_visualizations(data, output_dir):
    """Generate visualization plots"""
    logger.info("\n" + "="*60)
    logger.info("GENERATING VISUALIZATIONS")
    logger.info("="*60)
    
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. Events per person distribution
    events_per_person = [len(p.get('life_events', [])) for p in data]
    plt.figure(figsize=(10, 6))
    plt.hist(events_per_person, bins=30, edgecolor='black')
    plt.xlabel('Number of Life Events')
    plt.ylabel('Number of Persons')
    plt.title('Distribution of Life Events per Person')
    plt.savefig(f'{output_dir}/events_per_person.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("  ✓ Saved events_per_person.png")
    
    # 2. Event types breakdown
    event_types = Counter()
    for person in data:
        for event in person.get('life_events', []):
            event_types[event.get('event_type', 'unknown')] += 1
    
    plt.figure(figsize=(12, 6))
    types, counts = zip(*event_types.most_common(15))
    plt.bar(types, counts)
    plt.xlabel('Event Type')
    plt.ylabel('Count')
    plt.title('Event Type Distribution (Top 15)')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(f'{output_dir}/event_types.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("  ✓ Saved event_types.png")
    
    # 3. Quality score distribution
    scores = [calculate_profile_quality_score(p) for p in data]
    plt.figure(figsize=(10, 6))
    plt.hist(scores, bins=20, edgecolor='black')
    plt.xlabel('Quality Score')
    plt.ylabel('Number of Persons')
    plt.title('Distribution of Profile Quality Scores')
    plt.axvline(x=60, color='r', linestyle='--', label='Target threshold')
    plt.legend()
    plt.savefig(f'{output_dir}/quality_scores.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("  ✓ Saved quality_scores.png")
    
    logger.info(f"\nAll visualizations saved to {output_dir}/")

def main():
    """Main execution"""
    logger.info("="*60)
    logger.info("WIKIDATA CRAWL DATA - EDA & CLEANING")
    logger.info("="*60)
    
    # Configuration
    input_file = os.path.join(config.DATA_DIR, "raw", "wikidata_people_1000.json")
    output_file = os.path.join(config.DATA_DIR, "processed", "wikidata_people_cleaned.json")
    viz_dir = os.path.join(config.DATA_DIR, "processed", "eda_visualizations")
    
    # Load data
    data = load_crawl_data(input_file)
    
    # Run EDA
    field_stats = analyze_field_completeness(data)
    events_per_person, event_types, event_field_stats = analyze_life_events(data)
    birth_years, death_years, event_years = analyze_temporal_coverage(data)
    occupations, fields = analyze_occupation_diversity(data)
    profiles_with_scores = identify_quality_profiles(data)
    
    # Clean and filter data
    cleaned_data = clean_and_filter_data(
        data, 
        min_quality_score=40,  # Relatively lenient for v1
        min_events=3
    )
    
    # Save cleaned data
    save_cleaned_data(cleaned_data, output_file)
    
    # Generate visualizations
    generate_visualizations(cleaned_data, viz_dir)
    
    # Final summary
    logger.info("\n" + "="*60)
    logger.info("SUMMARY")
    logger.info("="*60)
    logger.info(f"Original profiles: {len(data)}")
    logger.info(f"Cleaned profiles: {len(cleaned_data)}")
    logger.info(f"Rejection rate: {((len(data) - len(cleaned_data)) / len(data) * 100):.1f}%")
    logger.info(f"\nOutput files:")
    logger.info(f"  Cleaned data: {output_file}")
    logger.info(f"  Visualizations: {viz_dir}/")
    logger.info(f"  Log file: {config.LOGS_DIR}/eda_cleaning.log")
    logger.info("="*60)
    
    return cleaned_data

if __name__ == "__main__":
    main()
