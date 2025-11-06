-- Post-Ingestion Validation Queries
-- Run these queries in BigQuery Console to validate data quality after ingestion

-- ============================================================
-- 1. ROW COUNTS
-- ============================================================

-- Total persons ingested
SELECT COUNT(*) as total_persons
FROM `lifeembedding.lifeembedding_data.persons`;

-- Total life events ingested
SELECT COUNT(*) as total_life_events
FROM `lifeembedding.lifeembedding_data.life_events`;


-- ============================================================
-- 2. JOIN INTEGRITY
-- ============================================================

-- Check all events have valid person_ids
SELECT 
    COUNT(*) as orphaned_events
FROM `lifeembedding.lifeembedding_data.life_events` e
LEFT JOIN `lifeembedding.lifeembedding_data.persons` p 
    ON e.person_id = p.person_id
WHERE p.person_id IS NULL;
-- Should return 0


-- Check persons without events (should be 0 after filtering)
SELECT 
    p.person_id,
    p.name,
    p.wikidata_id
FROM `lifeembedding.lifeembedding_data.persons` p
LEFT JOIN `lifeembedding.lifeembedding_data.life_events` e 
    ON p.person_id = e.person_id
WHERE e.event_id IS NULL;


-- ============================================================
-- 3. FIELD COMPLETENESS
-- ============================================================

-- Field completeness for persons table
SELECT 
    COUNT(*) as total,
    COUNTIF(name IS NOT NULL) as has_name,
    COUNTIF(description IS NOT NULL) as has_description,
    COUNTIF(birth_date IS NOT NULL) as has_birth_date,
    COUNTIF(death_date IS NOT NULL) as has_death_date,
    COUNTIF(birth_place IS NOT NULL) as has_birth_place,
    COUNTIF(ARRAY_LENGTH(occupation) > 0) as has_occupation,
    COUNTIF(ARRAY_LENGTH(field_of_work) > 0) as has_field_of_work,
    COUNTIF(ARRAY_LENGTH(citizenship) > 0) as has_citizenship,
    COUNTIF(ARRAY_LENGTH(languages) > 0) as has_languages,
    
    -- Percentages
    ROUND(COUNTIF(name IS NOT NULL) / COUNT(*) * 100, 1) as name_pct,
    ROUND(COUNTIF(description IS NOT NULL) / COUNT(*) * 100, 1) as description_pct,
    ROUND(COUNTIF(birth_date IS NOT NULL) / COUNT(*) * 100, 1) as birth_date_pct,
    ROUND(COUNTIF(ARRAY_LENGTH(occupation) > 0) / COUNT(*) * 100, 1) as occupation_pct,
    ROUND(COUNTIF(ARRAY_LENGTH(field_of_work) > 0) / COUNT(*) * 100, 1) as field_pct
FROM `lifeembedding.lifeembedding_data.persons`;


-- Event field completeness
SELECT 
    COUNT(*) as total_events,
    COUNTIF(event_title IS NOT NULL) as has_title,
    COUNTIF(event_description IS NOT NULL) as has_description,
    COUNTIF(start_date IS NOT NULL) as has_start_date,
    COUNTIF(end_date IS NOT NULL) as has_end_date,
    COUNTIF(point_in_time IS NOT NULL) as has_point_in_time,
    COUNTIF(location IS NOT NULL) as has_location,
    COUNTIF(organization IS NOT NULL) as has_organization,
    COUNTIF(role_or_degree IS NOT NULL) as has_role_or_degree,
    COUNTIF(field_or_major IS NOT NULL) as has_field_or_major,
    
    -- At least one temporal field
    COUNTIF(start_date IS NOT NULL OR end_date IS NOT NULL OR point_in_time IS NOT NULL) as has_any_date,
    ROUND(COUNTIF(start_date IS NOT NULL OR end_date IS NOT NULL OR point_in_time IS NOT NULL) / COUNT(*) * 100, 1) as date_coverage_pct
FROM `lifeembedding.lifeembedding_data.life_events`;


-- ============================================================
-- 4. DATA DISTRIBUTION
-- ============================================================

-- Events per person distribution
SELECT 
    event_count_bucket,
    COUNT(*) as num_persons
FROM (
    SELECT 
        p.person_id,
        CASE 
            WHEN COUNT(e.event_id) = 0 THEN '0 events'
            WHEN COUNT(e.event_id) BETWEEN 1 AND 3 THEN '1-3 events'
            WHEN COUNT(e.event_id) BETWEEN 4 AND 6 THEN '4-6 events'
            WHEN COUNT(e.event_id) BETWEEN 7 AND 10 THEN '7-10 events'
            WHEN COUNT(e.event_id) BETWEEN 11 AND 15 THEN '11-15 events'
            ELSE '16+ events'
        END as event_count_bucket
    FROM `lifeembedding.lifeembedding_data.persons` p
    LEFT JOIN `lifeembedding.lifeembedding_data.life_events` e 
        ON p.person_id = e.person_id
    GROUP BY p.person_id
)
GROUP BY event_count_bucket
ORDER BY event_count_bucket;


-- Event type breakdown
SELECT 
    event_type,
    COUNT(*) as count,
    ROUND(COUNT(*) / (SELECT COUNT(*) FROM `lifeembedding.lifeembedding_data.life_events`) * 100, 1) as percentage
FROM `lifeembedding.lifeembedding_data.life_events`
GROUP BY event_type
ORDER BY count DESC;


-- Top 20 occupations
SELECT 
    occupation,
    COUNT(*) as count
FROM `lifeembedding.lifeembedding_data.persons`,
UNNEST(occupation) as occupation
GROUP BY occupation
ORDER BY count DESC
LIMIT 20;


-- Top 20 fields of work
SELECT 
    field,
    COUNT(*) as count
FROM `lifeembedding.lifeembedding_data.persons`,
UNNEST(field_of_work) as field
GROUP BY field
ORDER BY count DESC
LIMIT 20;


-- ============================================================
-- 5. TEMPORAL SANITY CHECKS
-- ============================================================

-- Check birth_date < death_date (for deceased persons)
SELECT 
    person_id,
    name,
    birth_date,
    death_date
FROM `lifeembedding.lifeembedding_data.persons`
WHERE death_date IS NOT NULL 
    AND birth_date IS NOT NULL
    AND birth_date >= death_date;
-- Should return 0 rows


-- Event dates within reasonable ranges
SELECT 
    COUNT(*) as invalid_dates,
    MIN(start_date) as earliest_start,
    MAX(start_date) as latest_start
FROM `lifeembedding.lifeembedding_data.life_events`
WHERE start_date IS NOT NULL
    AND (EXTRACT(YEAR FROM PARSE_DATE('%Y-%m-%d', start_date)) < 1800 
         OR EXTRACT(YEAR FROM PARSE_DATE('%Y-%m-%d', start_date)) > 2025);


-- ============================================================
-- 6. SAMPLE DATA INSPECTION
-- ============================================================

-- View sample complete profiles
SELECT 
    p.person_id,
    p.name,
    p.description,
    p.occupation,
    p.field_of_work,
    p.birth_date,
    p.death_date,
    COUNT(e.event_id) as num_events
FROM `lifeembedding.lifeembedding_data.persons` p
LEFT JOIN `lifeembedding.lifeembedding_data.life_events` e 
    ON p.person_id = e.person_id
GROUP BY p.person_id, p.name, p.description, p.occupation, p.field_of_work, p.birth_date, p.death_date
ORDER BY num_events DESC
LIMIT 10;


-- View sample events for a specific person
SELECT 
    e.*
FROM `lifeembedding.lifeembedding_data.life_events` e
WHERE e.person_id = (
    SELECT person_id 
    FROM `lifeembedding.lifeembedding_data.persons` 
    LIMIT 1
)
ORDER BY e.start_date;


-- ============================================================
-- 7. OVERALL STATISTICS
-- ============================================================

SELECT 
    'Persons' as table_name,
    COUNT(*) as row_count
FROM `lifeembedding.lifeembedding_data.persons`

UNION ALL

SELECT 
    'Life Events' as table_name,
    COUNT(*) as row_count
FROM `lifeembedding.lifeembedding_data.life_events`

UNION ALL

SELECT 
    'Average Events per Person' as table_name,
    ROUND(COUNT(e.event_id) / COUNT(DISTINCT p.person_id), 2) as row_count
FROM `lifeembedding.lifeembedding_data.persons` p
LEFT JOIN `lifeembedding.lifeembedding_data.life_events` e 
    ON p.person_id = e.person_id;


-- ============================================================
-- 8. TEST VIEW FUNCTIONALITY
-- ============================================================

-- Test v_complete_profiles view
SELECT 
    person_id,
    name,
    num_events,
    event_types
FROM `lifeembedding.lifeembedding_data.v_complete_profiles`
ORDER BY num_events DESC
LIMIT 5;


-- Test v_event_timeline view
SELECT *
FROM `lifeembedding.lifeembedding_data.v_event_timeline`
WHERE person_id = (
    SELECT person_id 
    FROM `lifeembedding.lifeembedding_data.persons` 
    LIMIT 1
)
ORDER BY event_sequence;
