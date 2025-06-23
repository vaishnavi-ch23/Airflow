CREATE OR REPLACE SCHEMA IF NOT EXISTS SILVER_LAYER;

CREATE OR REPLACE TABLE SILVER_LAYER.TRACKS_CLEANED AS
SELECT
    track_id,
    LOWER(track_name) AS track_name,
    LOWER(album) AS album,
    TRY_TO_DATE(release_date) AS release_date,
    TRY_TO_NUMBER(duration_ms) AS duration_ms,
    CASE 
        WHEN LOWER(explicit) IN ('true', 'yes', '1') THEN TRUE
        ELSE FALSE
    END AS explicit,
    LOWER(artist_names) AS artist_names,
    TRY_TO_NUMBER(artist_count) AS artist_count,
    TRY_TO_NUMBER(popularity) AS popularity,
    LOWER(language) AS language,
    TRY_TO_NUMBER(streams) AS streams
FROM SPOTIFY.SPDATA.TRACKS
WHERE track_id IS NOT NULL AND release_date IS NOT NULL;