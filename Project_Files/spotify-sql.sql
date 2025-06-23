CREATE OR REPLACE DATABASE SPOTIFY;
CREATE OR REPLACE SCHEMA SPDATA;
CREATE OR REPLACE STAGE my_azure_stage_tracks
  URL = 'azure://yathin18.blob.core.windows.net/spotify-data/spotify/'
  CREDENTIALS = (AZURE_SAS_TOKEN = 'sv=2024-11-04&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2025-06-19T10:27:11Z&st=2025-06-17T02:27:11Z&spr=https&sig=0R9LT4r4LSsgmoxKvANE4WH%2F%2FUUNfFnn4dmaWWiHKRo%3D');

CREATE OR REPLACE STAGE my_azure_stage_user
URL = 'azure://yathin18.blob.core.windows.net/spotify-data/User/'
  CREDENTIALS = (AZURE_SAS_TOKEN = 'sv=2024-11-04&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2025-06-19T10:27:11Z&st=2025-06-17T02:27:11Z&spr=https&sig=0R9LT4r4LSsgmoxKvANE4WH%2F%2FUUNfFnn4dmaWWiHKRo%3D');

LIST @MY_AZURE_STAGE_TRACKS;
LIST @MY_AZURE_STAGE_USER;

CREATE OR REPLACE TABLE SPOTIFY.SPDATA.TRACKS (
                track_id VARCHAR,
                track_name VARCHAR,
                album VARCHAR,
                release_date DATE,
                duration_ms INTEGER,
                explicit BOOLEAN,
                artist_names VARCHAR,
                artist_count INTEGER,
                popularity INTEGER,
                bpm FLOAT,
                key INTEGER,
                mode INTEGER,
                danceability FLOAT,
                energy FLOAT,
                valence FLOAT,
                acousticness FLOAT,
                instrumentalness FLOAT,
                liveness FLOAT,
                speechiness FLOAT,
                language VARCHAR,
                streams INTEGER
            );


create or replace TABLE SPOTIFY.SPDATA.SPOTIFY_USER_DATA (
	AGE VARCHAR(16777216),
	GENDER VARCHAR(16777216),
	SPOTIFY_USAGE_PERIOD VARCHAR(16777216),
	SPOTIFY_LISTENING_DEVICE VARCHAR(16777216),
	SPOTIFY_SUBSCRIPTION_PLAN VARCHAR(16777216),
	PREMIUM_SUB_WILLINGNESS VARCHAR(16777216),
	PREFFERED_PREMIUM_PLAN VARCHAR(16777216),
	PREFERRED_LISTENING_CONTENT VARCHAR(16777216),
	FAV_MUSIC_GENRE VARCHAR(16777216),
	MUSIC_TIME_SLOT VARCHAR(16777216),
	MUSIC_INFLUENCIAL_MOOD VARCHAR(16777216),
	MUSIC_LIS_FREQUENCY VARCHAR(16777216),
	MUSIC_EXPL_METHOD VARCHAR(16777216),
	MUSIC_RECC_RATING VARCHAR(16777216),
	POD_LIS_FREQUENCY VARCHAR(16777216),
	FAV_POD_GENRE VARCHAR(16777216),
	PREFFERED_POD_FORMAT VARCHAR(16777216),
	POD_HOST_PREFERENCE VARCHAR(16777216),
	PREFFERED_POD_DURATION VARCHAR(16777216),
	POD_VARIETY_SATISFACTION VARCHAR(16777216)
);

COPY INTO SPOTIFY.SPDATA.SPOTIFY_USER_DATA
FROM @SPOTIFY.SPDATA.MY_AZURE_STAGE_USER
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);

DROP TABLE SPOTIFY.SPDATA.TRACKS;

SELECT COUNT(*) FROM SPOTIFY.SPDATA.TRACKS;
SELECT COUNT(*) FROM SPOTIFY.SPDATA.SPOTIFY_USER_DATA;


use database SPOTIFY;
-- Step 1: Create the schema for Silver Layer
CREATE OR REPLACE SCHEMA SILVER_LAYER;
-- Step 2: Create the cleaned table in the silver layer
CREATE OR REPLACE TABLE SILVER_LAYER.TRACKS_CLEANED (
    track_id            STRING,            -- unique identifier
    track_name          STRING,            -- lowercased track name
    album               STRING,            -- lowercased album name
    release_date        DATE,              -- converted from string
    duration_ms         NUMBER,            -- track duration in milliseconds
    explicit            BOOLEAN,           -- true/false flag
    artist_names        STRING,            -- all artist names
    artist_count        NUMBER,            -- number of artists
    popularity          NUMBER,            -- popularity score
    language            STRING,            -- detected language
    streams             NUMBER             -- number of streams
);

INSERT INTO SILVER_LAYER.TRACKS_CLEANED
SELECT
    track_id,
    LOWER(track_name),
    LOWER(album),
    TRY_TO_DATE(release_date) AS release_date,
    TRY_TO_NUMBER(duration_ms),
    CASE 
        WHEN LOWER(explicit) IN ('true', 'yes', '1') THEN TRUE
        ELSE FALSE
    END AS explicit,
    LOWER(artist_names),
    TRY_TO_NUMBER(artist_count),
    TRY_TO_NUMBER(popularity),
    LOWER(language),
    TRY_TO_NUMBER(streams)
FROM SPOTIFY.SPDATA.TRACKS
WHERE track_id IS NOT NULL
  AND release_date IS NOT NULL;

SELECT COUNT(*) FROM SPOTIFY.SILVER_LAYER.TRACKS_CLEANED;

=================================================================================================

CREATE OR REPLACE TABLE GOLD_LAYER.dim_artist AS
SELECT DISTINCT
  MD5(LOWER(TRIM(f.value::STRING))) AS artist_id,
  TRIM(f.value::STRING) AS artist_name
FROM silver_layer.tracks_cleaned,
     LATERAL FLATTEN(input => SPLIT(artist_names, ',')) f;

SELECT COUNT(*) FROM SPOTIFY.GOLD_LAYER.DIM_ARTIST;

--------------------------------------------------------------

CREATE OR REPLACE TABLE SPOTIFY.GOLD_LAYER.dim_album AS
SELECT DISTINCT
  MD5(LOWER(album)) AS album_id,
  album AS album_name
FROM SPOTIFY.SILVER_LAYER.tracks_cleaned;

SELECT * FROM SPOTIFY.GOLD_LAYER.DIM_ALBUM;

----------------------------------------------------------------

CREATE OR REPLACE TABLE SPOTIFY.GOLD_LAYER.DIM_DATE AS
SELECT DISTINCT
  TO_DATE(release_date) AS date_id,
  YEAR(TO_DATE(release_date)) AS year,
  MONTH(TO_DATE(release_date)) AS month,
  DAY(TO_DATE(release_date)) AS day,
  DAYNAME(TO_DATE(release_date)) AS weekday
FROM silver_layer.tracks_cleaned
WHERE release_date IS NOT NULL;

SELECT * FROM SPOTIFY.GOLD_LAYER.DIM_DATE;

-------------------------------------------------------------------

CREATE OR REPLACE TABLE SPOTIFY.GOLD_LAYER.DIM_LANGUAGE AS
SELECT DISTINCT
  MD5(LOWER(language)) AS language_id,
  language
FROM silver_layer.tracks_cleaned;

SELECT * FROM SPOTIFY.GOLD_LAYER.DIM_LANGUAGE;

-------------------------------------------------------------------

CREATE OR REPLACE TABLE SPOTIFY.GOLD_LAYER.FACT_TRACKS AS
SELECT
  t.track_id,
  MD5(LOWER(SPLIT_PART(t.artist_names, ',', 1))) AS artist_id,
  MD5(LOWER(t.album)) AS album_id,
  TO_DATE(t.release_date) AS date_id,
  MD5(LOWER(t.language)) AS language_id,
  t.duration_ms,
  t.popularity,
  t.streams
FROM silver_layer.tracks_cleaned t;

SELECT * FROM SPOTIFY.GOLD_LAYER.FACT_TRACKS;
----------------------------------------------------------------------

CREATE OR REPLACE TABLE SPOTIFY.GOLD_LAYER.FACT_ARTIST_SUMMARY AS
WITH exploded_artists AS (
  SELECT
    t.track_id,
    TRIM(f.value::STRING) AS artist_name,
    t.album,
    t.track_name,
    t.popularity
  FROM silver_layer.tracks_cleaned t,
       LATERAL FLATTEN(input => SPLIT(t.artist_names, ',')) f
),

ranked_songs AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY LOWER(artist_name) ORDER BY popularity DESC, track_name) AS song_rank
  FROM exploded_artists
),

artist_summary AS (
  SELECT
    MD5(LOWER(artist_name)) AS artist_id,
    artist_name,
    COUNT(DISTINCT track_id) AS total_songs,
    COUNT(DISTINCT album) AS total_albums,
    MAX(CASE WHEN song_rank = 1 THEN track_name END) AS most_popular_song
  FROM ranked_songs
  GROUP BY artist_name
)

SELECT * FROM artist_summary;


SELECT * FROM SPOTIFY.GOLD_LAYER.FACT_ARTIST_SUMMARY;

---------------------------------------------------------------------------------
=================================================================================


CREATE OR REPLACE TABLE silver_layer.spotify_user_data_cleaned (
  age STRING,
  gender STRING,
  spotify_usage_period STRING,
  spotify_listening_device STRING,
  spotify_subscription_plan STRING,
  premium_sub_willingness STRING,
  preffered_premium_plan STRING,
  preferred_listening_content STRING,
  fav_music_genre STRING,
  music_time_slot STRING,
  music_influencial_mood STRING,
  music_lis_frequency STRING,
  music_expl_method STRING,
  music_recc_rating INT,
  pod_lis_frequency STRING,
  fav_pod_genre STRING,
  preffered_pod_format STRING,
  pod_host_preference STRING,
  preffered_pod_duration STRING,
  pod_variety_satisfaction STRING
);

INSERT INTO silver_layer.spotify_user_data_cleaned
SELECT
  TRIM(Age) AS age,
  INITCAP(TRIM(Gender)) AS gender,
  INITCAP(TRIM(spotify_usage_period)) AS spotify_usage_period,
  INITCAP(TRIM(spotify_listening_device)) AS spotify_listening_device,
  INITCAP(TRIM(spotify_subscription_plan)) AS spotify_subscription_plan,
  INITCAP(TRIM(premium_sub_willingness)) AS premium_sub_willingness,
  INITCAP(TRIM(preffered_premium_plan)) AS preffered_premium_plan,
  INITCAP(TRIM(preferred_listening_content)) AS preferred_listening_content,
  INITCAP(TRIM(fav_music_genre)) AS fav_music_genre,
  INITCAP(TRIM(music_time_slot)) AS music_time_slot,
  INITCAP(TRIM(music_Influencial_mood)) AS music_influencial_mood,
  INITCAP(TRIM(music_lis_frequency)) AS music_lis_frequency,
  INITCAP(TRIM(music_expl_method)) AS music_expl_method,
  INITCAP(TRIM(music_recc_rating)) AS music_recc_rating,
  INITCAP(TRIM(pod_lis_frequency)) AS pod_lis_frequency,
  INITCAP(TRIM(fav_pod_genre)) AS fav_pod_genre,
  INITCAP(TRIM(preffered_pod_format)) AS preffered_pod_format,
  INITCAP(TRIM(pod_host_preference)) AS pod_host_preference,
  INITCAP(TRIM(preffered_pod_duration)) AS preffered_pod_duration,
  INITCAP(TRIM(pod_variety_satisfaction)) AS pod_variety_satisfaction
FROM SPOTIFY.SPDATA.SPOTIFY_USER_DATA;

SELECT COUNT(*) FROM SPOTIFY.SILVER_LAYER.SPOTIFY_USER_DATA_CLEANED;

-----------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE SPOTIFY.gold_layer.dim_age_group (
  age_group_id INT AUTOINCREMENT PRIMARY KEY,
  age STRING
);

INSERT INTO gold_layer.dim_age_group (age)
SELECT DISTINCT age
FROM silver_layer.spotify_user_data_cleaned
WHERE age IS NOT NULL;
SELECT * FROM SPOTIFY.GOLD_LAYER.DIM_AGE_GROUP;
--------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE gold_layer.dim_gender (
  gender_id INT AUTOINCREMENT PRIMARY KEY,
  gender STRING
);

INSERT INTO SPOTIFY.GOLD_LAYER.DIM_GENDER (gender)
SELECT DISTINCT gender
FROM SPOTIFY.SILVER_LAYER.SPOTIFY_USER_DATA_CLEANED
WHERE gender IS NOT NULL;

SELECT * FROM SPOTIFY.GOLD_LAYER.DIM_GENDER;

-----------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE gold_layer.dim_device (
  device_id INT AUTOINCREMENT PRIMARY KEY,
  device STRING
);

INSERT INTO gold_layer.dim_device (device)
SELECT DISTINCT spotify_listening_device
FROM silver_layer.spotify_user_data_cleaned
WHERE spotify_listening_device IS NOT NULL;

SELECT * FROM SPOTIFY.GOLD_LAYER.DIM_DEVICE;

-------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE gold_layer.dim_subscription_plan (
  plan_id INT AUTOINCREMENT PRIMARY KEY,
  subscription_plan STRING
);

INSERT INTO gold_layer.dim_subscription_plan (subscription_plan)
SELECT DISTINCT spotify_subscription_plan
FROM silver_layer.spotify_user_data_cleaned
WHERE spotify_subscription_plan IS NOT NULL;

SELECT * FROM SPOTIFY.GOLD_LAYER.DIM_SUBSCRIPTION_PLAN;

--------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE gold_layer.dim_premium_willingness (
  premium_id INT AUTOINCREMENT PRIMARY KEY,
  premium_willingness STRING,
  preferred_plan STRING
);

INSERT INTO gold_layer.dim_premium_willingness (premium_willingness, preferred_plan)
SELECT DISTINCT premium_sub_willingness, preffered_premium_plan
FROM silver_layer.spotify_user_data_cleaned
WHERE premium_sub_willingness IS NOT NULL;

SELECT * FROM SPOTIFY.GOLD_LAYER.DIM_PREMIUM_WILLINGNESS; 

---------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE gold_layer.dim_listening_preferences (
  listening_pref_id INT AUTOINCREMENT PRIMARY KEY,
  preferred_content STRING,
  fav_music_genre STRING,
  music_time_slot STRING,
  music_mood STRING
);

INSERT INTO gold_layer.dim_listening_preferences (
  preferred_content, fav_music_genre, music_time_slot, music_mood
)
SELECT DISTINCT
  preferred_listening_content,
  fav_music_genre,
  music_time_slot,
  music_influencial_mood
FROM silver_layer.spotify_user_data_cleaned
WHERE preferred_listening_content IS NOT NULL;

SELECT * FROM SPOTIFY.GOLD_LAYER.DIM_LISTENING_PREFERENCES;

-------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE gold_layer.dim_podcast_behavior (
  podcast_behavior_id INT AUTOINCREMENT PRIMARY KEY,
  lis_frequency STRING,
  fav_genre STRING,
  format STRING,
  host_pref STRING,
  duration_pref STRING,
  satisfaction STRING
);

INSERT INTO gold_layer.dim_podcast_behavior (
  lis_frequency, fav_genre, format, host_pref, duration_pref, satisfaction
)
SELECT DISTINCT
  pod_lis_frequency,
  fav_pod_genre,
  preffered_pod_format,
  pod_host_preference,
  preffered_pod_duration,
  pod_variety_satisfaction
FROM silver_layer.spotify_user_data_cleaned
WHERE pod_lis_frequency IS NOT NULL;
SELECT * FROM SPOTIFY.GOLD_LAYER.DIM_PODCAST_BEHAVIOR;

------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE gold_layer.fact_user_behavior AS
SELECT DISTINCT
  ROW_NUMBER() OVER (ORDER BY s.age, s.gender) AS user_id,  -- surrogate key for fact table

  ag.age_group_id,
  g.gender_id,
  d.device_id,
  sp.plan_id AS subscription_plan_id,
  pw.premium_id,
  lp.listening_pref_id,
  md.discovery_id,
  pb.podcast_behavior_id,

  -- measurable metric
  TRY_CAST(s.music_recc_rating AS INT) AS music_recc_rating

FROM silver_layer.spotify_user_data_cleaned s

-- join with dimension tables
LEFT JOIN gold_layer.dim_age_group ag
  ON s.age = ag.age

LEFT JOIN gold_layer.dim_gender g
  ON s.gender = g.gender

LEFT JOIN gold_layer.dim_device d
  ON s.spotify_listening_device = d.device

LEFT JOIN gold_layer.dim_subscription_plan sp
  ON s.spotify_subscription_plan = sp.subscription_plan

LEFT JOIN gold_layer.dim_premium_willingness pw
  ON s.premium_sub_willingness = pw.premium_willingness
 AND s.preffered_premium_plan = pw.preferred_plan

LEFT JOIN gold_layer.dim_listening_preferences lp
  ON s.preferred_listening_content = lp.preferred_content
 AND s.fav_music_genre = lp.fav_music_genre
 AND s.music_time_slot = lp.music_time_slot
 AND s.music_influencial_mood = lp.music_mood

LEFT JOIN gold_layer.dim_music_discovery md
  ON s.music_lis_frequency = md.lis_frequency
 AND s.music_expl_method = md.expl_method

LEFT JOIN gold_layer.dim_podcast_behavior pb
  ON s.pod_lis_frequency = pb.lis_frequency
 AND s.fav_pod_genre = pb.fav_genre
 AND s.preffered_pod_format = pb.format
 AND s.pod_host_preference = pb.host_pref
 AND s.preffered_pod_duration = pb.duration_pref
 AND s.pod_variety_satisfaction = pb.satisfaction;


SELECT * FROM SPOTIFY.GOLD_LAYER.FACT_USER_BEHAVIOR;
========================================================================================================

========================================================================================================

CREATE OR REPLACE VIEW SPOTIFY.GOLD_LAYER.VW_MOST_PREFERRED_GENRE_BY_AGE AS
SELECT
  AGE_GROUP,
  FAV_MUSIC_GENRE,
  GENRE_COUNT
FROM (
  SELECT
    AGE AS AGE_GROUP,
    FAV_MUSIC_GENRE,
    COUNT(*) AS GENRE_COUNT,
    ROW_NUMBER() OVER (PARTITION BY AGE ORDER BY COUNT(*) DESC) AS genre_rank
  FROM SPOTIFY.SILVER_LAYER.SPOTIFY_USER_DATA_CLEANED
  WHERE FAV_MUSIC_GENRE IS NOT NULL AND AGE IS NOT NULL
  GROUP BY AGE, FAV_MUSIC_GENRE
) ranked_genres
WHERE genre_rank = 1
ORDER BY AGE_GROUP;

SELECT * FROM SPOTIFY.GOLD_LAYER.VW_MOST_PREFERRED_GENRE_BY_AGE;

-----------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW SPOTIFY.GOLD_LAYER.VW_MOST_PREFERRED_GENRE_BY_AGE AS
SELECT
  AGE_GROUP,
  FAV_MUSIC_GENRE,
  GENRE_COUNT
FROM (
  SELECT
    AGE AS AGE_GROUP,
    FAV_MUSIC_GENRE,
    COUNT(*) AS GENRE_COUNT,
    ROW_NUMBER() OVER (PARTITION BY AGE ORDER BY COUNT(*) DESC) AS genre_rank
  FROM SPOTIFY.SILVER_LAYER.SPOTIFY_USER_DATA_CLEANED
  WHERE FAV_MUSIC_GENRE IS NOT NULL AND AGE IS NOT NULL
  GROUP BY AGE, FAV_MUSIC_GENRE
) ranked_genres
WHERE genre_rank = 1
ORDER BY 
  CASE AGE_GROUP
    WHEN '6-12' THEN 1
    WHEN '12-20' THEN 2
    WHEN '20-35' THEN 3
    WHEN '35-60' THEN 4
    WHEN '60 +' THEN 5
    ELSE 6
  END;

SELECT * FROM SPOTIFY.GOLD_LAYER.VW_MOST_PREFERRED_GENRE_BY_AGE;

------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW gold_layer.vw_dim_listening_preferences AS
SELECT DISTINCT
  preferred_listening_content AS preferred_content,
  fav_music_genre,
  music_time_slot,
  music_influencial_mood AS music_mood
FROM silver_layer.spotify_user_data_cleaned
WHERE preferred_listening_content IS NOT NULL;

SELECT * FROM SPOTIFY.GOLD_LAYER.VW_DIM_LISTENING_PREFERENCES;

------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW SPOTIFY.GOLD_LAYER.VW_TRACK_REUSABILITY_RATIO AS
SELECT
    TRACK_NAME,
    COUNT(DISTINCT ALBUM) AS album_count,
    CASE 
        WHEN COUNT(DISTINCT ALBUM) > 1 THEN 'High Reuse'
        ELSE 'Single Use'
    END AS reuse_category
FROM SPOTIFY.SILVER_LAYER.TRACKS_CLEANED
WHERE LOWER(TRACK_NAME) NOT ILIKE 'a'
GROUP BY TRACK_NAME;

SELECT * FROM SPOTIFY.GOLD_LAYER.VW_TRACK_REUSABILITY_RATIO;

--------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW SPOTIFY.GOLD_LAYER.VW_VIRAL_COEFFICIENT AS
SELECT
    TRACK_NAME,
    POPULARITY,
    DATEDIFF(DAY, TO_DATE(RELEASE_DATE), CURRENT_DATE) AS days_since_release,
    ROUND(POPULARITY / NULLIF(DATEDIFF(DAY, TO_DATE(RELEASE_DATE), CURRENT_DATE), 0), 2) AS viral_coefficient
FROM SPOTIFY.SILVER_LAYER.TRACKS_CLEANED;

SELECT * FROM SPOTIFY.GOLD_LAYER.VW_VIRAL_COEFFICIENT;

----------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW SPOTIFY.GOLD_LAYER.VW_CROSS_REGION_GENRE_DRIFT AS
SELECT
    LANGUAGE,
    COUNT(DISTINCT TRACK_NAME) AS total_tracks,
    COUNT(DISTINCT ARTIST_NAMES) AS unique_artists,
    COUNT(DISTINCT ALBUM) AS unique_albums
FROM SPOTIFY.SILVER_LAYER.TRACKS_CLEANED
GROUP BY LANGUAGE;

SELECT * FROM SPOTIFY.GOLD_LAYER.VW_CROSS_REGION_GENRE_DRIFT;

----------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW SPOTIFY.GOLD_LAYER.VW_EMERGING_ARTIST_MOMENTUM AS
WITH recent_tracks AS (
    SELECT 
        ARTIST_NAMES,
        TRACK_NAME,
        RELEASE_DATE,
        POPULARITY,
        DATEDIFF(DAY, RELEASE_DATE, CURRENT_DATE) AS days_since_release
    FROM SPOTIFY.SILVER_LAYER.TRACKS_CLEANED
    WHERE RELEASE_DATE >= DATEADD(DAY, -60, CURRENT_DATE)
),

artist_stats AS (
    SELECT
        ARTIST_NAMES,
        COUNT(*) AS recent_release_count,
        AVG(POPULARITY) AS avg_recent_popularity,
        MAX(POPULARITY) - MIN(POPULARITY) AS popularity_growth
    FROM recent_tracks
    WHERE days_since_release <= 30
    GROUP BY ARTIST_NAMES
)

SELECT
    ARTIST_NAMES,
    recent_release_count,
    avg_recent_popularity,
    popularity_growth,
    (0.4 * popularity_growth + 0.4 * avg_recent_popularity + 0.2 * recent_release_count) AS momentum_score

FROM artist_stats
ORDER BY momentum_score DESC;

SELECT * FROM SPOTIFY.GOLD_LAYER.VW_EMERGING_ARTIST_MOMENTUM;
------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW SPOTIFY.GOLD_LAYER.VW_TRACK_RELEASE_TREND AS
SELECT
    DATE_TRUNC('MONTH', RELEASE_DATE) AS release_month,
    COUNT(*) AS track_count
FROM SPOTIFY.SILVER_LAYER.TRACKS_CLEANED
WHERE RELEASE_DATE IS NOT NULL
GROUP BY DATE_TRUNC('MONTH', RELEASE_DATE)
ORDER BY release_month;

SELECT * FROM SPOTIFY.GOLD_LAYER.VW_TRACK_RELEASE_TREND;
-------------------------------------------------------------------------------
ALTER TABLE SPOTIFY.GOLD_LAYER.FACT_USER_BEHAVIOR
ADD CONSTRAINT FK_FACT_USER_SUBSCRIPTION
FOREIGN KEY (SUBSCRIPTION_PLAN_ID)
REFERENCES SPOTIFY.GOLD_LAYER.DIM_SUBSCRIPTION_PLAN(PLAN_ID);
------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW SPOTIFY.GOLD_LAYER.VW_SUBSCRIPTION_STATS_BY_AGE_GENDER AS
SELECT
    AGE,
    GENDER,
    SPOTIFY_SUBSCRIPTION_PLAN AS SUBSCRIPTION_PLAN,
    COUNT(*) AS TOTAL_USERS
FROM SPOTIFY.SILVER_LAYER.SPOTIFY_USER_DATA_CLEANED
GROUP BY AGE, GENDER, SPOTIFY_SUBSCRIPTION_PLAN;

SELECT * FROM SPOTIFY.GOLD_LAYER.VW_SUBSCRIPTION_STATS_BY_AGE_GENDER;
-------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW SPOTIFY.GOLD_LAYER.VW_DEVICE_USAGE_BY_SUBSCRIPTION AS
SELECT
    SPOTIFY_SUBSCRIPTION_PLAN AS SUBSCRIPTION_PLAN,
    SPOTIFY_LISTENING_DEVICE AS DEVICE_USED,
    COUNT(*) AS USER_COUNT
FROM SPOTIFY.SILVER_LAYER.SPOTIFY_USER_DATA_CLEANED
GROUP BY SPOTIFY_SUBSCRIPTION_PLAN, SPOTIFY_LISTENING_DEVICE;

SELECT * FROM SPOTIFY.GOLD_LAYER.VW_DEVICE_USAGE_BY_SUBSCRIPTION;

------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW SPOTIFY.GOLD_LAYER.VW_MUSIC_PREFS_BY_TIME_SLOT AS
SELECT
    MUSIC_TIME_SLOT,
    MUSIC_INFLUENCIAL_MOOD AS MUSIC_MOOD,
    COUNT(*) AS OCCURRENCES
FROM SPOTIFY.SILVER_LAYER.SPOTIFY_USER_DATA_CLEANED
GROUP BY MUSIC_TIME_SLOT, MUSIC_INFLUENCIAL_MOOD;

SELECT * FROM SPOTIFY.GOLD_LAYER.VW_MUSIC_PREFS_BY_TIME_SLOT;

-------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW SPOTIFY.GOLD_LAYER.VW_PREMIUM_CONVERSION_RATE AS
SELECT
    AGE,
    COUNT(CASE WHEN PREMIUM_SUB_WILLINGNESS = 'Yes' THEN 1 END) AS WILLING,
    COUNT(*) AS TOTAL_USERS,
    ROUND(COUNT(CASE WHEN PREMIUM_SUB_WILLINGNESS = 'Yes' THEN 1 END) * 100.0 / COUNT(*), 2) AS CONVERSION_PERCENT
FROM SPOTIFY.SILVER_LAYER.SPOTIFY_USER_DATA_CLEANED
GROUP BY AGE;

SELECT * FROM SPOTIFY.GOLD_LAYER.VW_PREMIUM_CONVERSION_RATE;

------------------------------------------------------------------------------------------------------

SELECT * FROM SPOTIFY.GOLD_LAYER.VW_MUSIC_RECC_RATING_BY_GENRE;

-----------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW SPOTIFY.GOLD_LAYER.VW_PODCAST_PREFERENCES AS
SELECT
    FAV_POD_GENRE,
    POD_HOST_PREFERENCE,
    COUNT(*) AS PODCAST_USER_COUNT
FROM SPOTIFY.SILVER_LAYER.SPOTIFY_USER_DATA_CLEANED
GROUP BY FAV_POD_GENRE, POD_HOST_PREFERENCE;;

SELECT * FROM SPOTIFY.GOLD_LAYER.VW_PODCAST_PREFERENCES;
----------------------------------------------------------------------------------------







































