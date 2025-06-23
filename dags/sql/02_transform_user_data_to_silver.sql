CREATE OR REPLACE TABLE SILVER_LAYER.SPOTIFY_USER_DATA_CLEANED AS
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