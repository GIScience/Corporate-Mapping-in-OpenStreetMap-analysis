SET THREADS TO 50;
SET max_memory TO '150GB';

INSTALL SPATIAL;
LOAD SPATIAL;

INSTALL h3 FROM community;
LOAD h3;


----------------------------------------------------------------
-- Get all countries with corporate mapping above threshold
----------------------------------------------------------------
SET VARIABLE countries = (
SELECT
    array_agg(iso_a3_eh)
FROM st_read('/data/processing/corporate-stats/world_boundaries_with_stats.gpkg')
WHERE 1=1
    AND share_sum_edits_corporate > 0.10
    AND sum_edits_corporate > 100_000
);

----------------------------------------------------------------
-- load OSM user IDs from csv into variable for each company
----------------------------------------------------------------
SET VARIABLE amazon_user_ids = (SELECT array_agg("User ID") FROM '/data/processing/corporate-stats/user_ids/AmazonUser.csv');
SET VARIABLE apple_user_ids = (SELECT array_agg("User ID") FROM '/data/processing/corporate-stats/user_ids/AppleUser.csv');
SET VARIABLE applogica_user_ids = (SELECT array_agg("User ID") FROM '/data/processing/corporate-stats/user_ids/AppLogicaUser.csv');
SET VARIABLE balad_user_ids = (SELECT array_agg("User ID") FROM '/data/processing/corporate-stats/user_ids/BaladUser.csv');
SET VARIABLE bolt_user_ids = (SELECT array_agg("User ID") FROM '/data/processing/corporate-stats/user_ids/BoltUser.csv');
SET VARIABLE devseed_user_ids = (SELECT array_agg("User ID") FROM '/data/processing/corporate-stats/user_ids/DevSeedUser.csv');
SET VARIABLE digitalegypt_user_ids = (SELECT array_agg("User ID") FROM '/data/processing/corporate-stats/user_ids/DigitalEgyptUser.csv');
SET VARIABLE expedia_user_ids = (SELECT array_agg("User ID") FROM '/data/processing/corporate-stats/user_ids/ExpediaUser.csv');
SET VARIABLE gojek_user_ids = (SELECT array_agg("User ID") FROM '/data/processing/corporate-stats/user_ids/GojekUser.csv');
SET VARIABLE grab_user_ids = (SELECT array_agg("User ID") FROM '/data/processing/corporate-stats/user_ids/GrabUser.csv');
SET VARIABLE graphmasters_user_ids = (SELECT array_agg("User ID") FROM '/data/processing/corporate-stats/user_ids/GraphmastersUser.csv');
SET VARIABLE kaart_user_ids = (SELECT array_agg("User ID") FROM '/data/processing/corporate-stats/user_ids/KaartUser.csv');
SET VARIABLE kontur_user_ids = (SELECT array_agg("User ID") FROM '/data/processing/corporate-stats/user_ids/KonturUser.csv');
SET VARIABLE lightcyphers_user_ids = (SELECT array_agg("User ID") FROM '/data/processing/corporate-stats/user_ids/LightcyphersUser.csv');
SET VARIABLE lyft_user_ids = (SELECT array_agg("User ID") FROM '/data/processing/corporate-stats/user_ids/LyftUser.csv');
SET VARIABLE mapbox_user_ids = (SELECT array_agg("User ID") FROM '/data/processing/corporate-stats/user_ids/MapboxUser.csv');
SET VARIABLE menextbillio_user_ids = (SELECT array_agg("User ID") FROM '/data/processing/corporate-stats/user_ids/MeNextBillionUser.csv');
SET VARIABLE meta_user_ids = (SELECT array_agg("User ID") FROM '/data/processing/corporate-stats/user_ids/MetaUser.csv');
SET VARIABLE microsoft_user_ids = (SELECT array_agg("User ID") FROM '/data/processing/corporate-stats/user_ids/MicrosoftUser.csv');
SET VARIABLE neshan_user_ids = (SELECT array_agg("User ID") FROM '/data/processing/corporate-stats/user_ids/NeshanUser.csv');
SET VARIABLE rocketdata_user_ids = (SELECT array_agg("User ID") FROM '/data/processing/corporate-stats/user_ids/RocketdataUser.csv');
SET VARIABLE snapp_user_ids = (SELECT array_agg("User ID") FROM '/data/processing/corporate-stats/user_ids/SnappUser.csv');
SET VARIABLE snap_user_ids = (SELECT array_agg("User ID") FROM '/data/processing/corporate-stats/user_ids/SnapUser.csv');
SET VARIABLE stackbox_user_ids = (SELECT array_agg("User ID") FROM '/data/processing/corporate-stats/user_ids/StackboxUser.csv');
SET VARIABLE swiggy_user_ids = (SELECT array_agg("User ID") FROM '/data/processing/corporate-stats/user_ids/SwiggyUser.csv');
SET VARIABLE telenav_user_ids = (SELECT array_agg("User ID") FROM '/data/processing/corporate-stats/user_ids/TelenavUser.csv');
SET VARIABLE tfnsw_user_ids = (SELECT array_agg("User ID") FROM '/data/processing/corporate-stats/user_ids/TfNSWUser.csv');
SET VARIABLE tidbo_user_ids = (SELECT array_agg("User ID") FROM '/data/processing/corporate-stats/user_ids/TIDBOUser.csv');
SET VARIABLE tomtom_user_ids = (SELECT array_agg("User ID") FROM '/data/processing/corporate-stats/user_ids/TomTomUser.csv');
SET VARIABLE uber_user_ids = (SELECT array_agg("User ID") FROM '/data/processing/corporate-stats/user_ids/UberUser.csv');
SET VARIABLE wigeogis_user_ids = (SELECT array_agg("User ID") FROM '/data/processing/corporate-stats/user_ids/WIGeoGISUser.csv');
SET VARIABLE wonder_user_ids = (SELECT array_agg("User ID") FROM '/data/processing/corporate-stats/user_ids/WonderUser.csv');
SET VARIABLE zartico_user_ids = (SELECT array_agg("User ID") FROM '/data/processing/corporate-stats/user_ids/ZarticoUser.csv');


----------------------------------------------------------------
-- load OSM user IDs from csv into variable for each company
----------------------------------------------------------------
DROP TABLE IF EXISTS ohsome_contributions;
CREATE TABLE ohsome_contributions AS
    (
    SELECT
    country_iso_a3,
    CASE
        WHEN CAST(hashtags AS VARCHAR) ILIKE '%amap%' THEN 'Amazon'
        WHEN list_contains(getvariable('amazon_user_ids'), user_id ) THEN 'Amazon'

        WHEN CAST(hashtags AS VARCHAR) ILIKE '%adt%' THEN 'Apple'
        WHEN list_contains(getvariable('apple_user_ids'), user_id ) THEN 'Apple'

        WHEN CAST(hashtags AS VARCHAR) ILIKE '%AppLogica%' THEN 'AppLogica'
        WHEN list_contains(getvariable('applogica_user_ids'), user_id ) THEN 'AppLogica'

        WHEN list_contains(getvariable('balad_user_ids'), user_id ) THEN 'Balad'

        WHEN CAST(hashtags AS VARCHAR) ILIKE '%bolt%' THEN 'Bolt'
        WHEN list_contains(getvariable('bolt_user_ids'), user_id ) THEN 'Bolt'

        WHEN list_contains(getvariable('devseed_user_ids'), user_id ) THEN 'DevelopmentSeed'

        WHEN CAST(hashtags AS VARCHAR) ILIKE '%DigitalEgypt%' THEN 'DigitalEgypt'
        WHEN list_contains(getvariable('digitalegypt_user_ids'), user_id ) THEN 'DigitalEgypt'

        WHEN CAST(hashtags AS VARCHAR) ILIKE '%expedia%' THEN 'Expedia'
        WHEN list_contains(getvariable('expedia_user_ids'), user_id ) THEN 'Expedia'

        WHEN CAST(hashtags AS VARCHAR) ILIKE '%gojek%' THEN 'Gojek'
        WHEN list_contains(getvariable('gojek_user_ids'), user_id ) THEN 'Gojek'

        WHEN CAST(hashtags AS VARCHAR) ILIKE '%grab%' THEN 'Grab'
        WHEN list_contains(getvariable('grab_user_ids'), user_id ) THEN 'Grab'

        WHEN list_contains(getvariable('graphmasters_user_ids'), user_id ) THEN 'Graphmasters'

        WHEN CAST(hashtags AS VARCHAR) ILIKE '%Kaart%' THEN 'Kaart'
        WHEN list_contains(getvariable('kaart_user_ids'), user_id ) THEN 'Kaart'

        WHEN CAST(hashtags AS VARCHAR) ILIKE '%Komoot%' THEN 'Komoot'

        WHEN CAST(hashtags AS VARCHAR) ILIKE '%Kontur%' THEN 'Kontur'
        WHEN list_contains(getvariable('kontur_user_ids'), user_id ) THEN 'Kontur'

        WHEN CAST(hashtags AS VARCHAR) ILIKE '%Lightcyphers%' THEN 'Lightcyphers'
        WHEN list_contains(getvariable('lightcyphers_user_ids'), user_id ) THEN 'Lightcyphers'

        WHEN list_contains(getvariable('lyft_user_ids'), user_id ) THEN 'Lyft'

        WHEN CAST(hashtags AS VARCHAR) ILIKE '%mapbox%' THEN 'Mapbox'
        WHEN CAST(hashtags AS VARCHAR) ILIKE '%mbx%' THEN 'Mapbox'
        WHEN list_contains(getvariable('mapbox_user_ids'), user_id ) THEN 'Mapbox'

        WHEN list_contains(getvariable('menextbillio_user_ids'), user_id ) THEN 'MeNextBillion'

        WHEN list_contains(getvariable('meta_user_ids'), user_id ) THEN 'Meta'

        WHEN CAST(hashtags AS VARCHAR) ILIKE '%MSFTOpenMaps%' THEN 'Microsoft'
        WHEN list_contains(getvariable('microsoft_user_ids'), user_id ) THEN 'Microsoft'

        WHEN CAST(hashtags AS VARCHAR) ILIKE '%نشان%' THEN 'Neshan'
        WHEN list_contains(getvariable('neshan_user_ids'), user_id ) THEN 'Neshan'

        WHEN CAST(hashtags AS VARCHAR) ILIKE '%RocketData%' THEN 'RocketData'
        WHEN list_contains(getvariable('rocketdata_user_ids'), user_id ) THEN 'RocketData'

        WHEN CAST(hashtags AS VARCHAR) ILIKE '%Snapp%' THEN 'Snapp'
        WHEN list_contains(getvariable('snapp_user_ids'), user_id ) THEN 'Snapp'

        WHEN CAST(hashtags AS VARCHAR) ILIKE '%disputed_by_claimed_by%' THEN 'Snap'
        WHEN list_contains(getvariable('snap_user_ids'), user_id ) THEN 'Snap'

        WHEN CAST(hashtags AS VARCHAR) ILIKE '%stackbox%' THEN 'Stackbox'
        WHEN list_contains(getvariable('stackbox_user_ids'), user_id ) THEN 'Stackbox'

        WHEN list_contains(getvariable('swiggy_user_ids'), user_id ) THEN 'Swiggy'

        WHEN CAST(hashtags AS VARCHAR) ILIKE '%Telenav%' THEN 'Telenav'
        WHEN list_contains(getvariable('telenav_user_ids'), user_id ) THEN 'Telenav'

        WHEN list_contains(getvariable('tfnsw_user_ids'), user_id ) THEN 'TfNWS'

        WHEN CAST(hashtags AS VARCHAR) ILIKE '%TIDBO%' THEN 'TIDBO'
        WHEN list_contains(getvariable('tidbo_user_ids'), user_id ) THEN 'TIDBO'

        WHEN CAST(hashtags AS VARCHAR) ILIKE '%tomtom%' THEN 'TomTom'
        WHEN list_contains(getvariable('tomtom_user_ids'), user_id ) THEN 'TomTom'

        WHEN list_contains(getvariable('uber_user_ids'), user_id ) THEN 'Uber'

        WHEN CAST(hashtags AS VARCHAR) ILIKE '%WIGeoGIS-OMV%' THEN 'WIGeoGIS-OMV'
        WHEN list_contains(getvariable('wigeogis_user_ids'), user_id ) THEN 'WIGeoGIS-OMV'

        WHEN list_contains(getvariable('wonder_user_ids'), user_id ) THEN 'Wonder'

        WHEN list_contains(getvariable('zartico_user_ids'), user_id ) THEN 'Zartico'

        ELSE 'nc'
    END AS corporation,
    case
        when valid_from < '2019-06-01' then 'pre_t0'
        when valid_from >= '2019-06-01' and valid_from < '2021-06-01' then 't0'
        when valid_from >= '2021-06-01' and valid_from < '2023-06-01' then 't1'
        when valid_from >= '2023-06-01' then 'post_t1'
    end as time_range,
    FROM read_parquet('/data/osmdb/ohsome-parquet/ohsome-stats-104933/*.parquet')
    WHERE 1=1
        AND valid_from >= '2019-06-01'
        AND valid_from < '2023-06-01'
        AND list_has_any(country_iso_a3, getvariable('countries'))
);

----------------------------------------------------
-- sum edits per company and country
----------------------------------------------------
DROP TABLE IF EXISTS step_1;
CREATE TABLE step_1 AS
SELECT
    country_iso_a3,
    corporation,
    SUM(CASE
        WHEN time_range = 't0' THEN 1
        ELSE 0
    END) as t0_edits,
    SUM(CASE
        WHEN time_range = 't1' THEN 1
        ELSE 0
    END) as t1_edits,
FROM ohsome_contributions
WHERE 1=1
    and corporation != 'nc'
GROUP BY country_iso_a3, corporation
ORDER BY country_iso_a3
;

DROP TABLE IF EXISTS step_2;
CREATE TABLE step_2 AS
SELECT
    unnest(country_iso_a3) as country_iso_a3,
    corporation,
    t0_edits,
    t1_edits
FROM step_1;


COPY
(
SELECT
    country_iso_a3,
    corporation,
    sum(t0_edits) as t0_edits_total,
    sum(t1_edits) as t1_edits_total,
    t1_edits_total - t0_edits_total as diff_edits,
    (t1_edits_total - t0_edits_total) / (t1_edits_total + t0_edits_total) as norm_diff_edits
FROM step_2
WHERE 1=1
    AND list_contains(getvariable('countries'), country_iso_a3)
GROUP BY country_iso_a3, corporation
ORDER BY country_iso_a3, diff_edits
) TO '/data/processing/corporate-stats/figX_corporate_mapping_trends_per_country.csv'(
        FORMAT CSV,
        OVERWRITE_OR_IGNORE
);