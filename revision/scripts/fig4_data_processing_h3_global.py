import sys
import logging
import duckdb

con = duckdb.connect(
    config={
        'threads': 32,
        'max_memory': '150GB'
    }
)

# configure logging
root = logging.getLogger()
root.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)


def run_workflow():
    logging.info(f"start calculation for h3_country_map")
    output_file = f'/data/processing/corporate-stats/results/h3_map_global.gpkg'

    sql = f"""
    SET THREADS TO 50;
    SET max_memory TO '50GB';
    
    INSTALL SPATIAL;
    LOAD SPATIAL;
    
    INSTALL h3 FROM community;
    LOAD h3;
    
    -- load OSM user IDs from csv into variable for each company
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
    """
    con.sql(sql)
    logging.info(f"finish set variables")

    sql = f"""
    ----------------------------------------------------------------------
    -- Monthly mapping activity per company per h3 cell at resolution 6
    ----------------------------------------------------------------------
    
    DROP TABLE IF EXISTS stats_per_h3_cell_per_month;
    CREATE TABLE stats_per_h3_cell_per_month AS
    SELECT
        h3_latlng_to_cell(
            centroid.ST_GeomFromWKB().ST_y(), -- latitude = y
            centroid.ST_GeomFromWKB().ST_x(), -- longitude = x
            6
        ) as h3_r6
        ,date_trunc('month', changeset_timestamp) as month
        ,CASE
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
        COUNT(*) AS edits,
        COUNT(DISTINCT user_id) as contributors
    FROM read_parquet("/data/osmdb/ohsome-parquet/ohsome-stats-104933/*.parquet")
    GROUP BY h3_r6, month, corporation
    ORDER BY h3_r6, month, corporation
    ;
    
    DESCRIBE stats_per_h3_cell_per_month;
    """
    con.sql(sql)
    logging.info(f"created temp table stats_per_h3_cell_per_month")

    sql = """
    -------------------------------------------------------------------
    -- Monthly Average mapping activity for t0 and t1
    -------------------------------------------------------------------
    
    DROP TABLE IF EXISTS avg_stats_per_h3_cell_per_month;
    CREATE TABLE avg_stats_per_h3_cell_per_month AS
    (
    with step_1 AS (
        select
            h3_r6,
            month,
            SUM(CASE
                WHEN corporation = 'nc' THEN edits
                ELSE 0
            END) as edits_non_corporate,
            SUM(CASE
                WHEN corporation != 'nc' THEN edits
                ELSE 0
            END) as edits_corporate,
            SUM(CASE
                WHEN corporation = 'nc' THEN contributors
                ELSE 0
            END) as contributors_non_corporate,
            SUM(CASE
                WHEN corporation != 'nc' THEN contributors
                ELSE 0
            END) as contributors_corporate,
        from stats_per_h3_cell_per_month
        GROUP BY h3_r6, month
        order by month
    )
    SELECT
        h3_r6,
        case
            when month < '2019-06-01' then 'pre_t0'
            when month >= '2019-06-01' and month < '2021-06-01' then 't0'
            when month >= '2021-06-01' and month < '2023-06-01' then 't1'
            when month >= '2023-06-01' then 'post_t1'
        end as time_range,
        avg(edits_non_corporate) as avg_monthly_edits_non_corporate,
        avg(edits_corporate) as avg_monthly_edits_corporate,
        avg(contributors_non_corporate) as avg_monthly_contributors_non_corporate,
        avg(contributors_corporate) as avg_monthly_contributors_corporate,
        sum(edits_non_corporate)::int as sum_edits_non_corporate,
        sum(edits_corporate)::int as sum_edits_corporate
    FROM step_1
    GROUP BY h3_r6, time_range
    ORDER BY time_range
    )
    ;
    
    DESCRIBE avg_stats_per_h3_cell_per_month;
    """
    con.sql(sql)
    logging.info(f"created temp table avg_stats_per_h3_cell_per_month")


    sql = """
    -------------------------------------------------------------------
    -- Pivot table
    -------------------------------------------------------------------
    
    DROP TABLE IF EXISTS avg_stats_per_h3_cell_per_month_flipped;
    CREATE TABLE avg_stats_per_h3_cell_per_month_flipped AS (
    SELECT
        h3_r6,
        -- t0
        max(CASE
            WHEN time_range = 't0' THEN avg_monthly_edits_non_corporate
            ELSE 0
        END) as t0_avg_monthly_edits_non_corporate,
        max(CASE
            WHEN time_range = 't0' THEN avg_monthly_edits_corporate
            ELSE 0
        END) as t0_avg_monthly_edits_corporate,
        max(CASE
            WHEN time_range = 't0' THEN avg_monthly_contributors_non_corporate
            ELSE 0
        END) as t0_avg_monthly_contributors_non_corporate,
        max(CASE
            WHEN time_range = 't0' THEN avg_monthly_contributors_corporate
            ELSE 0
        END) as t0_avg_monthly_contributors_corporate,
        max(CASE
            WHEN time_range = 't0' THEN sum_edits_non_corporate
            ELSE 0
        END) as t0_sum_edits_non_corporate,
        max(CASE
            WHEN time_range = 't0' THEN sum_edits_corporate
            ELSE 0
        END) as t0_sum_edits_corporate,
        -- t1
        max(CASE
            WHEN time_range = 't1' THEN avg_monthly_edits_non_corporate
            ELSE 0
        END) as t1_avg_monthly_edits_non_corporate,
        max(CASE
            WHEN time_range = 't1' THEN avg_monthly_edits_corporate
            ELSE 0
        END) as t1_avg_monthly_edits_corporate,
        max(CASE
            WHEN time_range = 't1' THEN avg_monthly_contributors_non_corporate
            ELSE 0
        END) as t1_avg_monthly_contributors_non_corporate,
        max(CASE
            WHEN time_range = 't1' THEN avg_monthly_contributors_corporate
            ELSE 0
        END) as t1_avg_monthly_contributors_corporate,
        max(CASE
            WHEN time_range = 't1' THEN sum_edits_non_corporate
            ELSE 0
        END) as t1_sum_edits_non_corporate,
        max(CASE
            WHEN time_range = 't1' THEN sum_edits_corporate
            ELSE 0
        END) as t1_sum_edits_corporate,
        
    FROM avg_stats_per_h3_cell_per_month
    WHERE time_range = 't1' or time_range = 't0'
    GROUP BY h3_r6
    );
    
    DESCRIBE avg_stats_per_h3_cell_per_month_flipped;
    """
    con.sql(sql)
    logging.info(f"created temp table avg_stats_per_h3_cell_per_month_pivot")


    sql = """
    -------------------------------------------------------------------
    -- Overall stats for country h3 cell map
    --------------------------------------------------------------------
    
    DROP TABLE IF EXISTS h3_country_map;
    CREATE TABLE h3_country_map as (
    SELECT
        a.*
        ,h3_r6.h3_cell_to_boundary_wkt().ST_GeomFromText() as geom
        ,t0_sum_edits_non_corporate + t1_sum_edits_non_corporate as sum_edits_non_corporate
        ,t0_sum_edits_corporate + t1_sum_edits_corporate as sum_edits_corporate
        ,round(
            sum_edits_corporate / (sum_edits_corporate + sum_edits_non_corporate), 3
         ) as share_sum_edits_corporate
        ,t1_avg_monthly_edits_non_corporate - t0_avg_monthly_edits_non_corporate as difference_avg_monthly_edits_non_corporate
        ,t1_avg_monthly_edits_corporate - t0_avg_monthly_edits_corporate as difference_avg_monthly_edits_corporate
        ,CASE
            WHEN (t1_avg_monthly_edits_non_corporate + t0_avg_monthly_edits_non_corporate) = 0 THEN 0
            ELSE
            (t1_avg_monthly_edits_non_corporate - t0_avg_monthly_edits_non_corporate)
            /
            (t1_avg_monthly_edits_non_corporate + t0_avg_monthly_edits_non_corporate)
        END as normalized_difference_avg_monthly_edits_non_corporate
        ,CASE
            WHEN (t1_avg_monthly_edits_corporate + t0_avg_monthly_edits_corporate) = 0 THEN 0
            ELSE 
            (t1_avg_monthly_edits_corporate - t0_avg_monthly_edits_corporate)
            /
            (t1_avg_monthly_edits_corporate + t0_avg_monthly_edits_corporate)
        END as normalized_difference_avg_monthly_edits_corporate
    FROM avg_stats_per_h3_cell_per_month_flipped as a
    WHERE
        -- only select cells with mapping activity in time range 2019-06-01 - 2023-06-01
        t0_sum_edits_non_corporate > 0
        OR t1_sum_edits_non_corporate > 0
        OR t0_sum_edits_corporate > 0
        OR t1_sum_edits_corporate > 0
    ORDER BY share_sum_edits_corporate DESC
    );
    
    DESCRIBE h3_country_map;
    """
    con.sql(sql)
    logging.info(f"created temp table h3_country_map")

    sql = f"""
    COPY(
    SELECT * REPLACE (h3_r6::int64 as h3_r6)
    FROM h3_country_map
    ) TO
    '{output_file}'
    WITH (FORMAT GDAL, DRIVER 'GPKG', LAYER_CREATION_OPTIONS 'WRITE_BBOX=YES');
    """
    con.sql(sql)
    logging.info(f"exported into geopackage: {output_file}")


if __name__ == '__main__':

    sql = f"""
    SET THREADS TO 50;
    SET max_memory TO '50GB';
    
    INSTALL SPATIAL;
    LOAD SPATIAL;
    
    INSTALL h3 FROM community;
    LOAD h3;
    """
    con.sql(sql)
    run_workflow()
