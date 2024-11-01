import sys
import logging
import duckdb
import pandas as pd


con = duckdb.connect()

# configure logging
root = logging.getLogger()
root.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)


def extract_contributions(iso_a3_code):
    sql = f"""
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
    logging.info(f"set variable for user id files")

    sql = f"""
        DROP TABLE IF EXISTS ohsome_contributions;
        CREATE TABLE ohsome_contributions AS
        (
        SELECT
            centroid,
            valid_from,
            valid_to,
            osm_type,
            osm_id,
            tags,
            tags_before,
            changeset_tags,
            hashtags,
            country_iso_a3,
            user_id,
            user_name,
            editor,
            contrib_type,
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
            AND list_contains(country_iso_a3, '{iso_a3_code}')
    );
    """
    con.sql(sql)
    logging.info(f"created temp table ohsome_contributions")


def define_primary_features():
    sql = """
        --------------------------------------------
        -- Define primary feature key
        --------------------------------------------
        SET VARIABLE primary_features_tag_keys = [
            'aerialway',
            'aeroway',
            'amenity',
            'barrier',
            'boundary',
            'building',
            'craft',
            'emergency',
            'geological',
            'healthcare',
            'highway',
            'historic',
            'landuse',
            'landuse',
            'leisure',
            'man_made',
            'military',
            'natural',
            'office',
            'place',
            'power',
            'public_transport',
            'railway',
            'route',
            'shop',
            'sport',
            'telecom',
            'tourism',
            'water',
            'waterway'
        ];
    """
    con.sql(sql)
    logging.info(f"defined osm keys for primary features")


def get_detailed_stats(time_range, corporate):

    if corporate:
        corporate_filter = "AND corporation != 'nc'"
        table_name = f"{time_range}_corporate_summary_stats"
    else:
        corporate_filter = "AND corporation = 'nc'"
        table_name = f"{time_range}_non_corporate_summary_stats"

    sql = f"""
        --------------------------------------------------------------
        -- Detailed stats for different categories
        ---------------------------------------------------------------
        DROP TABLE IF EXISTS ohsome_contributions_selection ;
        CREATE TABLE ohsome_contributions_selection AS
        (
        SELECT
          corporation,
          user_id,
          contrib_type,
          hashtags,
          tags,
          tags_before,
          valid_from,
          time_range
        FROM ohsome_contributions
        WHERE 1=1
            AND valid_from >= '2019-06-01'
            AND valid_from < '2023-06-01'
            {corporate_filter}
            AND time_range = '{time_range}'
        );
        
        SET VARIABLE total_edits = (SELECT COUNT(*)  FROM ohsome_contributions_selection);
        SET VARIABLE total_contributors = (SELECT COUNT( DISTINCT user_id ) FROM ohsome_contributions_selection);
        
        
        DROP TABLE IF EXISTS {table_name};
        CREATE TABLE {table_name} AS
        SELECT
            time_range,
            corporation,
            contrib_type,
            CASE
                WHEN len(list_intersect( map_keys(tags), getvariable('primary_features_tag_keys'))) > 0
                THEN list_intersect( map_keys(tags), getvariable('primary_features_tag_keys') )
                WHEN contrib_type = 'DELETED' AND len(list_intersect( map_keys(tags_before), getvariable('primary_features_tag_keys'))) > 0
                THEN list_intersect( map_keys(tags_before), getvariable('primary_features_tag_keys') )
                ELSE ['other']
            END as primary_osm_tag_keys,
            COUNT(*) AS edits,
            round( edits / getvariable('total_edits') , 3) edits_share,
            round( SUM(edits / getvariable('total_edits')) OVER ( ORDER BY edits DESC), 3) as edits_cumulative_share,
            COUNT(DISTINCT user_id) as contributors,
            round( COUNT(DISTINCT user_id) / getvariable('total_contributors'), 3) as contributors_share,
        FROM ohsome_contributions_selection
        GROUP BY corporation, time_range, contrib_type, primary_osm_tag_keys
        ORDER BY edits DESC
        LIMIT 20
        ;
        
        SELECT * FROM {table_name};
    """
    df = con.sql(sql).df()
    logging.info(f'created table {table_name}')
    return df


def run_workflow(iso_a3_code):

    extract_contributions(iso_a3_code)
    define_primary_features()

    t0_corporate = get_detailed_stats('t0', corporate=True)
    t0_non_corporate = get_detailed_stats('t0', corporate=False)
    t1_corporate = get_detailed_stats('t1', corporate=True)
    t1_non_corporate= get_detailed_stats('t1', corporate=False)

    print(t0_corporate)
    print(t0_non_corporate)
    print(t1_corporate)
    print(t1_non_corporate)


    df_merged = pd.concat([
        t0_corporate,
        t1_corporate,
        t0_non_corporate,
        t1_non_corporate
    ])

    output_file = f'./results/summary_stats_{iso_a3_code}.csv'
    df_merged.to_csv(output_file)
    logging.info(f'exported csv file {output_file}')


if __name__ == '__main__':
    input_file = "/data/processing/corporate-stats/world_boundaries_with_stats.gpkg"

    sql = f"""
        SET THREADS TO 50;
        SET max_memory TO '150GB';

        INSTALL SPATIAL;
        LOAD SPATIAL;

        INSTALL h3 FROM community;
        LOAD h3;

        SELECT
            iso_a3_eh
        FROM st_read('{input_file}')
        WHERE 1=1
            AND share_sum_edits_corporate > 0.10
            AND sum_edits_corporate > 100_000;
        """
    df = con.sql(sql).df()
    country_list = df["ISO_A3_EH"].to_list()


    for iso_a3_code in country_list:
        run_workflow(iso_a3_code)