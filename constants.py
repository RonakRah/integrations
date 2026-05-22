
TORKIN_POSITIONS_PROJECT_ID = "centered-radius-89610"
INTEGRATIONS_AND_THEIR_PROVIDERS_PROJECT_ID = "centered-radius-89610"
TORKIN_POSITIONS_DATASET_ID='dwh_raw'
TORKIN_POSITIONS_TABLE_NAME ='torkin_position'
OUTPUT_PROJECT_ID = "centered-radius-89610"
OUTPUT_DATASET_ID = "b2b_gtw"
OUTPUT_TABLE_NAME = "gtw_positions"
MANUAL_OUTPUT_FILE = "gtw_integrations_and_positions.xlsx"
COMPARISON_OUTPUT_TABLE_NAME = "gtw_positions_comparison_old_new"
COMPARISON_OUTPUT_SCHEMA_FILE = "gtw_positions_comparison_schema.yml"
COMPARISON_OUTPUT_COLUMNS = [
    "stopId",
    "stopName",
    "countryName",
    "integration",
    "providerName",
    "dropped_reason",
    "updateAt",
]
FINAL_OUTPUT_SOURCE_COLUMNS = [
    "stop_id",
    "stop_name",
    "positionType",
    "latitude",
    "longitude",
    "country_name",
    "bookingCountYearly",
    "searchCountYearly",
    "usageFactor",
    "source_priority",
    "cluster_id",
    "keep_flag",
    "integration",
]
FINAL_OUTPUT_RENAME_MAP = {
    "stop_id": "stopId",
    "stop_name": "stopName",
    "country_name": "countryName",
    "source_priority": "sourcePriority",
    "cluster_id": "clusterId",
    "keep_flag": "keepFlag",
}
FINAL_OUTPUT_COLUMNS = [
    "stopId",
    "stopName",
    "positionType",
    "latitude",
    "longitude",
    "countryName",
    "bookingCountYearly",
    "searchCountYearly",
    "usageFactor",
    "sourcePriority",
    "clusterId",
    "keepFlag",
    "integration",
    "updateAt",
]

PROVIDER_RESTRICTION_QUERY = """
SELECT DISTINCT
    LOWER(service_provider) AS provider
FROM `centered-radius-89610.b2b_gtw.gtw_integrations_with_allowed_providers`
"""

TORKIN_POSITIONS_QUERY = f"""
WITH torkin_positions AS (
SELECT
    CAST(t1.id AS INT64) AS stop_id,
    t1.defaultName AS stop_name,
    ROUND(t1.latitude, 6) AS latitude,
    ROUND(t1.longitude, 6) AS longitude,
    t1.usage.bookingCountYearly AS bookingCountYearly,
    t1.usage.searchCountYearly AS searchCountYearly,
    t1.usage.usageFactor AS usageFactor,
    t1.countryId,
    LOWER(t1.positionType) AS positionType,
    rt.element.providerId AS provider_id,
    2 AS source_priority
  FROM `centered-radius-89610.dwh_raw.torkin_position_v1` AS t1,
  UNNEST(relatedTerminals.list) AS rt
  WHERE TRUE
         AND t1.deleted = FALSE
         AND positionType IN ('busStation','trainStation')

   GROUP BY ALL
   )
   , torkin_countries AS (
  SELECT id,
         LOWER(name) AS name,
         continentname
  FROM`centered-radius-89610.dwh_raw.torkin_country_v1`

)
,providers AS (
SELECT DISTINCT
       provider_id,
       LOWER(provider_name) AS provider_name
FROM `centered-radius-89610.dwh_core.providers`
)
,torkin_positions_with_providers AS (
     SELECT tp.*,
            pro.provider_name
     FROM torkin_positions AS tp
     LEFT JOIN providers AS pro ON tp.provider_id = pro.provider_id
)
,join_positions_with_countries AS (
SELECT ts.stop_id,
       ts.stop_name,
       ts.provider_name,
       ts.positionType,
       ts.latitude,
       ts.longitude,
       tc.name AS country_name,
       ts.bookingCountYearly,
       ts.searchCountYearly,
       ts.usageFactor,
       ts.source_priority
FROM  torkin_positions_with_providers AS ts
LEFT JOIN torkin_countries AS tc ON ts.countryId = tc.id
)
SELECT *
FROM join_positions_with_countries
GROUP BY ALL
    """
INTEGRATIONS_AND_THEIR_PROVIDERS_QUERY = f"""
 SELECT LOWER(integration) AS integration,
        LOWER(service_provider) AS service_provider,
 FROM `centered-radius-89610.b2b_gtw.gtw_integrations_with_allowed_providers`

"""
QUERY_FOR_POTENTIAL_STOPS = f"""

WITH  potential_stops AS (
  SELECT
    node_id  AS stop_id,
    node_name AS stop_name,
    ROUND(node_lat, 6) AS latitude,
    ROUND(node_lng, 6) AS longitude,
    CASE WHEN LOWER(input_type) LIKE '%bus%' THEN 'busstation'
         WHEN LOWER(input_type) LIKE '%train%' THEN 'trainstation'
    END AS positionType
  FROM `centered-radius-89610.b2b_gtw.gtw_potential_stops`
        
)
,potential_stations_joined_with_torkin AS (
SELECT
  CAST(potential.stop_id AS INT64) AS stop_id,
  potential.stop_name,
  LOWER(p.provider_name) AS provider_name,
  potential.positionType,
  potential.latitude,
  potential.longitude,
  LOWER(tc.name) AS country_name,
  1 AS bookingCountYearly,
  1 AS searchCountYearly,
  1 AS usageFactor,
  1 AS source_priority
FROM potential_stops AS potential
LEFT JOIN `centered-radius-89610.dwh_raw.torkin_position_v1` AS tp
  ON CAST(potential.stop_id AS STRING)= tp.id
 AND tp.deleted = FALSE
LEFT JOIN UNNEST(tp.relatedTerminals.list) AS rt
LEFT JOIN `centered-radius-89610.dwh_raw.torkin_country_v1` AS tc
  ON tp.countryid = tc.id
LEFT JOIN  `centered-radius-89610.dwh_core.providers` as p ON rt.element.providerId  = p.provider_id
)
SELECT * FROM potential_stations_joined_with_torkin
"""
QUERY_FOR_CURRENT_STOPS = f"""
WITH current_stops AS (
    SELECT
        CASE
            WHEN integration_id = 'jp_omio' THEN 'jp_omio_train'
            ELSE integration_id
        END AS integration,
            SAFE_CAST(
        REGEXP_EXTRACT(TRIM(CAST(stop_id AS STRING)), r'[0-9]+')
        AS INT64
    ) AS stop_id,
        stop_name
    FROM `centered-radius-89610.b2b_gtw.tac_ft_transport_stop_mapping`
)
SELECT
    current_stops.integration,
    current_stops.stop_id,
    current_stops.stop_name,
    LOWER(torkin_countries.name) AS country_name,
    LOWER(providers.provider_name) AS provider_name
FROM current_stops
LEFT JOIN `centered-radius-89610.dwh_raw.torkin_position_v1` AS torkin_positions
    ON CAST(current_stops.stop_id AS STRING) = torkin_positions.id
    AND torkin_positions.deleted = FALSE
LEFT JOIN UNNEST(torkin_positions.relatedTerminals.list) AS related_terminal
LEFT JOIN `centered-radius-89610.dwh_core.providers` AS providers
    ON related_terminal.element.providerId = providers.provider_id
LEFT JOIN `centered-radius-89610.dwh_raw.torkin_country_v1` AS torkin_countries
    ON torkin_positions.countryid = torkin_countries.id
WHERE current_stops.stop_id IS NOT NULL
"""
QUERY_PROCESSED_GTW_POSITIONS = f"""
WITH processed_gtw_positions AS (
    SELECT *
    FROM centered-radius-89610.b2b_gtw.gtw_positions
    WHERE (
        positionType = 'trainstation'
        AND keepFlag = TRUE
    )
    OR positionType = 'busstation'
)
SELECT * EXCEPT(updateAt)
FROM processed_gtw_positions
# SELECT
#     processed_gtw_positions.* EXCEPT(updateAt),
#     LOWER(providers.provider_name) AS provider_name
# FROM processed_gtw_positions
# LEFT JOIN `centered-radius-89610.dwh_raw.torkin_position_v1` AS torkin_positions
#     ON CAST(processed_gtw_positions.stopId AS STRING) = torkin_positions.id
#     AND torkin_positions.deleted = FALSE
# LEFT JOIN UNNEST(torkin_positions.relatedTerminals.list) AS related_terminal
# LEFT JOIN `centered-radius-89610.dwh_core.providers` AS providers
#     ON related_terminal.element.providerId = providers.provider_id
"""
INTEGRATION_COUNTRY_MODE_MAPPING_DICT = {
    "train": {
        "eu_omio": [
            "germany","italy","france","united kingdom","spain","austria",
            "sweden","switzerland","czechia","poland","belgium","netherlands",
            "hungary","denmark","slovakia","norway","finland","luxembourg",
            "liechtenstein"
        ],
        "jp_omio_train": ["japan"],
        "uk_omio_nationalrail": ["united kingdom"],
        "uk_lner": ["united kingdom"],
        "pt_omio_comboios": ["portugal"],

        "eu_omio_deutschebahn": ["germany","italy","france","united kingdom","spain","austria",
            "sweden","switzerland","czechia","poland","belgium","netherlands",
            "hungary","denmark","slovakia","norway","finland","luxembourg",
            "liechtenstein"],
        "us_omio": ["usa"],
    },

    "bus": {
        "eu_omio_bus": [
            "spain","italy","united kingdom","france","poland","germany",
            "portugal","croatia","norway","netherlands","sweden","czechia",
            "belgium","austria","slovakia","denmark","switzerland","slovenia",
            "hungary","luxembourg"
        ],
        "jp_omio_bus": ["japan"],
        "br_omio_bus": ["brazil"],
    }
}

NO_FILTER_FOR_THESE_INTEGRATIONS=['jp_omio_bus','jp_omio_train','br_omio_bus']
