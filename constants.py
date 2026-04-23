
TORKIN_POSITIONS_PROJECT_ID = "centered-radius-89610"
INTEGRATIONS_AND_THEIR_PROVIDERS_PROJECT_ID = "centered-radius-89610"
TORKIN_POSITIONS_DATASET_ID='dwh_raw'
TORKIN_POSITIONS_TABLE_NAME ='torkin_position_v1'
OUTPUT_PROJECT_ID = "centered-radius-89610"
OUTPUT_DATASET_ID = "b2b"
OUTPUT_TABLE_NAME = "gtw_positions"
MANUAL_OUTPUT_FILE = "gtw_integrations_and_positions.xlsx"
FINAL_OUTPUT_COLUMNS = [
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

TORKIN_POSITIONS_QUERY = f"""
WITH torkin_positions AS (
SELECT
    t1.id AS stop_id,
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
       LOWER(provider_name) AS provider_name,
FROM centered-radius-89610.dwh_core.providers
)
,google_provider_restriction AS (
SELECT

    LOWER(provider) AS provider
    FROM `centered-radius-89610.dwh_raw.cms_partner_restrictions`,
    UNNEST(JSON_VALUE_ARRAY(providers_list)) AS provider
    WHERE partner_id = 'google'
)

,torkin_positions_with_allowed_providers AS (
     SELECT tp.*,
            pro.provider_name
     FROM torkin_positions AS tp
     LEFT JOIN providers AS pro ON tp.provider_id = pro.provider_id
     WHERE LOWER(pro.provider_name) IN (SELECT provider FROM google_provider_restriction)
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
FROM  torkin_positions_with_allowed_providers AS ts
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
        "eu_omio_deutschebahn": ["germany"],
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

NO_FILTER_FOR_THESE_INTEGRATIONS=['jp_omio_bus','jp_omio_train']
