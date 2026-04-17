
TORKIN_POSITIONS_PROJECT_ID = "centered-radius-89610"
TORKIN_POSITIONS_DATASET_ID='dwh_raw'
TORKIN_POSITIONS_TABLE_NAME ='torkin_position_v1'

TORKIN_POSITIONS_QUERY = f"""
SELECT
    t1.id AS stop_id,
    t1.defaultName AS stop_name,
    ROUND(t1.latitude, 6) AS latitude,
    ROUND(t1.longitude, 6) AS longitude,
    t1.usage.bookingCountYearly AS bookingCountYearly,
    t1.usage.searchCountYearly AS searchCountYearly,
    t1.usage.usageFactor AS usageFactor,
    rt.element.providerId AS provider_id,
    providers.provider_name AS provider_name,
    t1.countryId,
    LOWER(t1.positionType) AS positionType,
    2 AS source_priority
  FROM `centered-radius-89610.dwh_raw.torkin_position_v1` AS t1,
    UNNEST(relatedTerminals.list) AS rt
  LEFT JOIN centered-radius-89610.dwh_core.providers AS providers ON rt.element.providerid = providers.provider_id

   WHERE TRUE
         AND t1.deleted = FALSE
         AND positionType IN ('busStation','trainStation')
    
    """
TORKIN_COUNTRY_QUERY = f"""
SELECT id,
         LOWER(name) AS country_name,
         continentname
  FROM`centered-radius-89610.dwh_raw.torkin_country_v1`
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