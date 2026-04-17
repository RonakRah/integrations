
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
    "eu_omio": [
        {"country": "germany", "travel_mode": "train"},
        {"country": "italy", "travel_mode": "train"},
        {"country": "france", "travel_mode": "train"},
        {"country": "united kingdom", "travel_mode": "train"},
        {"country": "spain", "travel_mode": "train"},
        {"country": "austria", "travel_mode": "train"},
        {"country": "sweden", "travel_mode": "train"},
        {"country": "switzerland", "travel_mode": "train"},
        {"country": "czechia", "travel_mode": "train"},
        {"country": "poland", "travel_mode": "train"},
        {"country": "belgium", "travel_mode": "train"},
        {"country": "netherlands", "travel_mode": "train"},
        {"country": "hungary", "travel_mode": "train"},
        {"country": "denmark", "travel_mode": "train"},
        {"country": "slovakia", "travel_mode": "train"},
        {"country": "norway", "travel_mode": "train"},
        {"country": "finland", "travel_mode": "train"},
        {"country": "luxembourg", "travel_mode": "train"},
        {"country": "liechtenstein", "travel_mode": "train"},
    ],

    "eu_omio_bus": [
        {"country": "spain", "travel_mode": "bus"},
        {"country": "italy", "travel_mode": "bus"},
        {"country": "united kingdom", "travel_mode": "bus"},
        {"country": "france", "travel_mode": "bus"},
        {"country": "poland", "travel_mode": "bus"},
        {"country": "germany", "travel_mode": "bus"},
        {"country": "portugal", "travel_mode": "bus"},
        {"country": "croatia", "travel_mode": "bus"},
        {"country": "norway", "travel_mode": "bus"},
        {"country": "netherlands", "travel_mode": "bus"},
        {"country": "sweden", "travel_mode": "bus"},
        {"country": "czechia", "travel_mode": "bus"},
        {"country": "belgium", "travel_mode": "bus"},
        {"country": "austria", "travel_mode": "bus"},
        {"country": "slovakia", "travel_mode": "bus"},
        {"country": "denmark", "travel_mode": "bus"},
        {"country": "switzerland", "travel_mode": "bus"},
        {"country": "slovenia", "travel_mode": "bus"},
        {"country": "hungary", "travel_mode": "bus"},
        {"country": "luxembourg", "travel_mode": "bus"},
    ],

    "jp_omio_bus": [{"country": "japan", "travel_mode": "bus"}],
    "jp_omio_train": [{"country": "japan", "travel_mode": "train"}],

    "uk_omio_nationalrail": [{"country": "united kingdom", "travel_mode": "train"}],
    "uk_lner": [{"country": "united kingdom", "travel_mode": "train"}],

    "pt_omio_comboios": [{"country": "portugal", "travel_mode": "train"}],
    "br_omio_bus": [{"country": "brazil", "travel_mode": "bus"}],
    "eu_omio_deutschebahn": [{"country": "germany", "travel_mode": "train"}],
    "us_omio": [{"country": "usa", "travel_mode": "train"}],
}