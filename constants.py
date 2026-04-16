
TORKIN_POSITIONS_PROJECT_ID = "centered-radius-89610"
TORKIN_POSITIONS_DATASET_ID='dwh_raw'
TORKIN_POSITIONS_TABLE_NAME ='torkin_position_v1'

TORKIN_POSITIONS_QUERY = f"""
        SELECT 
             id,
            defaultName,
            latitude,
            longitude,
            usage.bookingCountYearly,
            usage.searchCountYearly,
            usage.usageFactor,
            rt.element.providerId,
            rt.element.stationName
            
   FROM `centered-radius-89610.dwh_raw.torkin_position_v1` AS t1,
    UNNEST(relatedTerminals.list) AS rt
    WHERE t1.deleted = FALSE
    AND t1.positionType = 'trainStation'
 
    LIMIT 10
    """