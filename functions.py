from google.cloud import bigquery
from constants import NO_FILTER_FOR_THESE_INTEGRATIONS,INTEGRATION_COUNTRY_MODE_MAPPING_DICT
from google.cloud import bigquery

def get_data_from_dwh( project_id,query):
    client = bigquery.Client(project=project_id)

    df = client.query(query).to_dataframe()
    return df

def filter_positions(df, mode, integration):
    if integration in NO_FILTER_FOR_THESE_INTEGRATIONS:
        return df

    if mode == "train":
        return df[
            (
                    (df["country_name"].str.lower() == "switzerland") &
                    (df["bookingCountYearly"] > 0)
            )
            |
            (
                    (df["country_name"].str.lower() != "switzerland") &
                    (df["usageFactor"] > 0)
            )
            ]
    else:
        return df[df["usageFactor"] > 0]

import pandas as pd

def process_positions_by_mode(df, mode):

    #  integrations by mode
    integrations_for_travel_mode = list(
        INTEGRATION_COUNTRY_MODE_MAPPING_DICT[mode].keys()
    )
    # filter by mode (train / bus)
    positions = df[
        df["positionType"].str.startswith(mode, na=False)
    ]

    results = {}



    for integration in integrations_for_travel_mode:
        countries = INTEGRATION_COUNTRY_MODE_MAPPING_DICT[mode][integration]

        positions_by_countries = positions[
            positions["country_name"].isin(countries)
        ]

        filtered_df = filter_positions(
            df=positions_by_countries,
            mode=mode,
            integration=integration
        )

        # keep integration info

        filtered_df["integration"] = integration

        results[integration] = filtered_df

    return pd.concat(results, ignore_index=True)