import numpy as np
import pandas as pd
from google.cloud import bigquery
from sklearn.cluster import AgglomerativeClustering
from sklearn.metrics.pairwise import haversine_distances

from constants import NO_FILTER_FOR_THESE_INTEGRATIONS,INTEGRATION_COUNTRY_MODE_MAPPING_DICT



def get_data_from_dwh( project_id,query):
    client = bigquery.Client(project=project_id)

    df = client.query(query).to_dataframe()
    return df

def filter_positions_by_factors(df, mode, integration):
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

def cluster_positions(df):
    coords_rad = np.radians(df[["latitude", "longitude"]].to_numpy())

    # 2) compute pairwise haversine distances in meters
    earth_radius_m = 6371000
    dist_matrix_m = haversine_distances(coords_rad) * earth_radius_m

    # 3) cluster so that max distance inside each cluster is <= 30 m
    clustering = AgglomerativeClustering(
        n_clusters=None,
        metric="precomputed",
        linkage="complete",
        distance_threshold=30
    )

    df["cluster_id"] = clustering.fit_predict(dist_matrix_m)

    df["keep_flag"] = (
           df["usageFactor"] ==
            df.groupby("cluster_id")["usageFactor"].transform("max")
    )
    df = df.sort_values(["cluster_id", "stop_id"])
    return df

def filter_positions(df, mode,integrations):

    results = {}

    for integration in integrations:
        # allowed countries
        allowed_countries = INTEGRATION_COUNTRY_MODE_MAPPING_DICT[mode][integration]

        # only positions for those countries
        positions_by_countries = df[
            df["country_name"].isin(allowed_countries)
        ]

        filtered_by_factor = filter_positions_by_factors( df=positions_by_countries,
                                                    mode=mode,
                                                    integration=integration
                                                )

        filtered_by_clustering = cluster_positions(df=filtered_by_factor)

        filtered_by_clustering["integration"] = integration

        results[integration] = filtered_by_clustering

    return pd.concat(results, ignore_index=True)