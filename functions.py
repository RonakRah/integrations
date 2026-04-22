import numpy as np
import pandas as pd
from google.cloud import bigquery
from sklearn.cluster import AgglomerativeClustering
from sklearn.metrics.pairwise import haversine_distances

from constants import NO_FILTER_FOR_THESE_INTEGRATIONS,INTEGRATION_COUNTRY_MODE_MAPPING_DICT



def get_data_from_dwh( project_id,query):
    print(fr"loading data")
    client = bigquery.Client(project=project_id)

    df = client.query(query).to_dataframe()
    return df

def filter_positions_by_factors(df, mode, integration):
    print(f"mode:{mode} integration: {integration} is in filtering by factors process...")
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
    if df.empty:
        raise ValueError("Input DataFrame for clustering is empty")

    if len(df) == 1:
        df = df.copy()
        df["cluster_id"] = 0
        df["keep_flag"] = True
        return df

    coords_rad = np.radians(df[["latitude", "longitude"]].to_numpy())

    # 2) compute pairwise haversine distances in meters
    earth_radius_m = 6371000
    dist_matrix_m = haversine_distances(coords_rad) * earth_radius_m

    # 3) cluster so that max distance inside each cluster is <= 150 m
    clustering = AgglomerativeClustering(
        n_clusters=None,
        metric="precomputed",
        linkage="complete",
        distance_threshold=120
    )

    df["cluster_id"] = clustering.fit_predict(dist_matrix_m)

    df["keep_flag"] = (
           df["usageFactor"] ==
            df.groupby("cluster_id")["usageFactor"].transform("max")
    )
    df = df.sort_values(["cluster_id", "stop_id"])
    return df

def filter_positions(df,integration_providers, mode,integrations):

    results = {}


    for integration in integrations:

        # allowed countries
        allowed_countries = INTEGRATION_COUNTRY_MODE_MAPPING_DICT[mode][integration]
        allowed_providers = integration_providers.loc[integration_providers["integration"]==integration,"service_provider"].to_list()
        # only positions for those countries
        positions_by_allowed_countries_and_providers = df[
            df["country_name"].isin(allowed_countries)
            & df["provider_name"].isin(allowed_providers)
        ]
        positions_by_countries = positions_by_allowed_countries_and_providers.drop(columns=["provider_name"]).drop_duplicates().reset_index(drop=True)
        filtered_by_factor = filter_positions_by_factors( df=positions_by_countries,
                                                    mode=mode,
                                                    integration=integration)


        print(f" statrt clustering for -> mode:{mode} integration: {integration}")
        filtered_by_clustering = cluster_positions(df=filtered_by_factor)

        filtered_by_clustering["integration"] = integration

        results[integration] = filtered_by_clustering

    return pd.concat(results, ignore_index=True)


def write_dataframe_to_bigquery(df, project_id, dataset_id, table_name):
    print(f"writing {len(df)} rows to {project_id}.{dataset_id}.{table_name}")
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        schema=[
            bigquery.SchemaField("stop_id", "INT64"),
            bigquery.SchemaField("stop_name", "STRING"),
            bigquery.SchemaField("positionType", "STRING"),
            bigquery.SchemaField("latitude", "FLOAT64"),
            bigquery.SchemaField("longitude", "FLOAT64"),
            bigquery.SchemaField("country_name", "STRING"),
            bigquery.SchemaField("bookingCountYearly", "INT64"),
            bigquery.SchemaField("searchCountYearly", "INT64"),
            bigquery.SchemaField("usageFactor", "FLOAT64"),
            bigquery.SchemaField("source_priority", "INT64"),
            bigquery.SchemaField("cluster_id", "INT64"),
            bigquery.SchemaField("keep_flag", "BOOL"),
            bigquery.SchemaField("integration", "STRING"),
            bigquery.SchemaField("mode", "STRING"),
        ],
    )
    load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    load_job.result()
    return table_id
