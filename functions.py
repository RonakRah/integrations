import numpy as np
import pandas as pd
import logging
import sys
import time
from google.cloud import bigquery
from sklearn.cluster import AgglomerativeClustering
from sklearn.metrics.pairwise import haversine_distances

from constants import NO_FILTER_FOR_THESE_INTEGRATIONS,INTEGRATION_COUNTRY_MODE_MAPPING_DICT
from constants import OUTPUT_PROJECT_ID, OUTPUT_DATASET_ID, OUTPUT_TABLE_NAME

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s - %(message)s"
logger = logging.getLogger(__name__)


def configure_logger(logger_name: str = __name__) -> logging.Logger:
    configured_logger = logging.getLogger(logger_name)
    configured_logger.setLevel(logging.INFO)

    if not configured_logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter(LOG_FORMAT))
        configured_logger.addHandler(handler)

    configured_logger.propagate = False
    return configured_logger


logger = configure_logger(__name__)


def get_bigquery_client(project_id: str, LOCAL: bool = True) -> bigquery.Client:
    logger.info("Creating BigQuery client with local credentials")
    return bigquery.Client(project=project_id)


def get_default_table_schema():
    return [
        bigquery.SchemaField("stopId", "INT64"),
        bigquery.SchemaField("stopName", "STRING"),
        bigquery.SchemaField("positionType", "STRING"),
        bigquery.SchemaField("latitude", "FLOAT64"),
        bigquery.SchemaField("longitude", "FLOAT64"),
        bigquery.SchemaField("countryName", "STRING"),
        bigquery.SchemaField("bookingCountYearly", "INT64"),
        bigquery.SchemaField("searchCountYearly", "INT64"),
        bigquery.SchemaField("usageFactor", "FLOAT64"),
        bigquery.SchemaField("sourcePriority", "INT64"),
        bigquery.SchemaField("clusterId", "INT64"),
        bigquery.SchemaField("keepFlag", "BOOL"),
        bigquery.SchemaField("integration", "STRING"),
        bigquery.SchemaField("updateAt", "TIMESTAMP"),
    ]


def read_table_schema(schema_file: str | None):
    if schema_file:
        raise NotImplementedError("Custom schema_file loading is not implemented in this repo")
    return get_default_table_schema()
def get_data_from_dwh(
    project_id: str,
    query: str,
    job_config: bigquery.QueryJobConfig | None = None,
    progress_label: str | None = None,
    poll_interval_seconds: int = 30,
) -> pd.DataFrame:
    logger.info("Fetching data from BigQuery project=%s with local credentials", project_id)
    client = get_bigquery_client(project_id=project_id)
    query_job = client.query(query, job_config=job_config)
    label = progress_label or query_job.job_id
    logger.info("Started BigQuery job %s for %s", query_job.job_id, label)

    started_at = time.monotonic()
    while not query_job.done():
        elapsed_seconds = int(time.monotonic() - started_at)
        logger.info(
            "Still running BigQuery job %s for %s elapsed_seconds=%s",
            query_job.job_id,
            label,
            elapsed_seconds,
        )
        time.sleep(poll_interval_seconds)

    dataframe = query_job.to_dataframe()
    if {"from_id", "to_id"}.issubset(dataframe.columns):
        route_count = dataframe[["from_id", "to_id"]].drop_duplicates().shape[0]
        logger.info(
            "Fetched %s rows from BigQuery route=%s",
            len(dataframe),
            route_count,
        )
        return dataframe

    logger.info("Fetched %s rows from BigQuery", len(dataframe))
    return dataframe

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

def _cluster_single_country_positions(df):
    if len(df) == 1:
        df = df.copy()
        df["cluster_id"] = 0
        df["keep_flag"] = True
        return df

    df = df.copy()
    coords_rad = np.radians(df[["latitude", "longitude"]].to_numpy())

    earth_radius_m = 6371000
    # print("starting haversine_distances")
    dist_matrix_m = haversine_distances(coords_rad) * earth_radius_m

    # print("starting AgglomerativeClustering")
    clustering = AgglomerativeClustering(
        n_clusters=None,
        metric="precomputed",
        linkage="complete",
        distance_threshold=120
    )

    # print("starting fit_predict")
    df["cluster_id"] = clustering.fit_predict(dist_matrix_m)

    df["keep_flag"] = (
           df["usageFactor"] ==
            df.groupby("cluster_id")["usageFactor"].transform("max")
    )
    df = df.sort_values(["cluster_id", "stop_id"])
    return df


def cluster_positions(df, mode, integration):
    if df.empty:
        raise ValueError("Input DataFrame for clustering is empty")

    allowed_countries = INTEGRATION_COUNTRY_MODE_MAPPING_DICT[mode][integration]
    clustered_country_frames = []

    for country_name in allowed_countries:
        country_df = df[df["country_name"] == country_name].copy()
        if country_df.empty:
            continue

        # print(f"starting clustering for integration:{integration} country:{country_name}")
        clustered_country_df = _cluster_single_country_positions(country_df)
        clustered_country_frames.append(clustered_country_df)

    if not clustered_country_frames:
        raise ValueError(
            f"No rows available for clustering for mode:{mode} integration:{integration}"
        )

    return pd.concat(clustered_country_frames, ignore_index=True).sort_values(
        ["cluster_id", "stop_id"]
    )
def union_positions_and_potential_positions(stations,potentials ):
    df = pd.concat([stations, potentials], ignore_index=True)

    # Sort so priority 1 comes first
    df = df.sort_values(by="source_priority", ascending=True)

    # Drop duplicates keeping the first (which will be priority=1 if exists)
    df = df.drop_duplicates(
        subset=["stop_id", "provider_name", "positionType", "country_name"],
        keep="first"
    )

    # (optional) reset index
    df = df.reset_index(drop=True)
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
        stops_before_factor_filter = positions_by_countries["stop_id"].nunique()

        
        filtered_by_factor = filter_positions_by_factors( df=positions_by_countries,
                                                    mode=mode,
                                                    integration=integration)
        stops_after_factor_filter = filtered_by_factor["stop_id"].nunique()
        dropped_stops_by_factor_filter = (
            stops_before_factor_filter - stops_after_factor_filter
        )
        print(
            f"mode:{mode} integration:{integration} "
            f"factor filter dropped {dropped_stops_by_factor_filter} stops "
            f"from {stops_before_factor_filter} to {stops_after_factor_filter}"
        )


        print(f" statrt clustering for -> mode:{mode} integration: {integration}")
        filtered_by_clustering = cluster_positions(
            df=filtered_by_factor,
            mode=mode,
            integration=integration,
        )

        filtered_by_clustering["integration"] = integration

        results[integration] = filtered_by_clustering

    return pd.concat(results, ignore_index=True)


def find_dropped_provider_per_position(current_positions, new_positions):
    current_keys = current_positions[
        ["stopId", "countryName", "provider_name"]
    ].rename(columns={"stopId": "stop_id","countryName":"country_name"}).drop_duplicates()

    new_keys = new_positions[["stop_id","country_name" , "provider_name"]].copy()
    new_keys = new_keys[
        ["stop_id","country_name" , "provider_name"]
    ].drop_duplicates()

    dropped_by_provider = (
        current_keys.merge(
            new_keys,
            on=["stop_id",  "provider_name"],
            how="left",
            indicator=True,
        )
        .query("_merge == 'left_only'")
        .drop(columns="_merge")
        .reset_index(drop=True)
    )
    dropped_by_provider["dropped_reason"] = "dropped_by_provider"
    return dropped_by_provider


def find_dropped_positions(current_positions, new_positions, dropped_reason):
    new_keys = new_positions[
        ["stop_id", "country_name"]
    ].rename(
        columns={"stop_id": "stopId", "country_name": "countryName"}
    ).drop_duplicates()

    current_positions_with_flag = (
        current_positions.merge(
            new_keys,
            on=["stopId", "countryName"],
            how="left",
            indicator=True,
        )
        .reset_index(drop=True)
    )
    if "dropped_reason" not in current_positions_with_flag.columns:
        current_positions_with_flag["dropped_reason"] = None

    should_set_dropped_reason = (
        (current_positions_with_flag["_merge"] == "left_only")
        & current_positions_with_flag["dropped_reason"].isna()
    )
    current_positions_with_flag["dropped_reason"] = np.where(
        should_set_dropped_reason,
        dropped_reason,
        current_positions_with_flag["dropped_reason"],
    )
    return current_positions_with_flag.drop(columns="_merge")


def add_new_positions_to_comparison(comparison_positions, new_positions):
    new_positions_for_comparison = new_positions.rename(
        columns={
            "stop_id": "stopId",
            "stop_name": "stopName",
            "country_name": "countryName",
            "source_priority": "sourcePriority",
            "cluster_id": "clusterId",
            "keep_flag": "keepFlag",
        }
    )
    new_positions_for_comparison = new_positions_for_comparison.merge(
        comparison_positions[["stopId", "countryName"]].drop_duplicates(),
        on=["stopId", "countryName"],
        how="left",
        indicator=True,
    )
    new_positions_for_comparison = new_positions_for_comparison[
        new_positions_for_comparison["_merge"] == "left_only"
    ].drop(columns="_merge")
    new_positions_for_comparison["dropped_reason"] = "new"
    new_positions_for_comparison = new_positions_for_comparison.reindex(
        columns=comparison_positions.columns
    )

    return pd.concat(
        [comparison_positions, new_positions_for_comparison],
        ignore_index=True,
    )


def filter_positions_and_find_comparison(new_positions, current_positions, integration_providers, mode, integrations):
    results = {}

    for integration in integrations:
        # allowed countries
        allowed_countries = INTEGRATION_COUNTRY_MODE_MAPPING_DICT[mode][integration]
        allowed_providers = integration_providers.loc[
            integration_providers["integration"] == integration, "service_provider"].to_list()

        # only positions for those countries and providers (new)
        positions_by_allowed_countries_and_providers = new_positions[
            (new_positions["country_name"].isin(allowed_countries))
            & (new_positions["provider_name"].isin(allowed_providers))
            ]

        positions_by_countries = positions_by_allowed_countries_and_providers.drop(
            columns=["provider_name"]).drop_duplicates().reset_index(drop=True)

        """--------------------- second check /provider drop/ ----------------------"""
        # 2) dropped by provider
        dropped_positions_by_provider = find_dropped_positions(
            current_positions=current_positions[(current_positions["integration"] == integration)],
            new_positions=positions_by_countries,
            dropped_reason="dropped_by_provider",
        )
        """---------------------------------------------------------------------------"""

        filtered_by_factor = filter_positions_by_factors(df=positions_by_countries,
                                                         mode=mode,
                                                         integration=integration)

        """--------------------- third check /factor drop/ ----------------------"""

        # 3) dropped by factor
        dropped_positions_by_factor = find_dropped_positions(
            current_positions=dropped_positions_by_provider,
            new_positions=filtered_by_factor,
            dropped_reason="dropped_by_factor",
        )
        """---------------------------------------------------------------------"""

        filtered_by_clustering = cluster_positions(
            df=filtered_by_factor,
            mode=mode,
            integration=integration,
        )
        if mode == "train":
            filtered_by_clustering = filtered_by_clustering[
                filtered_by_clustering["keep_flag"].eq(True)
            ].copy()
        else:
            filtered_by_clustering = filtered_by_clustering.copy()

        filtered_by_clustering["integration"] = integration

        # 3) dropped by clustering
        dropped_positions_by_clustering = find_dropped_positions(
            current_positions=dropped_positions_by_factor,
            new_positions=filtered_by_clustering,
            dropped_reason="dropped_by_clustering",
        )

        """--------------------- final check /incoming ----------------------"""

        final_result_from_comparison = add_new_positions_to_comparison(
            comparison_positions=dropped_positions_by_clustering,
            new_positions=filtered_by_clustering,
        )

        results[integration] = final_result_from_comparison

    return pd.concat(results, ignore_index=True)


def write_dataframe_to_bigquery(df, project_id, dataset_id, table_name):
    # print(f"writing {len(df)} rows to {project_id}.{dataset_id}.{table_name}")
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        schema=get_default_table_schema(),
    )
    load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    load_job.result()
    return table_id


def export_main_results_to_dwh(df):
    return write_dataframe_to_bigquery(
        df=df,
        project_id=OUTPUT_PROJECT_ID,
        dataset_id=OUTPUT_DATASET_ID,
        table_name=OUTPUT_TABLE_NAME,
    )

def log_write_mode_action(write_mode: str, table_id: str) -> None:
    if write_mode == "append":
        # logger.info("Appending data to existing table: %s", table_id)
        return
    if write_mode == "overwrite":
        # logger.info("Overwriting table data: %s", table_id)
        return
def get_write_disposition(write_mode: str) -> str:
    # logger.info("Resolving write mode: %s", write_mode)
    write_dispositions = {
        "append": bigquery.WriteDisposition.WRITE_APPEND,
        "overwrite": bigquery.WriteDisposition.WRITE_TRUNCATE,
    }
    if write_mode not in write_dispositions:
        allowed_modes = ", ".join(sorted(write_dispositions))
        raise ValueError(f"Unsupported write_mode '{write_mode}'. Use one of: {allowed_modes}")
    return write_dispositions[write_mode]

def export_dataframe_to_dwh(
    df: pd.DataFrame,
    project_id: str,
    dataset_id: str,
    table_name: str,
    schema_file: str | None = None,
    LOCAL: bool = True,
    write_mode: str = "overwrite",
) -> None:
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    write_disposition = get_write_disposition(write_mode)
    # logger.info("Loading %s rows into %s with write_mode=%s", len(df), table_id, write_mode)
    log_write_mode_action(write_mode=write_mode, table_id=table_id)
    client = get_bigquery_client(project_id=project_id, LOCAL=LOCAL)
    job_config = bigquery.LoadJobConfig(
        schema=read_table_schema(schema_file),
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=write_disposition,
    )

    load_job = client.load_table_from_dataframe(
        dataframe=df,
        destination=table_id,
        job_config=job_config,
    )
    # logger.info("Started dataframe load job %s", load_job.job_id)
    load_job.result()
    print(f"Successfully exported {len(df)} rows to {table_id}")
    # logger.info("Loaded %s rows into %s", load_job.output_rows, table_id)


def export_dataframe_to_google_sheet(df, spreadsheet_id, sheet_name):
    import subprocess

    from google.oauth2.credentials import Credentials
    from gspread.exceptions import APIError
    import gspread

    access_token = subprocess.check_output(
        ["gcloud", "auth", "print-access-token"],
        text=True,
    ).strip()
    credentials = Credentials(token=access_token)
    client = gspread.authorize(credentials)
    try:
        spreadsheet = client.open_by_key(spreadsheet_id)
    except PermissionError as exc:
        raise RuntimeError(
            "Google Sheets write failed because the local Google credential does not "
            "have the required Sheets/Drive scope. Re-run local login with: "
            "`gcloud auth login --enable-gdrive-access --force`"
        ) from exc
    except APIError as exc:
        raise RuntimeError(
            "Google Sheets write failed while opening the spreadsheet. Check that "
            "the credential can access the spreadsheet and has Sheets API scope."
        ) from exc
    worksheet = spreadsheet.worksheet(sheet_name)

    output_df = df.copy().where(pd.notna(df), "")
    values = [output_df.columns.to_list()] + output_df.values.tolist()

    worksheet.clear()
    worksheet.update(values=values, range_name="A1")
    print(f"Successfully exported {len(output_df)} rows to {worksheet.url}")
    return worksheet.url
