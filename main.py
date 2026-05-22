import pandas as pd
from datetime import datetime, UTC

from functions import (
    export_dataframe_to_dwh,union_positions_and_potential_positions,
    filter_positions,
    get_data_from_dwh,find_dropped_positions
)
from constants import FINAL_OUTPUT_COLUMNS, FINAL_OUTPUT_RENAME_MAP, FINAL_OUTPUT_SOURCE_COLUMNS, MANUAL_OUTPUT_FILE,QUERY_FOR_POTENTIAL_STOPS
from constants import INTEGRATION_COUNTRY_MODE_MAPPING_DICT,QUERY_PROCESSED_GTW_POSITIONS
from constants import (
    INTEGRATIONS_AND_THEIR_PROVIDERS_PROJECT_ID,
    INTEGRATIONS_AND_THEIR_PROVIDERS_QUERY,TORKIN_POSITIONS_QUERY,QUERY_FOR_CURRENT_STOPS
)
from constants import TORKIN_POSITIONS_PROJECT_ID
from constants import (
    OUTPUT_PROJECT_ID, OUTPUT_DATASET_ID, OUTPUT_TABLE_NAME,
    COMPARISON_OUTPUT_COLUMNS, COMPARISON_OUTPUT_SCHEMA_FILE,
    COMPARISON_OUTPUT_TABLE_NAME,
)

def main(MANUAL_RUN=True,TASK_TYPE='gtw_positions'):
    print(F"------------------- |starting the job for {TASK_TYPE} |------------------")
    # data loading

    torkin_positions_df = get_data_from_dwh(
        project_id=TORKIN_POSITIONS_PROJECT_ID,
        query=TORKIN_POSITIONS_QUERY,
        progress_label="torkin_positions",
    )
    potential_stops = get_data_from_dwh(
        project_id=OUTPUT_PROJECT_ID,
        query=QUERY_FOR_POTENTIAL_STOPS,
        progress_label="potential_stops",
    )


    all_stations = union_positions_and_potential_positions(stations=torkin_positions_df,potentials=potential_stops)

    current_positions = get_data_from_dwh(project_id=OUTPUT_PROJECT_ID,
                                          query=QUERY_FOR_CURRENT_STOPS,
                                          progress_label="current_positions_tac")

    # ------------ first check if the old position is deleted in torkin
    current_positions_after_deletion_check = find_dropped_positions(
        current_positions=current_positions,
        new_positions=all_stations,
        dropped_reason="deleted_from_torkin",
    )


    integrations_and_providers_df = get_data_from_dwh(project_id=INTEGRATIONS_AND_THEIR_PROVIDERS_PROJECT_ID,
                                                      query=INTEGRATIONS_AND_THEIR_PROVIDERS_QUERY,

                                                      progress_label="integrations_and_providers")
    travel_modes = list(INTEGRATION_COUNTRY_MODE_MAPPING_DICT.keys())
    all_modes_results = []
    all_modes_comparison_results = []

    for mode in travel_modes:

        positions_by_mode = all_stations[
            all_stations["positionType"].str.startswith(mode, na=False)
        ]
        integrations_for_travel_mode = list(
            INTEGRATION_COUNTRY_MODE_MAPPING_DICT[mode].keys()
        )
        filtered_positions, comparison_result = filter_positions(
            new_processed=positions_by_mode,
            current_torkin_positions=current_positions_after_deletion_check,
            integration_providers= integrations_and_providers_df,
            mode=mode,
            integrations=integrations_for_travel_mode,
        )
        filtered_positions["mode"] = mode
        comparison_result["mode"] = mode
        all_modes_results.append(filtered_positions)
        all_modes_comparison_results.append(comparison_result)

    if not all_modes_results:
        raise ValueError("No results found: all_modes_results is empty")

    final_df = pd.concat(all_modes_results, ignore_index=True)
    comparison_df = pd.concat(all_modes_comparison_results, ignore_index=True)
    final_df = final_df.reindex(columns=FINAL_OUTPUT_SOURCE_COLUMNS)
    final_df = final_df.rename(columns=FINAL_OUTPUT_RENAME_MAP)
    final_df["updateAt"] = datetime.now(UTC)
    final_df = final_df.reindex(columns=FINAL_OUTPUT_COLUMNS)

    comparison_df = comparison_df[
        comparison_df["dropped_reason"].notna()
        & (comparison_df["dropped_reason"].astype(str).str.strip() != "")
        & (comparison_df["dropped_reason"].astype(str).str.lower() != "none")
    ]
    comparison_df = comparison_df.rename(
        columns={
            "stop_name": "stopName",
            "country_name": "countryName",
            "provider_name": "providerName",
        }
    )
    comparison_df["updateAt"] = datetime.now(UTC).strftime("%Y-%m-%d %H:%M")
    comparison_df = comparison_df.reindex(columns=COMPARISON_OUTPUT_COLUMNS)

    export_dataframe_to_dwh(
        df=final_df,
        project_id=OUTPUT_PROJECT_ID,
        dataset_id=OUTPUT_DATASET_ID,
        table_name=OUTPUT_TABLE_NAME,
        schema_file=None,
        LOCAL=MANUAL_RUN,
        write_mode="overwrite",
    )
    export_dataframe_to_dwh(
        df=comparison_df,
        project_id=OUTPUT_PROJECT_ID,
        dataset_id=OUTPUT_DATASET_ID,
        table_name=COMPARISON_OUTPUT_TABLE_NAME,
        schema_file=COMPARISON_OUTPUT_SCHEMA_FILE,
        LOCAL=MANUAL_RUN,
        write_mode="overwrite",
    )
    return final_df, comparison_df


if __name__ == "__main__":
    main(MANUAL_RUN=True)
