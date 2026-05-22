import pandas as pd
from datetime import datetime, UTC

from functions import (
    export_dataframe_to_dwh, union_positions_and_potential_positions,
    filter_positions,
    get_data_from_dwh, filter_positions_and_find_comparison, find_dropped_positions,
)
from constants import FINAL_OUTPUT_COLUMNS, FINAL_OUTPUT_RENAME_MAP, FINAL_OUTPUT_SOURCE_COLUMNS, MANUAL_OUTPUT_FILE, \
    QUERY_FOR_POTENTIAL_STOPS
from constants import INTEGRATION_COUNTRY_MODE_MAPPING_DICT, QUERY_PROCESSED_GTW_POSITIONS
from constants import (
    INTEGRATIONS_AND_THEIR_PROVIDERS_PROJECT_ID,
    INTEGRATIONS_AND_THEIR_PROVIDERS_QUERY, TORKIN_POSITIONS_QUERY
)
from constants import TORKIN_POSITIONS_PROJECT_ID
from constants import (
    OUTPUT_PROJECT_ID, OUTPUT_DATASET_ID, OUTPUT_TABLE_NAME,
    COMPARISON_OUTPUT_COLUMNS, COMPARISON_OUTPUT_SCHEMA_FILE,
    COMPARISON_OUTPUT_TABLE_NAME, QUERY_FOR_CURRENT_STOPS
)


def run_old_new_positions_comparison(MANUAL_RUN=True, TASK_TYPE='position_comparison'):
    # data loading
    print(F"------------------- |starting the job for {TASK_TYPE} |------------------")

    # torkin_positions_df = get_data_from_dwh(
    #     project_id=TORKIN_POSITIONS_PROJECT_ID,
    #     query=TORKIN_POSITIONS_QUERY,
    #     progress_label="torkin_positions",
    # )
    # potential_stops = get_data_from_dwh(
    #     project_id=OUTPUT_PROJECT_ID,
    #     query=QUERY_FOR_POTENTIAL_STOPS,
    #     progress_label="potential_stops",
    # )

    current_positions = get_data_from_dwh(project_id=OUTPUT_PROJECT_ID,
                                          query=QUERY_FOR_CURRENT_STOPS,
                                          progress_label="current_positions_tac")

    processed_gtw_positions = get_data_from_dwh(project_id=OUTPUT_PROJECT_ID, query=QUERY_PROCESSED_GTW_POSITIONS,
                                                progress_label="processed_gtw_positions")

    integrations_and_providers_df = get_data_from_dwh(project_id=INTEGRATIONS_AND_THEIR_PROVIDERS_PROJECT_ID,
                                                      query=INTEGRATIONS_AND_THEIR_PROVIDERS_QUERY,

                                                      progress_label="integrations_and_providers")

    # --------------------- join data
    # all_stations = union_positions_and_potential_positions(stations=torkin_positions_df, potentials=potential_stops)

    #  first check - if the old position is deleted in torkin
    current_positions_after_deletion_check = find_dropped_positions(
        current_positions=current_positions,
        new_positions=processed_gtw_positions.rename(
            columns={"stop_id": "stopId", "country_name": "countryName"}
        ),
        dropped_reason="torkin_change",
    )

    # --------------------- loop session
    travel_modes = list(INTEGRATION_COUNTRY_MODE_MAPPING_DICT.keys())
    all_modes_results = []

    for mode in travel_modes:
        positions_by_mode = processed_gtw_positions[
            processed_gtw_positions["positionType"].str.startswith(mode, na=False)
        ]
        integrations_for_travel_mode = list(
            INTEGRATION_COUNTRY_MODE_MAPPING_DICT[mode].keys()
        )

        comparison = filter_positions_and_find_comparison(
            new_positions=positions_by_mode,
            current_positions=current_positions_after_deletion_check,
            integration_providers=integrations_and_providers_df,
            mode=mode,
            integrations=integrations_for_travel_mode,
        )

        comparison["mode"] = mode
        all_modes_results.append(comparison)

    if not all_modes_results:
        raise ValueError("No comparison results found: all_modes_results is empty")

    final_comparison = pd.concat(all_modes_results, ignore_index=True)
    final_comparison = final_comparison[
        final_comparison["dropped_reason"].notna()
        & (final_comparison["dropped_reason"].astype(str).str.strip() != "")
        & (final_comparison["dropped_reason"].astype(str).str.lower() != "none")
        ]
    final_comparison = final_comparison.rename(
        columns={
            "stop_name": "stopName",
            "country_name": "countryName",
            "provider_name": "providerName",
        }
    )
    final_comparison["updateAt"] = datetime.now(UTC).strftime("%Y-%m-%d %H:%M")
    final_comparison = final_comparison.reindex(columns=COMPARISON_OUTPUT_COLUMNS)

    return export_dataframe_to_dwh(
        df=final_comparison,
        project_id=OUTPUT_PROJECT_ID,
        dataset_id=OUTPUT_DATASET_ID,
        table_name=COMPARISON_OUTPUT_TABLE_NAME,
        schema_file=COMPARISON_OUTPUT_SCHEMA_FILE,
        LOCAL=MANUAL_RUN,
        write_mode="overwrite",
    )


if __name__ == "__main__":
    run_old_new_positions_comparison(MANUAL_RUN=True)
