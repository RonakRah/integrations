import pandas as pd
from datetime import datetime, UTC

from functions import (
    export_dataframe_to_dwh,union_positions_and_potential_positions,
    filter_positions,
    get_data_from_dwh
)
from constants import FINAL_OUTPUT_COLUMNS, FINAL_OUTPUT_RENAME_MAP, FINAL_OUTPUT_SOURCE_COLUMNS, MANUAL_OUTPUT_FILE,QUERY_FOR_POTENTIAL_STOPS
from constants import INTEGRATION_COUNTRY_MODE_MAPPING_DICT
from constants import (
    INTEGRATIONS_AND_THEIR_PROVIDERS_PROJECT_ID,
    INTEGRATIONS_AND_THEIR_PROVIDERS_QUERY,TORKIN_POSITIONS_QUERY
)
from constants import TORKIN_POSITIONS_PROJECT_ID
from constants import OUTPUT_PROJECT_ID, OUTPUT_DATASET_ID, OUTPUT_TABLE_NAME

def main(MANUAL_RUN=False):
    # data loading
    torkin_positions_df = get_data_from_dwh(
        project_id=TORKIN_POSITIONS_PROJECT_ID,
        query=TORKIN_POSITIONS_QUERY
    )
    potential_stops = get_data_from_dwh(project_id=OUTPUT_PROJECT_ID, query=QUERY_FOR_POTENTIAL_STOPS)

    all_stations = union_positions_and_potential_positions(stations=torkin_positions_df,potentials=potential_stops)
    integrations_and_providers_df = get_data_from_dwh(project_id=INTEGRATIONS_AND_THEIR_PROVIDERS_PROJECT_ID,
                                                      query=INTEGRATIONS_AND_THEIR_PROVIDERS_QUERY)



    travel_modes = list(INTEGRATION_COUNTRY_MODE_MAPPING_DICT.keys())
    all_modes_results = []

    for mode in travel_modes:
        # print(mode)

        positions_by_mode = all_stations[
            all_stations["positionType"].str.startswith(mode, na=False)
        ]
        integrations_for_travel_mode = list(
            INTEGRATION_COUNTRY_MODE_MAPPING_DICT[mode].keys()
        )
        filtered_positions = filter_positions(
            df=positions_by_mode,
            integration_providers= integrations_and_providers_df,
            mode=mode,
            integrations=integrations_for_travel_mode,
        )
        filtered_positions["mode"] = mode
        all_modes_results.append(filtered_positions)

    if not all_modes_results:
        raise ValueError("No results found: all_modes_results is empty")

    final_df = pd.concat(all_modes_results, ignore_index=True)
    final_df = final_df.reindex(columns=FINAL_OUTPUT_SOURCE_COLUMNS)
    final_df = final_df.rename(columns=FINAL_OUTPUT_RENAME_MAP)
    final_df["updateAt"] = datetime.now(UTC)
    final_df = final_df.reindex(columns=FINAL_OUTPUT_COLUMNS)
    # if MANUAL_RUN:
    #     # print()
    #     # return export_main_results_to_excel(final_df, MANUAL_OUTPUT_FILE)
    #     pass
    return export_dataframe_to_dwh(
        df=final_df,
        project_id=OUTPUT_PROJECT_ID,
        dataset_id=OUTPUT_DATASET_ID,
        table_name=OUTPUT_TABLE_NAME,
        schema_file=None,
        LOCAL=MANUAL_RUN,
        write_mode="overwrite",
    )
