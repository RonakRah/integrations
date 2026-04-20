import pandas as pd

from functions import get_data_from_dwh, filter_positions, write_dataframe_to_bigquery
from constants import TORKIN_POSITIONS_PROJECT_ID, TORKIN_POSITIONS_QUERY
from constants import INTEGRATION_COUNTRY_MODE_MAPPING_DICT
from constants import OUTPUT_PROJECT_ID, OUTPUT_DATASET_ID, OUTPUT_TABLE_NAME, FINAL_OUTPUT_COLUMNS


def main():
    torkin_positions_df = get_data_from_dwh(
        project_id=TORKIN_POSITIONS_PROJECT_ID,
        query=TORKIN_POSITIONS_QUERY,
    )

    travel_modes = list(INTEGRATION_COUNTRY_MODE_MAPPING_DICT.keys())
    all_modes_results = []

    for mode in travel_modes:
        print(mode)
        positions_by_mode = torkin_positions_df[
            torkin_positions_df["positionType"].str.startswith(mode, na=False)
        ]
        integrations_for_travel_mode = list(
            INTEGRATION_COUNTRY_MODE_MAPPING_DICT[mode].keys()
        )
        filtered_positions = filter_positions(
            positions_by_mode,
            mode,
            integrations_for_travel_mode,
        )
        filtered_positions["mode"] = mode
        all_modes_results.append(filtered_positions)

    if not all_modes_results:
        return pd.DataFrame(columns=FINAL_OUTPUT_COLUMNS)

    final_df = pd.concat(all_modes_results, ignore_index=True)
    return final_df.reindex(columns=FINAL_OUTPUT_COLUMNS)


def export_main_results():
    final_df = main()
    table_id = write_dataframe_to_bigquery(
        df=final_df,
        project_id=OUTPUT_PROJECT_ID,
        dataset_id=OUTPUT_DATASET_ID,
        table_name=OUTPUT_TABLE_NAME,
    )
    return table_id
