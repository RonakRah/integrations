import pandas as pd

from functions import get_data_from_dwh, filter_positions, export_main_results_to_dwh
from constants import TORKIN_POSITIONS_PROJECT_ID, TORKIN_POSITIONS_QUERY
from constants import INTEGRATION_COUNTRY_MODE_MAPPING_DICT
from constants import FINAL_OUTPUT_COLUMNS
from constants import INTEGRATIONS_AND_THEIR_PROVIDERS_QUERY,INTEGRATIONS_AND_THEIR_PROVIDERS_PROJECT_ID


def main():
    # data loading
    torkin_positions_df = get_data_from_dwh(
        project_id=TORKIN_POSITIONS_PROJECT_ID,
        query=TORKIN_POSITIONS_QUERY,
    )
    integrations_and_providers_df = get_data_from_dwh(project_id=INTEGRATIONS_AND_THEIR_PROVIDERS_PROJECT_ID,
                                                      query=INTEGRATIONS_AND_THEIR_PROVIDERS_QUERY)

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
    final_df = final_df.reindex(columns=FINAL_OUTPUT_COLUMNS)
    return export_main_results_to_dwh(final_df)
