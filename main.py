import pandas as pd

from bi.dag_resources.integrations.functions import (
    export_main_results_to_dwh,
    export_main_results_to_excel,
    filter_positions,
    get_data_from_dwh
)
from bi.dag_resources.integrations.constants import FINAL_OUTPUT_COLUMNS, MANUAL_OUTPUT_FILE
from bi.dag_resources.integrations.constants import INTEGRATION_COUNTRY_MODE_MAPPING_DICT
from bi.dag_resources.integrations.constants import (
    INTEGRATIONS_AND_THEIR_PROVIDERS_PROJECT_ID,
    INTEGRATIONS_AND_THEIR_PROVIDERS_QUERY,TORKIN_POSITIONS_QUERY
)
from bi.dag_resources.integrations.constants import TORKIN_POSITIONS_PROJECT_ID

def main(MANUAL_RUN=True):
    # data loading
    torkin_positions_df = get_data_from_dwh(
        project_id=TORKIN_POSITIONS_PROJECT_ID,
        query=TORKIN_POSITIONS_QUERY
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
    if MANUAL_RUN:
        return export_main_results_to_excel(final_df, MANUAL_OUTPUT_FILE)
    # return export_main_results_to_dwh(final_df)
    return final_df.head(10)
