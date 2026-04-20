
from functions import get_data_from_dwh,filter_positions
from constants import TORKIN_POSITIONS_PROJECT_ID,TORKIN_POSITIONS_QUERY
from constants import INTEGRATION_COUNTRY_MODE_MAPPING_DICT,NO_FILTER_FOR_THESE_INTEGRATIONS




# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # loading data
    torkin_positions_df = get_data_from_dwh(project_id=TORKIN_POSITIONS_PROJECT_ID,
                                            query= TORKIN_POSITIONS_QUERY
                                           )


    travel_modes = list(INTEGRATION_COUNTRY_MODE_MAPPING_DICT.keys())
    all_modes_results = {}
    for mode in travel_modes:
        print(mode)
        # filter station type by mode (train / bus)
        positions_by_mode = torkin_positions_df[
            torkin_positions_df["positionType"].str.startswith(mode, na=False)]

        # integrations by mode
        integrations_for_travel_mode = list(
            INTEGRATION_COUNTRY_MODE_MAPPING_DICT[mode].keys()
        )

        filtered_positions = filter_positions(positions_by_mode,mode,integrations_for_travel_mode)
        all_modes_results[mode] = filtered_positions
    print()




    print('hi')







# See PyCharm help at https://www.jetbrains.com/help/pycharm/
