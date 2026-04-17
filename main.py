from google.cloud import bigquery
import pandas as pd
from functions import get_data_from_dwh
from constants import TORKIN_POSITIONS_PROJECT_ID,TORKIN_POSITIONS_QUERY,TORKIN_COUNTRY_QUERY




# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # loading data
    torkin_positions_df = get_data_from_dwh(project_id=TORKIN_POSITIONS_PROJECT_ID,
                                            query= TORKIN_POSITIONS_QUERY
                                           )
    torkin_country_df = get_data_from_dwh(project_id=TORKIN_POSITIONS_PROJECT_ID,
                                          query=TORKIN_COUNTRY_QUERY)


    join_positions_with_country = torkin_positions_df.merge(
                                                            torkin_country_df,
                                                            how="left",
                                                            left_on="countryId",
                                                            right_on="id"
                                                        ).drop(columns='id')
    print('hi')







# See PyCharm help at https://www.jetbrains.com/help/pycharm/
