from google.cloud import bigquery
import pandas as pd
from functions import get_data_from_dwh
from constants import TORKIN_POSITIONS_PROJECT_ID,TORKIN_POSITIONS_QUERY




# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    torkin_positions_df = get_data_from_dwh(project_id=TORKIN_POSITIONS_PROJECT_ID,
                                            query= TORKIN_POSITIONS_QUERY
                                           )







# See PyCharm help at https://www.jetbrains.com/help/pycharm/
