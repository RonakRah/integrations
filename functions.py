from google.cloud import bigquery

from google.cloud import bigquery

def get_data_from_dwh( project_id,query):
    client = bigquery.Client(project=project_id)

    df = client.query(query).to_dataframe()
    return df