from google.cloud import storage, bigquery
import os
import tempfile
from datetime import datetime
import pandas as pd
import json

storage_client = storage.Client()
bigquery_client = bigquery.Client()

def upload_file():
    BUCKET_NAME = "globant-db-migration"

    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs()
 
    for blob in blobs:
        TEMP_FILE_PATH = os.path.join(tempfile.mkdtemp(),blob.name)
        blob.download_to_filename(TEMP_FILE_PATH)
        upload_dataframe(TEMP_FILE_PATH)
        os.remove(TEMP_FILE_PATH)
        os.rmdir(os.path.dirname(TEMP_FILE_PATH))
        blob_new_name = datetime.now().strftime("%Y/%m/%d/%H:%M")+'/'+blob.name
        bucket.rename_blob(blob,blob_new_name)

def upload_dataframe(TEMP_FILE_PATH):
    with open('./schemas.json') as file:
        file_contents = file.read()
        parsed_json = json.loads(file_contents)
    file_name = os.path.basename(TEMP_FILE_PATH).lower()
    match file_name:
        case file_name if file_name.startswith('departments'):
            table_id = parsed_json["departments"]["table_id"]
            schema = parsed_json["departments"]["schema"]
            columns = [column["name"] for column in schema]
            df = pd.read_csv(TEMP_FILE_PATH,header=None)
            df.columns = columns
        case file_name if file_name.startswith('jobs'):
            table_id = parsed_json["jobs"]["table_id"]
            schema = parsed_json["jobs"]["schema"]
            columns = [column["name"] for column in schema]
            df = pd.read_csv(TEMP_FILE_PATH,header=None)
            df.columns = columns
        case file_name if file_name.startswith('hired_employees'):
            table_id = parsed_json["employees"]["table_id"]
            schema = parsed_json["employees"]["schema"]
            columns = [column["name"] for column in schema]
            df = pd.read_csv(TEMP_FILE_PATH,header=None)
            df.columns = columns

    if df.shape[0]<=1000:
        upload_to_bq(df,table_id,schema)
        return
    elif df.shape[0]%1000 == 0:
        n_chunks = df.shape[0]/1000
    else:
        n_chunks = int(df.shape[0]/1000) + 1

    dataframes_dict = slice_df(df,n_chunks)
    
    for i in dataframes_dict:
        df = dataframes_dict[i]
        upload_to_bq(df,table_id,schema)

def slice_df(df, n_chunks):   
    batch_size = 1000
    row_start = 0
    row_end = batch_size

    dataframes = {}
    for chunk in range(n_chunks):
        dataframes[chunk] = df.iloc[row_start:row_end,:]
        row_start += batch_size
        row_end += batch_size

    return dataframes

def upload_to_bq(df,table_id,schema):
    dataset_id = 'globant'

    table = bigquery_client.dataset(dataset_id=dataset_id).table(table_id=table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.schema = schema
    job_config.write_disposition = 'WRITE_APPEND'
    job_config.create_disposition = 'CREATE_IF_NEEDED'
    load_job = bigquery_client.load_table_from_dataframe(
        dataframe=df
        ,destination=table
        ,job_config=job_config)
    load_job.result()