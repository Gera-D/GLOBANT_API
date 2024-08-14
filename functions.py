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

def hires_per_quarter_2021():
    query = """SELECT 
        d.department
        ,j.job
        ,COUNTIF(EXTRACT(QUARTER FROM cast(e.datetime as timestamp))=1) as Q1
        ,COUNTIF(EXTRACT(QUARTER FROM cast(e.datetime as timestamp))=2) as Q2
        ,COUNTIF(EXTRACT(QUARTER FROM cast(e.datetime as timestamp))=3) as Q3
        ,COUNTIF(EXTRACT(QUARTER FROM cast(e.datetime as timestamp))=4) as Q4
        FROM `globant.employees` e
        INNER JOIN `globant.jobs` j
        ON e.job_id = j.id
        INNER JOIN `globant.departments` d
        ON e.department_id = d.id
        WHERE EXTRACT(YEAR FROM cast(e.datetime as timestamp))=2021
        GROUP BY department, job
        ORDER BY department, job;"""
    query_job = bigquery_client.query(query=query)
    response = query_job.result()
    records = [dict(row) for row in response]
    response_json = json.dumps(str(records))
    return response_json

def hires_greater_than_mean_2021():
    query = """DECLARE MEAN_HIRES FLOAT64;
        SET MEAN_HIRES = (
        SELECT (COUNT(id)/COUNT(DISTINCT department_id)) 
        FROM `globant.employees` 
        WHERE EXTRACT(YEAR FROM cast(datetime as timestamp))=2021
        );

        SELECT
        d.id
        ,d.department
        ,COUNT(e.id) AS hired
        FROM `globant.employees` e
        INNER JOIN `globant.departments` d
        ON e.department_id = d.id
        WHERE EXTRACT(YEAR FROM cast(e.datetime as timestamp))=2021
        GROUP BY d.id,d.department
        HAVING hired > MEAN_HIRES
        ORDER BY hired DESC;"""
    query_job = bigquery_client.query(query=query)
    response = query_job.result()
    records = [dict(row) for row in response]
    response_json = json.dumps(str(records))
    return response_json