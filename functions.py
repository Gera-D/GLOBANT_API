from google.cloud import storage
import os
import tempfile
from datetime import datetime

storage_client = storage.Client()

def upload_file():
    BUCKET_NAME = "globant-db-migration"

    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs()
 
    for blob in blobs:
        TEMP_FILE_PATH = os.path.join(tempfile.mkdtemp(),blob.name)
        blob.download_to_filename(TEMP_FILE_PATH)
        os.remove(TEMP_FILE_PATH)
        os.rmdir(os.path.dirname(TEMP_FILE_PATH))
        blob_new_name = datetime.now().strftime("%Y/%m/%d/%H:%M")+'/'+blob.name
        bucket.rename_blob(blob,blob_new_name)