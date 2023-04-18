from google.cloud import storage
from google.cloud import bigquery

# Set up GCS client and bucket name
storage_client = storage.Client()
bucket_name = 'your_bucket_name'

# Set up BigQuery client and table name
bigquery_client = bigquery.Client()
table_name = 'your_dataset.your_table_name'

# Set the name of the file you're looking for
file_name = 'your_file_name.txt'

# Get list of files in bucket
blobs = storage_client.list_blobs(bucket_name)

# Loop through blobs and check prefix against table
for blob in blobs:
    if blob.name == file_name:
        for row in bigquery_client.list_rows(table_name):
            if blob.name.startswith(row.prefix):
                system_name = row.system_name
                print(f"The file {blob.name} belongs to system {system_name}")
