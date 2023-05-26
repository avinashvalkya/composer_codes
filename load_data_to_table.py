from google.cloud import bigquery

def load_data_to_table(project_id, dataset_id, table1_id, table2_id):
    # Create a BigQuery client
    client = bigquery.Client(project=project_id)

    # Get the references to the tables
    table1_ref = client.get_table(f"{project_id}.{dataset_id}.{table1_id}")
    table2_ref = client.get_table(f"{project_id}.{dataset_id}.{table2_id}")

    # Get the schema of Table1
    table1_schema = table1_ref.schema

    # Get the schema of Table2
    table2_schema = table2_ref.schema

    # Determine the index to insert the new columns
    insert_index = len(table1_schema.fields) - 4

    # Insert the new columns from Table2 into Table1 schema at the specified index
    updated_schema = table1_schema.fields[:insert_index] + table2_schema.fields + table1_schema.fields[insert_index:]

    # Generate the configuration for the load job
    job_config = bigquery.LoadJobConfig(
        schema=updated_schema,
        write_disposition="WRITE_APPEND"
    )

    # Start the load job to load data from Table2 into Table1
    load_job = client.load_table_from_table(
        source_table=table2_ref,
        destination_table=table1_ref,
        job_config=job_config
    )

    # Wait for the load job to complete
    load_job.result()

    # Check if the load job succeeded
    if load_job.errors:
        print("Error loading data into BigQuery table:")
        for error in load_job.errors:
            print(error)
    else:
        print("Data loaded successfully.")

# Set your project ID, dataset ID, table1 ID, and table2 ID
project_id = "your-project-id"
dataset_id = "your-dataset-id"
table1_id = "table1"
table2_id = "table2"

# Load data from table2 to table1 while modifying the schema of table1
load_data_to_table(project_id, dataset_id, table1_id, table2_id)
