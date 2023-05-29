from google.cloud import bigquery

def insert_data_with_new_columns(project_id, dataset_id, table1_id, temp_table_id):
    # Create a BigQuery client
    client = bigquery.Client(project=project_id)

    # Get references to the tables
    table1_ref = client.get_table(f"{project_id}.{dataset_id}.{table1_id}")
    temp_table_ref = client.get_table(f"{project_id}.{dataset_id}.{temp_table_id}")

    # Get the schema of table1
    table1_schema = table1_ref.schema

    # Get the schema of temp_table
    temp_table_schema = temp_table_ref.schema

    # Get the columns common to table1 and temp_table
    common_columns = [field.name for field in temp_table_schema if field.name in table1_schema]

    # Generate the SQL statement to insert data from temp_table into table1
    insert_into_sql = f"""
        INSERT INTO `{project_id}.{dataset_id}.{table1_id}`
        SELECT {", ".join(common_columns)}, NULL AS new_column1, NULL AS new_column2
        FROM `{project_id}.{dataset_id}.{temp_table_id}`
    """

    # Execute the INSERT INTO statement to insert data with new columns
    client.query(insert_into_sql).result()

    print("Data inserted successfully.")

# Set your project ID, dataset ID, table1 ID, and temp_table ID
project_id = "your-project-id"
dataset_id = "your-dataset-id"
table1_id = "table1"
temp_table_id = "temp_table"

# Insert data from temp_table into table1 with new columns
insert_data_with_new_columns(project_id, dataset_id, table1_id, temp_table_id)
