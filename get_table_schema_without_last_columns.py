from google.cloud import bigquery

def get_table_schema_without_last_columns(project_id, dataset_id, table_id, num_columns_to_exclude):
    # Create a BigQuery client
    client = bigquery.Client(project=project_id)

    # Get the table reference
    table_ref = client.get_table(f"{project_id}.{dataset_id}.{table_id}")

    # Get the list of columns
    columns = table_ref.schema[:len(table_ref.schema) - num_columns_to_exclude]

    # Create a new schema without the last columns
    new_schema = bigquery.Schema(columns)

    return new_schema

def check_schema_match(project_id, dataset_id, table1_id, table2_id):
    # Get the schema for table1
    table1_schema = get_table_schema_without_last_columns(project_id, dataset_id, table1_id, 2)

    # Get the schema for table2
    table2_schema = get_table_schema_without_last_columns(project_id, dataset_id, table2_id, 0)

    # Check if the schemas match
    if table1_schema == table2_schema:
        print("The schemas match.")
    else:
        print("The schemas do not match.")

# Set your project ID, dataset ID, and table IDs
project_id = 'your-project-id'
dataset_id = 'your-dataset-id'
table1_id = 'table1'
table2_id = 'table2'

# Check if the schemas match, excluding the last 2 columns of table1
# and comparing with the entire schema of table2
check_schema_match(project_id, dataset_id, table1_id, table2_id)
