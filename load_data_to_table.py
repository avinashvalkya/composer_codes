from google.cloud import bigquery

def load_data_to_table(project_id, dataset_id, table1_id, table2_id):
    # Create a BigQuery client
    client = bigquery.Client(project=project_id)

    # Get the references to the tables
    table1_ref = client.get_table(f"{project_id}.{dataset_id}.{table1_id}")
    table2_ref = client.get_table(f"{project_id}.{dataset_id}.{table2_id}")

    # Get the schema of Table1
    table1_schema = client.get_table(table1_ref).schema

    # Get the schema of Table2
    table2_schema = client.get_table(table2_ref).schema

    # Determine the index to insert the new columns
    insert_index = len(table1_schema) - 4

    # Generate the SQL statement to modify the schema of Table1
    alter_table_sql = f"ALTER TABLE `{project_id}.{dataset_id}.{table1_id}`"

    # Generate the SQL statement to add the new columns from Table2 to Table1 at the specified index
    add_columns_sql = ", ".join([f"ADD COLUMN {field.name} {field.field_type}" for field in table2_schema])

    # Execute the ALTER TABLE statement to modify the schema of Table1
    client.query(f"{alter_table_sql} {add_columns_sql} AFTER {table1_schema[insert_index].name}").result()

    # Generate the SQL statement to insert data from Table2 into Table1
    insert_into_sql = f"INSERT INTO `{project_id}.{dataset_id}.{table1_id}` SELECT * FROM `{project_id}.{dataset_id}.{table2_id}`"

    # Execute the INSERT INTO statement to load data from Table2 into Table1
    client.query(insert_into_sql).result()

    print("Data loaded successfully.")

# Set your project ID, dataset ID, table1 ID, and table2 ID
project_id = "your-project-id"
dataset_id = "your-dataset-id"
table1_id = "table1"
table2_id = "table2"

# Load data from table2 to table1 while modifying the schema of table1
load_data_to_table(project_id, dataset_id, table1_id, table2_id)
