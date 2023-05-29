from google.cloud import bigquery

def modify_column_order_and_insert_data(project_id, dataset_id, existing_table_id, temp_table_id):
    # Create a BigQuery client
    client = bigquery.Client(project=project_id)

    # Get references to the existing table and temporary table
    existing_table_ref = client.get_table(f"{project_id}.{dataset_id}.{existing_table_id}")
    temp_table_ref = client.get_table(f"{project_id}.{dataset_id}.{temp_table_id}")

    # Retrieve the schema of the existing table
    existing_table_schema = existing_table_ref.schema

    # Exclude the last 4 columns from the existing table's schema
    excluded_columns = [field for field in existing_table_schema[-4:]]
    new_column_order = existing_table_schema[:-4]

    # Retrieve the schema of the temporary table
    temp_table_schema = temp_table_ref.schema

    # Extend the new column order with the columns from the temporary table
    new_column_order.extend(temp_table_schema)

    # Extend the new column order with the excluded columns
    new_column_order.extend(excluded_columns)

    # Generate the SQL statement to modify the schema of the existing table
    alter_table_sql = f"ALTER TABLE `{project_id}.{dataset_id}.{existing_table_id}`"
    reorder_columns_sql = ", ".join([f"ALTER COLUMN {field.name} SET OPTIONS(position = {index+1})" for index, field in enumerate(new_column_order)])
    sql_statement = f"{alter_table_sql} {reorder_columns_sql}"

    # Execute the SQL statement to modify the schema of the existing table
    client.query(sql_statement).result()

    # Generate the SQL statement to insert data from the temporary table to the existing table
    insert_into_sql = f"INSERT INTO `{project_id}.{dataset_id}.{existing_table_id}` SELECT * FROM `{project_id}.{dataset_id}.{temp_table_id}`"

    # Execute the SQL statement to insert data from the temporary table to the existing table
    client.query(insert_into_sql).result()

    print("Column order modified and data inserted successfully.")

# Set your project ID, dataset ID, existing table ID, and temporary table ID
project_id = "your-project-id"
dataset_id = "your-dataset-id"
existing_table_id = "your-existing-table-id"
temp_table_id = "your-temp-table-id"

# Modify the column order of the existing table and insert data from the temporary table
modify_column_order_and_insert_data(project_id, dataset_id, existing_table_id, temp_table_id)
