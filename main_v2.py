import os
from rdflib import Graph
from tqdm import tqdm
import psycopg2
from utils import extract_field_name, extract_original_url, extract_table_name_and_id, convert_to_valid_date_format

table_name_dict = {}
# Step 1: Read the crunchbase data from the .nt file using streaming parser
def read_large_nt_file(file_path, batch_size):
    print("Initializing the graph")
    graph = Graph()
    with open(file_path, "rb") as file:
        for i, line in enumerate(tqdm(file, desc='Processing', unit='lines')):
            # Extract url from encoded text
            # print (line)
            line = line.decode('utf-8')
            line = extract_original_url(line)
            line = line.encode('utf-8')
            graph.parse(data=line, format="nt")
            if i > 0 and i % batch_size == 0:
                yield graph
                graph = Graph()

    # Return the remaining triples if the total number is not a multiple of batch_size
    if graph:
        yield graph


def write_to_script_file(graph, output_script_path, batch_size, index):
    _dir = "script_parts/"+output_script_path+str(index)+ ".sql"
    os.makedirs(os.path.dirname(_dir), exist_ok=True)
    with open(_dir, 'w', encoding='utf-8') as script_file:
        # Add a newline after the table creation statement
        script_file.write("\n")
        
    with open(_dir, "a", encoding='utf-8') as script_file:

        # Initialize an empty list to store data for each batch
        table_data_dict = {}
        cnt = 0

        for triple in graph:
            # Process the triple and extract relevant information to insert
            subject, predicate, obj = triple

            table_name, record_id = extract_table_name_and_id(subject)
            field_name = extract_field_name(predicate)
            value = extract_field_name(obj)
            if not table_name or not record_id or not field_name or not value:
                continue

            # Write the table creation statement to the script file
            if not table_name_dict.get(table_name, None):
                create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (id STRING PRIMARY KEY);"
                script_file.write(create_table_sql)
                table_name_dict[table_name] = {}
                table_data_dict[table_name] = {}

            if not table_name_dict[table_name].get(field_name, None):
                field_type = 'TEXT'
                if 'trust_code' in field_name or field_name == 'price' or field_name == 'price_usd':
                    field_type = 'INTEGER'
                elif field_name == 'created_at' or field_name == 'updated_at' or '_on' in field_name:
                    field_type = 'TIMESTAMP'

                alter_table_add_column_sql = f"""
IF NOT column_exists('{table_name}', '{field_name}') THEN
    EXECUTE 'ALTER TABLE {table_name} ADD COLUMN {field_name} {field_type};';
END IF;
"""
                script_file.write(alter_table_add_column_sql)
                table_name_dict[table_name][field_name] = True

            if not table_data_dict[table_name].get(record_id, None):
                table_data_dict[table_name][record_id] = {}
                table_data_dict[table_name][record_id]["field_name"] = []
                table_data_dict[table_name][record_id]["value"] = []

            # Add the processed data to the 'data' list (customize this based on your data structure)
            table_data_dict[table_name][record_id]["field_name"].append(str(field_name))
            if 'trust_code' in field_name:
                pass
            elif field_name == 'created_at' or field_name == 'updated_at' or '_on' in field_name:
                field_type = 'TIMESTAMP'
                value = convert_to_valid_date_format(value)
            table_data_dict[table_name][record_id]["value"].append("'"+str(value)+"'")
            cnt += 1

            if cnt >= batch_size:
                # Generate SQL insert statements for the current batch
                for key, val in table_data_dict.items():
                    for _key, _val in val.items():
                        field_names = ', '.join(_ for _ in table_data_dict[table_name][record_id]["field_name"])
                        values = ', '.join(_ for _ in table_data_dict[table_name][record_id]["value"])
                        insert_sql = f"INSERT INTO {key} (id, {field_names}) VALUES %s;"
                        values_sql = f"({record_id}, {values})"

                    sql_statement = insert_sql % values_sql

                    # Write the SQL insert statements to the script file for the current batch
                    script_file.write(sql_statement)
                    script_file.write("\n")

                # Clear the 'data' list for the next batch
                table_data_dict.clear()

        # Handle the remaining data that might not fill a full batch
        if table_data_dict:
            # Generate SQL insert statements for the current batch
            for key, val in table_data_dict.items():
                for _key, _val in val.items():
                    field_names = ', '.join(_ for _ in table_data_dict[table_name][record_id]["field_name"])
                    values = ', '.join(_ for _ in table_data_dict[table_name][record_id]["value"])
                    insert_sql = f"INSERT INTO {key} (id, {field_names}) VALUES %s;"
                    values_sql = f"('{record_id}', {values})"

                sql_statement = insert_sql % values_sql

                # Write the SQL insert statements to the script file for the current batch
                script_file.write(sql_statement)
                script_file.write("\n")


if __name__ == "__main__":
    db_params = {
        "dbname": "postgres",
        "user": "postgres",
        "password": "123456",
        "host": "localhost",
        "port": "5432",
    }
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()


    # file_name = "cb-complete_sorted"
    file_name = "temp"
    batch_size = 5000
    output_script_path = file_name

    ####

    file_path = file_name + ".nt"
    graph_batches = read_large_nt_file(file_path, batch_size*100)

    for i, graph_batch in enumerate(graph_batches):            
        write_to_script_file(graph_batch, output_script_path, batch_size, i+1)
        print(f"Batch {i + 1} processed.")

    print(f"SQL script file '{output_script_path}' created.")

