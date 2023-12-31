import os
from rdflib import Graph
from tqdm import tqdm
import psycopg2
from utils import extract_field_name, extract_original_url, extract_table_name_and_id, convert_to_valid_date_format, name_validator, value_validator
import json
from datetime import datetime

table_name_dict = {}

def table_structure_print():
    print (table_name_dict)
    with open ("table_structure_"+datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S-%f")+".json", 'w') as file:
        file.write(json.JSONEncoder().encode(table_name_dict))

# Step 1: Read the crunchbase data from the .nt file using streaming parser
def read_large_nt_file(file_path, batch_size):
    print("Initializing the graph")
    graph = Graph()
    with open(file_path, "rb") as file:
        for i, line in enumerate(tqdm(file, desc='Processing', unit='lines')):
            try:
                # Extract url from encoded text
                # print (line)
                line = line.decode('utf-8')
                line = extract_original_url(line)
                line = line.encode('utf-8')
                graph.parse(data=line, format="nt")
                if i > 0 and i % batch_size == 0:
                    yield graph
                    graph = Graph()
            except Exception as e:
                print (e.args[0])

    # Return the remaining triples if the total number is not a multiple of batch_size
    if graph:
        yield graph


def write_to_script_file(graph, output_script_path, batch_size, index):
    _dir = "script_parts/"+output_script_path+str(index)+ ".sql"
    os.makedirs(os.path.dirname(_dir), exist_ok=True) 
    with open(_dir, 'w', encoding='utf-8') as script_file:
        # Add a newline after the table creation statement
        # Function to check if the column already exists in the table
        function_str = """
CREATE OR REPLACE FUNCTION my_column_exists(input_table_name name, input_column_name name)
RETURNS boolean AS $$
DECLARE
    col_exists boolean;
BEGIN
    SELECT EXISTS(
        SELECT 1
        FROM information_schema.columns
        WHERE input_table_name = $1 AND input_column_name = $2
    ) INTO col_exists;

    RETURN col_exists;
END;
$$ LANGUAGE plpgsql;
"""
        # script_file.write(function_str)
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
            table_name = name_validator(table_name)
            field_name = name_validator(field_name)
            value = value_validator(value)

            # Write the table creation statement to the script file
            if not table_name_dict.get(table_name, None):
                create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (id VARCHAR(255) PRIMARY KEY);"
                # script_file.write(create_table_sql)
                table_name_dict[table_name] = {}

            if not table_name_dict[table_name].get(field_name, None):
                field_type = 'TEXT'
                if 'trust_code' in field_name or field_name == 'price' or field_name == 'price_usd':
                    field_type = 'INTEGER'
                elif field_name == 'created_at' or field_name == 'updated_at' or '_on' in field_name:
                    field_type = 'TIMESTAMP'

                alter_table_add_column_sql = f"""
DO $$
BEGIN
IF NOT my_column_exists('{table_name}', '{field_name}') THEN
    EXECUTE 'ALTER TABLE {table_name} ADD COLUMN {field_name} {field_type};';
END IF;
END $$;
"""
                # script_file.write(alter_table_add_column_sql)
                table_name_dict[table_name][field_name] = True

            if not table_data_dict.get(table_name, None):
                table_data_dict[table_name] = {}

            if not table_data_dict[table_name].get(record_id, None):
                table_data_dict[table_name][record_id] = {}

            # Add the processed data to the 'data' list (customize this based on your data structure)
            table_data_dict[table_name][record_id][field_name] = "'"+str(value)+"'"
            if 'trust_code' in field_name:
                pass
            elif field_name == 'created_at' or field_name == 'updated_at' or '_on' in field_name:
                field_type = 'TIMESTAMP'
                value = convert_to_valid_date_format(value)
                table_data_dict[table_name][record_id][field_name] = f"TO_TIMESTAMP('{value}', 'YYYY-MM-DD')"
            
            cnt += 1

            if cnt >= batch_size:
                # Generate SQL insert statements for the current batch
                for key, val in table_data_dict.items():
                    table_name = key

                    for _key, _val in val.items():
                        record_id = _key
                        field_names = ''
                        update_set_str = ''
                        values = ''
                        is_uuid = False
                        for __key, __val in _val.items():
                            field_names += f"{__key}, "
                            values += f"{__val}, "
                            update_set_str += f"{__key} = EXCLUDED.{__key}, "
                            if __key == 'uuid':
                                is_uuid = True
                        if not is_uuid:
                            field_names += "uuid, "
                            values += f"'{record_id}', "
                            update_set_str += "uuid = EXCLUDED.uuid, "
                        field_names = field_names[:-2]
                        values = values[:-2]
                        update_set_str = update_set_str[:-2]

                        insert_sql = f"""INSERT INTO {table_name} ({field_names}) VALUES %s
    ON CONFLICT (uuid) DO UPDATE SET {update_set_str};
    """
                        values_sql = f"({values})" # f"('{record_id}', {values})"

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
                table_name = key

                for _key, _val in val.items():
                    record_id = _key
                    field_names = ''
                    update_set_str = ''
                    values = ''
                    is_uuid = False
                    for __key, __val in _val.items():
                        field_names += f"{__key}, "
                        values += f"{__val}, "
                        update_set_str += f"{__key} = EXCLUDED.{__key}, "
                        if __key == 'uuid':
                            is_uuid = True
                    if not is_uuid:
                        field_names += "uuid, "
                        values += f"'{record_id}', "
                        update_set_str += "uuid = EXCLUDED.uuid, "
                    field_names = field_names[:-2]
                    values = values[:-2]
                    update_set_str = update_set_str[:-2]

                    insert_sql = f"""INSERT INTO {table_name} ({field_names}) VALUES %s
ON CONFLICT (uuid) DO UPDATE SET {update_set_str};
"""
                    values_sql = f"({values})" # f"('{record_id}', {values})"

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
    file_name = "cb-complete_sorted"
    batch_size = 5000
    output_script_path = file_name

    ####

    file_path = file_name + ".nt"
    graph_batches = read_large_nt_file(file_path, batch_size*300)

    for i, graph_batch in enumerate(graph_batches):            
        write_to_script_file(graph_batch, output_script_path, batch_size*300, i+1)
        print(f"Batch {i + 1} processed.")

    print(f"SQL script file '{output_script_path}' created.")

    table_structure_print()