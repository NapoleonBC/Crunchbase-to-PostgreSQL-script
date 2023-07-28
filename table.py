import json

def convert_name(name):
    # Replace hyphens with underscores in the name
    name = name.replace('-', '_')
    return name

def generate_sql_tables(json_data, sql_file_path):
    with open(sql_file_path, 'w') as sql_file:
        for table_name, columns in json_data.items():
            # Convert table_name to remove hyphens
            table_name = convert_name(table_name)
            
            sql_file.write(f"CREATE TABLE IF NOT EXISTS {table_name} (\n")
            primary_key_set = False
            for i, (column, data_type) in enumerate(columns.items()):
                # Convert column name to remove hyphens
                column = convert_name(column)
                
                field_type = 'TEXT'
                if 'trust_code' in column or column == 'price' or column == 'price_usd':
                    field_type = 'BIGINT'
                elif column == 'created_at' or column == 'updated_at' or '_on' in column:
                    field_type = 'TIMESTAMP'
                
                if column == 'uuid':
                    sql_file.write(f"    {column} {field_type} PRIMARY KEY")
                    primary_key_set = True
                else:
                    sql_file.write(f"    {column} {field_type}")
                if i < len(columns)-1:
                    sql_file.write(',\n')
                else:
                    sql_file.write('\n')
            
            # If the primary key is not set (uuid column not found), add it at the end.
            # if not primary_key_set:
            #     sql_file.write("    uuid TEXT PRIMARY KEY")
            
            sql_file.write(");\n\n")

if __name__ == "__main__":
    file_name = "table_structure_2023-07-26_07-30-44-899969"
    json_file_path = file_name + ".json"
    sql_file_path = file_name + ".sql"

    with open(json_file_path, 'r') as json_file:
        json_data = json.load(json_file)

    generate_sql_tables(json_data, sql_file_path)
