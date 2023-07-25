import psycopg2

def process_triple(triple, table_name):
    # Process the triple and extract relevant information to insert
    subject, predicate, obj = triple

    # Generate SQL insert statements for the triple and write to the script file
    insert_sql = f"INSERT INTO {table_name} (subject, predicate, obj) VALUES (%s, %s, %s);\n"
    values = (str(subject), str(predicate), str(obj))

    return insert_sql, values

def process_nt_file_part(file_path, table_name, output_script_path, start_line, end_line):
    with open(file_path, "rb") as file:
        with open(output_script_path, 'a') as script_file:  # Append to the script file
            # Seek to the starting line
            file.seek(start_line)

            for line_number, line in enumerate(file, start=1):
                # Parse the triple from the line using the streaming parser
                subject, predicate, obj = line.decode('utf-8').strip().split(' ', 2)

                # Process the triple and generate the SQL insert statement
                insert_sql, values = process_triple((subject, predicate, obj), table_name)

                # Write the SQL insert statement to the script file
                script_file.write(cur.mogrify(insert_sql, values).decode('utf-8'))

                # Check if we have reached the end line for this part
                if line_number == end_line:
                    # Add a newline to separate this part from the next part
                    script_file.write('\n')
                    break

# Usage example:
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
    
    file_name = 'temp'
    file_path = file_name+'.nt'  # Replace with the actual file path
    table_name = 'generated_cursor'  # Replace with your actual table name
    output_script_path = file_name+'.sql'  # Replace with the desired output script file path
    batch_size = 1000  # You can adjust the batch size as needed

    # Divide the large file into smaller parts (e.g., 1GB parts)
    part_size_bytes = 1 * 1024 * 1024 * 1024  # 1 GB in bytes
    with open(file_path, 'rb') as file:
        num_lines = sum(1 for _ in file)
    num_parts = (num_lines + batch_size - 1) // batch_size  # Calculate the number of parts
    lines_per_part = (num_lines + num_parts - 1) // num_parts  # Calculate lines per part

    # Process each part separately
    for part_index in range(num_parts):
        start_line = part_index * lines_per_part
        end_line = min((part_index + 1) * lines_per_part, num_lines)
        process_nt_file_part(file_path, table_name, output_script_path, start_line, end_line)
