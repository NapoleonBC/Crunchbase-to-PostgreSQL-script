import os
from rdflib import Graph
from tqdm import tqdm
import psycopg2

# Step 1: Read the crunchbase data from the .nt file using streaming parser
def read_large_nt_file(file_path, batch_size):
    print("Initializing the graph")
    graph = Graph()
    with open(file_path, "rb") as file:
        for i, line in enumerate(tqdm(file, desc='Processing', unit='lines')):
            # print (line)
            graph.parse(data=line, format="nt")
            if i > 0 and i % batch_size == 0:
                # print ("~~~~~~~~~~~~~~~~~", 0)
                yield graph
                graph = Graph()

    # Return the remaining triples if the total number is not a multiple of batch_size
    if graph:
        yield graph


def write_to_script_file(graph, table_name, output_script_path, batch_size, index):
    _dir = "script_parts/"+output_script_path+str(index)+ ".sql"
    os.makedirs(os.path.dirname(_dir), exist_ok=True)
    with open(_dir, 'w', encoding='utf-8') as script_file:
        # Write the table creation statement to the script file
        create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
subject TEXT,
predicate TEXT,
obj TEXT
);
"""
        script_file.write(create_table_sql)

        # Add a newline after the table creation statement
        script_file.write("\n")
    with open(_dir, "a", encoding='utf-8') as script_file:

        # Initialize an empty list to store data for each batch
        data = []

        for triple in graph:
            # Process the triple and extract relevant information to insert
            subject, predicate, obj = triple

            # Add the processed data to the 'data' list (customize this based on your data structure)
            data.append((str(subject), str(predicate), str(obj)))

            if len(data) >= batch_size:
                # Generate SQL insert statements for the current batch
                insert_sql = (
                    f"INSERT INTO {table_name} (subject, predicate, obj) VALUES %s;"
                )
                values_sql = ",".join(
                    cur.mogrify("(%s, %s, %s)", row).decode("utf-8") for row in data
                )
                sql_statement = insert_sql % values_sql

                # Write the SQL insert statements to the script file for the current batch
                script_file.write(sql_statement)
                script_file.write("\n")

                # Clear the 'data' list for the next batch
                data.clear()

        # Handle the remaining data that might not fill a full batch
        if data:
            # Generate SQL insert statements for the remaining data
            insert_sql = (
                f"INSERT INTO {table_name} (subject, predicate, obj) VALUES %s;"
            )
            values_sql = ",".join(
                cur.mogrify("(%s, %s, %s)", row).decode("utf-8") for row in data
            )
            sql_statement = insert_sql % values_sql

            # Write the SQL insert statements to the script file for the remaining data
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
    table_name = "generated_cursor"
    output_script_path = file_name

    ####

    file_path = file_name + ".nt"
    graph_batches = read_large_nt_file(file_path, batch_size*100)

    for i, graph_batch in enumerate(graph_batches):            
        write_to_script_file(graph_batch, table_name, output_script_path, batch_size, i+1)
        print(f"Batch {i + 1} processed.")

    print(f"SQL script file '{output_script_path}' created.")

