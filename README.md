# Crunchbase-to-PostgreSQL-script
Python code to change crunchbase nt file to postgresql script with some validation algorithms and graph of rdflib

pip install -r requirements.txt
python main.py


# temporary
rdf2pg -f cb-complete_sorted.nt -o crunchbase_script.sql
psql -U your_username -d your_database -f crunchbase_script.sql