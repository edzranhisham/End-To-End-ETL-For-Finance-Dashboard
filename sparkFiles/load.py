import psycopg2

# Database connection parameters
host = "192.168.1.15"  # Use the host's IP address
port = 5432  # Default PostgreSQL port
dbname = "personal-finance"
user = "postgres"
password = ""

# set up the connection to the Postgres database
conn = psycopg2.connect(f'dbname={dbname} host={host} port={port} user={user} password={password}')

# start a cursor to perform database operations
cursor = conn.cursor()


# COPY the data from docker container directory
# the COPY command only appends the CSV data to the table. It does not replace

sql = f"""
copy fond.transactions 
FROM '/Users/mac/Desktop/VSCode/Projects/Development-Personal-Finance/data/transformJoinTransactions.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',');            
"""

cursor.execute(sql)
conn.commit()
cursor.close()
conn.close()

