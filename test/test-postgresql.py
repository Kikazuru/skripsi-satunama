import psycopg2
import os
from dotenv import load_dotenv
import time

load_dotenv()

data_mart = psycopg2.connect(
    f'host={os.getenv("DBHOST_DM_PSQL")} dbname={os.getenv("DBNAME_DM_PSQL")} user={os.getenv("DBUSER_DM_PSQL")} password={os.getenv("DBPASS_DM_PSQL")}')
cursor = data_mart.cursor()

print("start")
# get the start time
st = time.time()

query = "SELECT * FROM dim_peserta"
cursor.execute(query)
result = cursor.fetchall()

# get the end time
et = time.time()
print("done")

# get the execution time
elapsed_time = et - st
print('Execution time:', round(elapsed_time, ), 'seconds')
