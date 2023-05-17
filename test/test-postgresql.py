import psycopg2
import os
from dotenv import load_dotenv
import time

load_dotenv()

data_mart = psycopg2.connect(
    f'dbname={os.getenv("DB_NAME_DM")} user={os.getenv("DB_USER_DM")} password={os.getenv("DB_PASS_DM")}')
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
