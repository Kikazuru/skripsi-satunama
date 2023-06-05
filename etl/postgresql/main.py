from concurrent import futures
from dotenv import load_dotenv
import psycopg2
import os

from job import job

load_dotenv()

for i in range(1, 2):
    n_proyek = 10 ** i

    print(f"===ETL PROYEK {n_proyek}===")

    dbname_dm = f'{os.getenv("DBNAME_DM_PSQL")}_{n_proyek}_proyek'
    data_mart = psycopg2.connect(
        f'host={os.getenv("DBHOST_DM_PSQL")} dbname={dbname_dm} user={os.getenv("DBUSER_DM_PSQL")} password={os.getenv("DBPASS_DM_PSQL")}')

    dbname_op = f'{os.getenv("DBNAME_OP")}_{n_proyek}_proyek'
    operasional = psycopg2.connect(
        f'host={os.getenv("DBHOST_OP")} dbname={dbname_op} user={os.getenv("DBUSER_OP")} password={os.getenv("DBPASS_OP")}')

    job(data_mart, operasional)

    print()

print("ETL SELESAI")
