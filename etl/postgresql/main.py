from concurrent import futures
from dotenv import load_dotenv
import psycopg2
import os

from job import job

load_dotenv()

dbname_dm = f'{os.getenv("DBNAME_DM_PSQL")}'
data_mart = psycopg2.connect(
    f'host={os.getenv("DBHOST_DM_PSQL")} dbname={dbname_dm} user={os.getenv("DBUSER_DM_PSQL")} password={os.getenv("DBPASS_DM_PSQL")}')
data_mart.autocommit = True

dbname_op = f'{os.getenv("DBNAME_OP")}'
operasional = psycopg2.connect(
    f'host={os.getenv("DBHOST_OP")} dbname={dbname_op} user={os.getenv("DBUSER_OP")} password={os.getenv("DBPASS_OP")}')
data_mart.commit()

job(data_mart, operasional)

print("ETL SELESAI")
