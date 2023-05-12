import petl
import psycopg2
import os

from dotenv import load_dotenv
load_dotenv()

data_mart = psycopg2.connect(
    f'dbname={os.getenv("DB_NAME_DM")} user={os.getenv("DB_USER_DM")} password={os.getenv("DB_PASS_DM")}')

operasional = psycopg2.connect(
    f'dbname={os.getenv("DB_NAME")} user={os.getenv("DB_USER")} password={os.getenv("DB_PASS")}')

peserta = petl.fromdb(operasional, "SELECT * FROM peserta")

cursor = data_mart.cursor()
cursor.execute("TRUNCATE dim_peserta RESTART IDENTITY CASCADE")
petl.todb(peserta, cursor, "dim_peserta")
