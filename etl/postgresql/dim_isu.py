import petl
import psycopg2
import os

from dotenv import load_dotenv
load_dotenv()

data_mart = psycopg2.connect(
    f'dbname={os.getenv("DB_NAME_DM")} user={os.getenv("DB_USER_DM")} password={os.getenv("DB_PASS_DM")}')

operasional = psycopg2.connect(
    f'dbname={os.getenv("DB_NAME")} user={os.getenv("DB_USER")} password={os.getenv("DB_PASS")}')

isu = petl.fromdb(operasional, "SELECT * FROM isu")

cursor = data_mart.cursor()
cursor.execute("TRUNCATE dim_isu RESTART IDENTITY CASCADE")
petl.todb(isu, cursor, "dim_isu")