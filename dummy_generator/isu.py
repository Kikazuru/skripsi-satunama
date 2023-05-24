import petl
import psycopg2
import os

from dotenv import load_dotenv
load_dotenv()

dbname = os.getenv("DB_NAME")
dbuser = os.getenv("DB_USER")
dbpass = os.getenv("DB_PASS")


def isu(n):
    print("===DUMMY ISU===")
    connection = psycopg2.connect(
        f'dbname={dbname} user={dbuser} password={dbpass}')

    dummy_isu = [{"nama_isu": f"isu{i}"} for i in range(n)]
    dummy_isu = petl.fromdicts(dummy_isu)

    cursor = connection.cursor()
    cursor.execute("TRUNCATE isu RESTART IDENTITY CASCADE")
    petl.todb(dummy_isu, cursor, "isu")
