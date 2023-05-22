import petl
import psycopg2
import os
import random
from functools import partial

from dotenv import load_dotenv
load_dotenv()

dbname = os.getenv("DB_NAME")
dbuser = os.getenv("DB_USER")
dbpass = os.getenv("DB_PASS")

def output(n, seed=42):
    
    connection = psycopg2.connect(
        f'dbname={dbname} user={dbuser} password={dbpass}')

    outcome = petl.fromdb(connection, "SELECT * FROM outcome")

    if petl.nrows(outcome) == 0:
        print("Masukan data outcome terlebih dahulu")
    else:
        id_outcome = list(outcome["id_outcome"])

        fields = [
            ("id_outcome", partial(random.choice, id_outcome))
        ]

        dummy_output = petl.dummytable(n, fields=fields, seed=seed)
        dummy_output = petl.addcolumn(dummy_output,
                                    field="nama_output",
                                    col=[f"output{i}" for i in range(n)])

        cursor = connection.cursor()
        cursor.execute("TRUNCATE output RESTART IDENTITY CASCADE")
        petl.todb(dummy_output, cursor, "output")
