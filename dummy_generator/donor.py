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

connection = psycopg2.connect(
    f'dbname={dbname} user={dbuser} password={dbpass}')

negara = petl.fromdb(connection, "SELECT * FROM negara")

if petl.nrows(negara) == 0:
    print("Masukan data negara terlebih dahulu")
else:
    n = 100
    id_negara = list(negara["id_negara"])

    fields = [
        ("id_negara", partial(random.choice, id_negara))
    ]

    dummy_donor = petl.dummytable(n, fields=fields, seed=42)
    dummy_donor = petl.addcolumn(dummy_donor,
                                 field="nama_donor",
                                 col=[f"donor{i}" for i in range(n)],
                                 index=0)
    dummy_donor = petl.addcolumn(dummy_donor,
                                 field="singkatan",
                                 col=[f"dnr{i}" for i in range(n)])

    cursor = connection.cursor()
    cursor.execute("TRUNCATE donor RESTART IDENTITY CASCADE")
    petl.todb(dummy_donor, cursor, "donor")
