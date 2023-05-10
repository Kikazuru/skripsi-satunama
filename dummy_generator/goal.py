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

proyek = petl.fromdb(connection, "SELECT * FROM proyek")

if petl.nrows(proyek) == 0:
    print("Masukan data proyek terlebih dahulu")
else:
    n = 6000

    id_proyek = list(proyek["id_proyek"])

    fields = [
        ("id_proyek", partial(random.choice, id_proyek))
    ]

    dummy_goal = petl.dummytable(n, fields=fields, seed=42)
    dummy_goal = petl.addcolumn(dummy_goal,
                                field="nama_goal",
                                col=[f"goal{i}" for i in range(n)])

    cursor = connection.cursor()
    cursor.execute("TRUNCATE goal RESTART IDENTITY CASCADE")
    petl.todb(dummy_goal, cursor, "goal")
