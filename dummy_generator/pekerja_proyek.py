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


def pekerja_proyek(n, seed=42):
    print("===DUMMY PEKERJA PROYEK===")
    connection = psycopg2.connect(
        f'dbname={dbname} user={dbuser} password={dbpass}')

    proyek = petl.fromdb(connection, "SELECT * FROM proyek")
    pekerja = petl.fromdb(connection, "SELECT * FROM pekerja")

    if petl.nrows(proyek) == 0 or petl.nrows(pekerja) == 0:
        print("Masukan proyek dan pekerja terlebih dahulu")
    else:
        id_proyek = list(proyek["id_proyek"])
        id_pekerja = list(pekerja["id_pekerja"])

        fields = [
            ("id_proyek", partial(random.choice, id_proyek)),
            ("id_pekerja", partial(random.choice, id_pekerja)),
        ]

        dummy_pekerja = petl.dummytable(n, fields=fields, seed=seed)

        cursor = connection.cursor()
        cursor.execute("TRUNCATE pekerja_proyek RESTART IDENTITY CASCADE")
        petl.todb(dummy_pekerja, cursor, "pekerja_proyek")
