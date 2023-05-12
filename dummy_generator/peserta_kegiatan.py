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

kegiatan = petl.fromdb(connection, "SELECT * FROM kegiatan")
peserta = petl.fromdb(connection, "SELECT * FROM peserta")

if petl.nrows(kegiatan) == 0 or petl.nrows(peserta) == 0:
    pass
else:
    n = 10_000_000

    id_kegiatan = list(kegiatan["id_kegiatan"])
    id_peserta = list(peserta["id_peserta"])

    fields = [
        ("id_kegiatan", partial(random.choice, id_kegiatan)),
        ("id_peserta", partial(random.choice, id_peserta))
    ]

    dummy_peserta_kegiatan = petl.dummytable(n, fields=fields, seed=42)

    cursor = connection.cursor()
    cursor.execute("TRUNCATE peserta_kegiatan_proyek RESTART IDENTITY CASCADE")
    petl.todb(dummy_peserta_kegiatan, cursor, "peserta_kegiatan_proyek")


