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
jabatan_proyek = petl.fromdb(connection, "SELECT * FROM jabatan_proyek")

if petl.nrows(proyek) == 0 or petl.nrows(jabatan_proyek) == 0:
    print("Masukan proyek dan jabatan_proyek terlebih dahulu")
else:
    n = 5_000

    id_proyek = list(proyek["id_proyek"])
    id_jabatan = list(jabatan_proyek["id_jabatan"])
    id_karyawan = [None, ] + [i for i in range(100)]

    fields = [
        ("id_proyek", partial(random.choice, id_proyek)),
        ("id_jabatan", partial(random.choice, id_jabatan)),
        ("id_karyawan", partial(random.choice, id_karyawan))
    ]

    dummy_pekerja = petl.dummytable(n, fields=fields, seed=42) 
    dummy_pekerja = petl.addcolumn(dummy_pekerja,
                                   field="nama_pekerja",
                                   col=[f"pekerja{i}" for i in range(n)])
    
    cursor = connection.cursor()
    cursor.execute("TRUNCATE pekerja_proyek RESTART IDENTITY CASCADE")
    petl.todb(dummy_pekerja, cursor, "pekerja_proyek")