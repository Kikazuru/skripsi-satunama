import petl
import psycopg2
import os
import random
from functools import partial
from faker import Faker

from dotenv import load_dotenv
load_dotenv()

dbname = os.getenv("DB_NAME")
dbuser = os.getenv("DB_USER")
dbpass = os.getenv("DB_PASS")


def pekerja(n, seed=42):
    print("===DUMMY PEKERJA===")
    fake = Faker("id")
    connection = psycopg2.connect(
        f'dbname={dbname} user={dbuser} password={dbpass}')

    jabatan_proyek = petl.fromdb(connection, "SELECT * FROM jabatan_proyek")
    if petl.nrows(jabatan_proyek) == 0:
        pass
    else:
        id_karyawan = [None, ] + [i for i in range(100)]

        fields = [
            ("id_karyawan", partial(random.choice, id_karyawan))
        ]

        dummy_pekerja = petl.dummytable(n, fields=fields, seed=seed)
        dummy_pekerja = petl.addfield(dummy_pekerja, "nama_pekerja", fake.name())

        cursor = connection.cursor()
        cursor.execute("TRUNCATE pekerja RESTART IDENTITY CASCADE")
        petl.todb(dummy_pekerja, cursor, "pekerja")
