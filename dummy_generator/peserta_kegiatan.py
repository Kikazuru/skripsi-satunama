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


def peserta_kegiatan(jumlah_peserta, seed=42):
    print("===DUMMY PESERTA KEGIATAN===")
    connection = psycopg2.connect(
        f'dbname={dbname} user={dbuser} password={dbpass}')

    kegiatan = petl.fromdb(connection, "SELECT * FROM kegiatan")
    peserta = petl.fromdb(connection, "SELECT * FROM peserta")

    if petl.nrows(kegiatan) == 0 or petl.nrows(peserta) == 0:
        pass
    else:
        list_id_kegiatan = list(kegiatan["id_kegiatan"])
        list_id_peserta = list(peserta["id_peserta"])

        tabel_peserta_kegiatan = []
        for id_kegiatan in list_id_kegiatan:
            sample_peserta = random.sample(list_id_peserta, jumlah_peserta)
            for id_peserta in sample_peserta:
                tabel_peserta_kegiatan.append(
                    {
                        "id_peserta": id_peserta,
                        "id_kegiatan": id_kegiatan
                    }
                )
        
        dummy_peserta_kegiatan = petl.fromdicts(tabel_peserta_kegiatan)

        cursor = connection.cursor()
        cursor.execute(
            "TRUNCATE peserta_kegiatan_proyek RESTART IDENTITY CASCADE")
        petl.todb(dummy_peserta_kegiatan, cursor, "peserta_kegiatan_proyek")

peserta_kegiatan(30)