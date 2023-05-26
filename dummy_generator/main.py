from isu import isu
from pekerja import pekerja
from pekerja_proyek import pekerja_proyek
from proyek import proyek
from kegiatan import kegiatan
from peserta import peserta
from peserta_kegiatan import peserta_kegiatan

import psycopg2
import os

from dotenv import load_dotenv
load_dotenv()

dbname = os.getenv("DB_NAME")
dbuser = os.getenv("DB_USER")
dbpass = os.getenv("DB_PASS")

for i in range(1, 5):
    n_proyek = 10 ** i
    print(f"===PROYEK {n_proyek}===")

    dbname_interval = f"{dbname}_{n_proyek}_proyek"
    connection = psycopg2.connect(
        f'dbname={dbname_interval} user={dbuser} password={dbpass}')

    # === JUMLAH DATA ====
    n_isu = 20

    n_kegiatan = 15 * n_proyek
    peserta_per_kegiatan = 10
    pekerja_per_proyek = 10
    n_peseta = 5 * n_kegiatan
    n_pekerja = 100

    # ==== DUMMY ====
    isu(connection, n_isu)

    pekerja(connection, n_pekerja)
    proyek(connection, n_proyek)
    pekerja_proyek(connection, jumlah_pekerja=pekerja_per_proyek)

    kegiatan(connection, n_kegiatan)
    peserta(connection, n_peseta)
    peserta_kegiatan(connection, jumlah_peserta=peserta_per_kegiatan)

    connection.close()
    print()

print("===GENERATE DUMMY SELESAI===")
