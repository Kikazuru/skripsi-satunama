import petl
import psycopg2
import os
import random
import datetime
from functools import partial

from dotenv import load_dotenv
load_dotenv()

dbname = os.getenv("DB_NAME")
dbuser = os.getenv("DB_USER")
dbpass = os.getenv("DB_PASS")

connection = psycopg2.connect(
    f'dbname={dbname} user={dbuser} password={dbpass}')


def random_dates(start_date: datetime.date, end_date: datetime.date, min_day: int = 60, step: int = 30):
    num_days = (end_date - start_date).days
    random_days = random.randrange(min_day, num_days, step=step)
    random_date = start_date + datetime.timedelta(days=random_days)
    return random_date


donor = petl.fromdb(connection, "SELECT * FROM donor")
provinsi = petl.fromdb(connection, "SELECT * FROM provinsi")
kabupaten_kota = petl.fromdb(connection, "SELECT * FROM kabupaten_kota")
isu = petl.fromdb(connection, "SELECT * FROM isu")

if petl.nrows(donor) == 0 or petl.nrows(provinsi) == 0 or petl.nrows(kabupaten_kota) == 0:
    print("Masukan data donor, provinsi, dan kota terlebih dahulu")
else:
    n = 1_000

    id_donor = list(donor["id_donor"])
    id_kota = list(kabupaten_kota["id_kab_kota"])
    id_isu = list(isu["id_isu"])

    fields = [
        ("tanggal_mulai_proyek",
         partial(random_dates,
                 datetime.date(2017, 1, 1),
                 datetime.date(2020, 1, 1))),
        ("tanggal_selesai_proyek",
         partial(random_dates,
                 datetime.date(2020, 1, 1),
                 datetime.date(2025, 1, 1))),
        ("dana_anggaran", partial(random.randrange,
         100_000_000, 7_000_000_000, 30_000_000)),
        ("id_donor", partial(random.choice, id_donor)),
        ("id_kab_kota", partial(random.choice, id_kota)),
        ("id_isu", partial(random.choice, id_isu))
    ]

    dummy_proyek = petl.dummytable(n, fields=fields, seed=42)
    dummy_proyek = petl.addcolumn(dummy_proyek,
                                  field="nama_proyek",
                                  col=[f"proyek{i}" for i in range(n)],
                                  index=0)
    dummy_proyek = petl.addfield(dummy_proyek,
                                 field="satuan_anggaran",
                                 value="IDR",
                                 index=4)
    dummy_proyek = petl.addfield(dummy_proyek,
                                 field="id_negara",
                                 value=78)
    dummy_proyek = petl.join(dummy_proyek, kabupaten_kota, key="id_kab_kota")
    dummy_proyek = petl.cutout(dummy_proyek, "nama_kab_kota", "kode_bps")
    dummy_proyek = petl.rename(dummy_proyek, "id_kab_kota", "id_kota")
    dummy_proyek = petl.sort(dummy_proyek, key="nama_proyek")

    cursor = connection.cursor()
    cursor.execute("TRUNCATE proyek RESTART IDENTITY CASCADE")
    petl.todb(dummy_proyek, cursor, "proyek")
