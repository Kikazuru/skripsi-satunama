import petl
import psycopg2
import os
import random
import datetime
from utils import random_dates
from functools import partial

from dotenv import load_dotenv
load_dotenv()

dbname = os.getenv("DB_NAME")
dbuser = os.getenv("DB_USER")
dbpass = os.getenv("DB_PASS")

connection = psycopg2.connect(
    f'dbname={dbname} user={dbuser} password={dbpass}')

proyek = petl.fromdb(connection, "SELECT * FROM proyek")

penerima_manfaat = petl.fromdb(connection, "SELECT * FROM penerima_manfaat")
jenis_kegiatan = petl.fromdb(connection, "SELECT * FROM jenis_kegiatan")
lembaga_pelaksana = petl.fromdb(connection, "SELECT * FROM lembaga_pelaksana")
kota = petl.fromdb(connection, "SELECT * FROM kabupaten_kota")
kecamatan = petl.fromdb(connection, "SELECT * FROM kecamatan")
desa = petl.fromdb(connection, "SELECT * FROM desa_kelurahan")

def random_pengeluaran(id_proyek: str, lookup_data, kolom_anggaran: str = "dana_anggaran"):
        range = lookup_data[id_proyek][kolom_anggaran] 
        pengeluran = random.randrange(0, range // 100, step=1_000)
        lookup_data[id_proyek][kolom_anggaran] -= pengeluran
        return pengeluran

if petl.nrows(proyek) == 0:
    print("Masukan data output terlebih dahulu")
else:
    n = 100_000

    id_proyek = list(proyek["id_proyek"])
    id_penerima_manfaat = list(penerima_manfaat["id_penerima_manfaat"])
    id_jenis_kegiatan = list(jenis_kegiatan["id_jenis_kegiatan"])
    id_lembaga_pelaksana = list(lembaga_pelaksana["id_lembaga_pelaksana"])
    id_kab_kota = list(kota["id_kab_kota"])
    id_kecamatan = list(kecamatan["id_kecamatan"])
    id_desa_kel = list(desa["id_desa_kel"])

    fields = [
        ("id_proyek", partial(random.choice, id_proyek)),
        ("id_penerima_manfaat", partial(random.choice, id_penerima_manfaat)),
        ("id_jenis_kegiatan", partial(random.choice, id_jenis_kegiatan)),
        ("id_lembaga_pelaksana", partial(random.choice, id_lembaga_pelaksana)),
        ("id_kab_kota", partial(random.choice, id_kab_kota)),
        ("id_kecamatan", partial(random.choice, id_kecamatan)),
        ("id_desa", partial(random.choice, id_desa_kel))
    ]

    dummy_kegiatan = petl.dummytable(n, fields=fields, seed=42)
    lkp_proyek = petl.util.dictlookupone(proyek, key="id_proyek")

    def rowmapper(row):
        return [
            row["id_proyek"]
        ]

    dummy_kegiatan = petl.rename(dummy_kegiatan, {"id_kab_kota": "id_kota"})
    dummy_kegiatan = petl.addfield(dummy_kegiatan, 
                                    field="tanggal_rencana", 
                                    value= lambda row: random_dates(
                                            lkp_proyek[row["id_proyek"]]["tanggal_mulai_proyek"], 
                                            lkp_proyek[row["id_proyek"]]["tanggal_selesai_proyek"], 
                                            min_day=7,
                                            step=random.randint(1, 30))
                                    )
    dummy_kegiatan = petl.addfield(dummy_kegiatan, 
                                    field="tanggal_pelaksanaan", 
                                    value= lambda row: random.choice(
                                            [row["tanggal_rencana"], 
                                            random_dates(
                                                row["tanggal_rencana"], 
                                                row["tanggal_rencana"] + datetime.timedelta(days=10), 
                                                min_day=0,
                                                step=1)
                                            ])
                                    )
    dummy_kegiatan = petl.addcolumn(dummy_kegiatan,
                                   field="nama_kegiatan",
                                   col=[f'kegiatan{i}' for i in range(n)])
    dummy_kegiatan = petl.addcolumn(dummy_kegiatan,
                                   field="deskripsi_kegiatan",
                                   col=[f'deskripsi kegiatan{i}' for i in range(n)])
        
    dummy_kegiatan = petl.addfield(dummy_kegiatan,
                                   field="pengeluaran",
                                   value= lambda row: random_pengeluaran(row["id_proyek"], lkp_proyek, "dana_anggaran"))
    
    cursor = connection.cursor()
    cursor.execute("TRUNCATE kegiatan RESTART IDENTITY CASCADE")
    petl.todb(dummy_kegiatan, cursor, "kegiatan")
