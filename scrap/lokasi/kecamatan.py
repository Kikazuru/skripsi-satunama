import petl
import psycopg2
import os
from scrap import get_lokasi

from dotenv import load_dotenv
load_dotenv()

dbname = os.getenv("DBNAME_OP")
dbuser = os.getenv("DBUSER_OP")
dbpass = os.getenv("DBPASS_OP")

connection = psycopg2.connect(
    f'host={os.getenv("DBHOST_OP")} dbname={dbname} user={dbuser} password={dbpass}')

cursor = connection.cursor()
kab_kota = petl.fromdb(connection, "SELECT * FROM kabupaten_kota")

cursor.execute("TRUNCATE kecamatan RESTART IDENTITY CASCADE")

for id_kab_kota, kode_bps in petl.values(kab_kota, "id_kab_kota", "kode_bps"):
    kecamatan = get_lokasi("kecamatan", kode_bps)

    kecamatan_table = petl.fromdicts(kecamatan)
    kecamatan_table = petl.rename(
        kecamatan_table, {"nama_bps": "nama_kecamatan"})
    kecamatan_table = petl.cutout(
        kecamatan_table, "kode_dagri", "nama_dagri")

    kecamatan_table = petl.addfield(
        kecamatan_table, "id_kab_kota", id_kab_kota)

    petl.appenddb(kecamatan_table, cursor, "kecamatan")

cursor.close()
