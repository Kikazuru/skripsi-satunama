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
kecamatan = petl.fromdb(connection, "SELECT * FROM kecamatan")

for id_kecamatan, kode_bps in petl.values(kecamatan, "id_kecamatan", "kode_bps"):

    desa_kel = get_lokasi("desa", kode_bps)

    desa_kel_table = petl.fromdicts(desa_kel)
    desa_kel_table = petl.rename(
        desa_kel_table, {"nama_bps": "nama_desa_kel"})
    desa_kel_table = petl.cutout(
        desa_kel_table, "kode_dagri", "nama_dagri")

    desa_kel_table = petl.addfield(
        desa_kel_table, "id_kecamatan", id_kecamatan)

    petl.appenddb(desa_kel_table, cursor, "desa_kelurahan")

cursor.close()
