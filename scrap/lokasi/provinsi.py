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

negara = petl.fromdb(
    connection, "SELECT * FROM negara WHERE UPPER(nama_negara) = UPPER('Indonesia') LIMIT 1")

if petl.nrows(negara) == 0:
    raise ValueError("Data negara Indonesia tidak ditemukan")

id_negara = negara["id_negara"][0]

provinsi = get_lokasi("provinsi")
provinsi_table = petl.fromdicts(provinsi)
provinsi_table = petl.rename(provinsi_table, {"nama_bps": "nama_provinsi"})
provinsi_table = petl.addfield(provinsi_table, "id_negara", id_negara)
provinsi_table = petl.cutout(provinsi_table, "kode_dagri", "nama_dagri")

cursor = connection.cursor()
cursor.execute("TRUNCATE provinsi RESTART IDENTITY CASCADE")
petl.todb(provinsi_table, cursor, 'provinsi')
