import petl
import psycopg2
import os

from dotenv import load_dotenv
load_dotenv()

data_mart = psycopg2.connect(
    f'dbname={os.getenv("DB_NAME_DM")} user={os.getenv("DB_USER_DM")} password={os.getenv("DB_PASS_DM")}')

operasional = psycopg2.connect(
    f'dbname={os.getenv("DB_NAME")} user={os.getenv("DB_USER")} password={os.getenv("DB_PASS")}')

kab_kota = petl.fromdb(operasional, "SELECT * FROM kabupaten_kota")
dim_provinsi = petl.fromdb(data_mart, "SELECT * FROM dim_provinsi")

lkp_dim_provinsi = petl.dictlookupone(dim_provinsi, "id_provinsi")
dim_kab_kota = petl.convert(kab_kota, {"id_provinsi" : lambda id_provinsi: lkp_dim_provinsi[id_provinsi]["provinsi_key"]})
dim_kab_kota = petl.rename(dim_kab_kota, {"id_provinsi": "provinsi_key"})
dim_kab_kota = petl.cutout(dim_kab_kota, "kode_bps")

cursor = data_mart.cursor()
cursor.execute("TRUNCATE dim_kabupaten_kota RESTART IDENTITY CASCADE")
petl.todb(dim_kab_kota, cursor, "dim_kabupaten_kota")