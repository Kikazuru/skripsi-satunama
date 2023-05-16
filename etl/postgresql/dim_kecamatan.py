import petl
import psycopg2
import os

from dotenv import load_dotenv
load_dotenv()

data_mart = psycopg2.connect(
    f'dbname={os.getenv("DB_NAME_DM")} user={os.getenv("DB_USER_DM")} password={os.getenv("DB_PASS_DM")}')

operasional = psycopg2.connect(
    f'dbname={os.getenv("DB_NAME")} user={os.getenv("DB_USER")} password={os.getenv("DB_PASS")}')

kecamatan = petl.fromdb(operasional, "SELECT * FROM kecamatan")
dim_kab_kota = petl.fromdb(data_mart, "SELECT * FROM dim_kabupaten_kota")

lkp_dim_kab_kota = petl.dictlookupone(dim_kab_kota, "id_kab_kota")
dim_kecamatan = petl.convert(kecamatan, {"id_kab_kota" : lambda id_kab_kota: lkp_dim_kab_kota[id_kab_kota]["kab_kota_key"]})
dim_kecamatan = petl.rename(dim_kecamatan, {"id_kab_kota": "kab_kota_key"})
dim_kecamatan = petl.cutout(dim_kecamatan, "kode_bps")

cursor = data_mart.cursor()
cursor.execute("TRUNCATE dim_kecamatan RESTART IDENTITY CASCADE")
petl.todb(dim_kecamatan, cursor, "dim_kecamatan")