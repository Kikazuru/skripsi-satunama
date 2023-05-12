import petl
import psycopg2
import os

from dotenv import load_dotenv
load_dotenv()

data_mart = psycopg2.connect(
    f'dbname={os.getenv("DB_NAME_DM")} user={os.getenv("DB_USER_DM")} password={os.getenv("DB_PASS_DM")}')

operasional = psycopg2.connect(
    f'dbname={os.getenv("DB_NAME")} user={os.getenv("DB_USER")} password={os.getenv("DB_PASS")}')

desa = petl.fromdb(operasional, "SELECT * FROM desa_kelurahan")
dim_kecamatan = petl.fromdb(data_mart, "SELECT * FROM dim_kecamatan")

lkp_dim_kecamatan = petl.dictlookupone(dim_kecamatan, "id_kecamatan")
dim_desa = petl.convert(desa, {"id_kecamatan" : lambda id_kecamatan: lkp_dim_kecamatan[id_kecamatan]["kecamatan_key"]})
dim_desa = petl.rename(dim_desa, {"id_kecamatan": "kecamatan_key"})
dim_desa = petl.cutout(dim_desa, "kode_bps")

cursor = data_mart.cursor()
cursor.execute("TRUNCATE dim_desa_kelurahan RESTART IDENTITY CASCADE")
petl.todb(dim_desa, cursor, "dim_desa_kelurahan")