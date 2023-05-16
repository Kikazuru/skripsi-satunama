import petl
import psycopg2
import os

from dotenv import load_dotenv
load_dotenv()

data_mart = psycopg2.connect(
    f'dbname={os.getenv("DB_NAME_DM")} user={os.getenv("DB_USER_DM")} password={os.getenv("DB_PASS_DM")}')

operasional = psycopg2.connect(
    f'dbname={os.getenv("DB_NAME")} user={os.getenv("DB_USER")} password={os.getenv("DB_PASS")}')

lembaga_pelaksana = petl.fromdb(operasional, "SELECT * FROM lembaga_pelaksana")
dim_kota = petl.fromdb(data_mart, "SELECT * FROM dim_kabupaten_kota")

lkp_kota = petl.dictlookupone(dim_kota, "id_kab_kota")

dim_lembaga_pelaksana = petl.convert(lembaga_pelaksana, {"id_kota": lambda id_kota: lkp_kota[id_kota]["kab_kota_key"]})
dim_lembaga_pelaksana = petl.rename(dim_lembaga_pelaksana, {"id_kota": "kota_key"})

cursor = data_mart.cursor()
cursor.execute("TRUNCATE dim_lembaga_pelaksana RESTART IDENTITY CASCADE")
petl.todb(dim_lembaga_pelaksana, cursor, 'dim_lembaga_pelaksana')