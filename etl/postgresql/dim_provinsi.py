import petl
import psycopg2
import os

from dotenv import load_dotenv
load_dotenv()

data_mart = psycopg2.connect(
    f'dbname={os.getenv("DB_NAME_DM")} user={os.getenv("DB_USER_DM")} password={os.getenv("DB_PASS_DM")}')

operasional = psycopg2.connect(
    f'dbname={os.getenv("DB_NAME")} user={os.getenv("DB_USER")} password={os.getenv("DB_PASS")}')

provinsi = petl.fromdb(operasional, "SELECT * FROM provinsi")
dim_negara = petl.fromdb(data_mart, "SELECT * FROM dim_negara")

lkp_dim_negara = petl.dictlookupone(dim_negara, "id_negara")
dim_provinsi = petl.convert(provinsi, {"id_negara" : lambda id_negara: lkp_dim_negara[id_negara]["negara_key"]})
dim_provinsi = petl.rename(dim_provinsi, {"id_negara": "negara_key"})
dim_provinsi = petl.cutout(dim_provinsi, "kode_bps")

cursor = data_mart.cursor()
cursor.execute("TRUNCATE dim_provinsi RESTART IDENTITY CASCADE")
petl.todb(dim_provinsi, cursor, "dim_provinsi")