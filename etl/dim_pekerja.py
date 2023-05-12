import petl
import psycopg2
import os

from dotenv import load_dotenv
load_dotenv()

data_mart = psycopg2.connect(
    f'dbname={os.getenv("DB_NAME_DM")} user={os.getenv("DB_USER_DM")} password={os.getenv("DB_PASS_DM")}')

operasional = psycopg2.connect(
    f'dbname={os.getenv("DB_NAME")} user={os.getenv("DB_USER")} password={os.getenv("DB_PASS")}')

pekerja = petl.fromdb(operasional, "SELECT * FROM pekerja")
jabatan = petl.fromdb(operasional, "SELECT * FROM jabatan_proyek")
lkp_jabatan = petl.dictlookupone(jabatan, "id_jabatan")

pekerja = petl.convert(pekerja, {"id_jabatan": lambda id_jabatan: lkp_jabatan[id_jabatan]["nama_jabatan"]})
pekerja = petl.rename(pekerja, {"id_jabatan": "jabatan"})

cursor = data_mart.cursor()
cursor.execute("TRUNCATE dim_pekerja RESTART IDENTITY CASCADE")
petl.todb(pekerja, cursor, "dim_pekerja")