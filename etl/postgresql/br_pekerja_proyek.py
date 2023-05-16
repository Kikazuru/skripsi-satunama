import petl
import psycopg2
import os

from dotenv import load_dotenv
load_dotenv()

data_mart = psycopg2.connect(
    f'dbname={os.getenv("DB_NAME_DM")} user={os.getenv("DB_USER_DM")} password={os.getenv("DB_PASS_DM")}')

operasional = psycopg2.connect(
    f'dbname={os.getenv("DB_NAME")} user={os.getenv("DB_USER")} password={os.getenv("DB_PASS")}')

pekerja_proyek = petl.fromdb(operasional, "SELECT * FROM pekerja_proyek")

dim_pekerja = petl.fromdb(data_mart, "SELECT * FROM dim_pekerja")
fact_proyek = petl.fromdb(data_mart, "SELECT * FROM fact_proyek")

lkp_pekerja = petl.dictlookupone(dim_pekerja, "id_pekerja")
lkp_proyek = petl.dictlookupone(fact_proyek, "id_proyek")

br_pekerja_proyek = petl.convert(pekerja_proyek, {"id_pekerja": lambda id_pekerja : lkp_pekerja[id_pekerja]["pekerja_key"],
                                                  "id_proyek": lambda id_proyek : lkp_proyek[id_proyek]["proyek_key"]})

br_pekerja_proyek = petl.rename(br_pekerja_proyek, {"id_pekerja": "pekerja_key", "id_proyek": "proyek_key"})
br_pekerja_proyek = petl.cutout(br_pekerja_proyek, "id_pekerja_proyek")

cursor = data_mart.cursor()
cursor.execute("TRUNCATE br_pekerja_proyek RESTART IDENTITY CASCADE")
petl.todb(br_pekerja_proyek, cursor, "br_pekerja_proyek")