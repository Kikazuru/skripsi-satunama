import petl
import psycopg2
import os

from dotenv import load_dotenv
load_dotenv()

data_mart = psycopg2.connect(
    f'dbname={os.getenv("DB_NAME_DM")} user={os.getenv("DB_USER_DM")} password={os.getenv("DB_PASS_DM")}')

operasional = psycopg2.connect(
    f'dbname={os.getenv("DB_NAME")} user={os.getenv("DB_USER")} password={os.getenv("DB_PASS")}')

peserta_kegiatan = petl.fromdb(operasional, "SELECT * FROM peserta_kegiatan_proyek")

dim_peserta = petl.fromdb(data_mart, "SELECT * FROM dim_peserta")
fact_kegiatan = petl.fromdb(data_mart, "SELECT * FROM fact_kegiatan")

lkp_peserta = petl.dictlookupone(dim_peserta, "id_peserta")
lkp_kegiatan = petl.dictlookupone(fact_kegiatan, "id_kegiatan")

br_peserta_kegiatan = petl.convert(peserta_kegiatan, {"id_peserta": lambda id_peserta: lkp_peserta[id_peserta]["peserta_key"],
                                                      "id_kegiatan": lambda id_kegiatan: lkp_kegiatan[id_kegiatan]["kegiatan_key"]})

br_peserta_kegiatan = petl.rename(br_peserta_kegiatan, {"id_peserta": "peserta_key", "id_kegiatan": "kegiatan_key"})
br_peserta_kegiatan = petl.cutout(br_peserta_kegiatan, "id_peserta_kegiatan")

cursor = data_mart.cursor()
cursor.execute("TRUNCATE br_peserta_kegiatan RESTART IDENTITY CASCADE")
petl.todb(br_peserta_kegiatan, cursor, "br_peserta_kegiatan")