import petl
import psycopg2
import os

from dotenv import load_dotenv
load_dotenv()

data_mart = psycopg2.connect(
    f'dbname={os.getenv("DB_NAME_DM")} user={os.getenv("DB_USER_DM")} password={os.getenv("DB_PASS_DM")}')

operasional = psycopg2.connect(
    f'dbname={os.getenv("DB_NAME")} user={os.getenv("DB_USER")} password={os.getenv("DB_PASS")}')

donor = petl.fromdb(operasional, "SELECT * FROM donor")
dim_negara = petl.fromdb(data_mart, "SELECT * FROM dim_negara")
lkp_negara = petl.dictlookupone(dim_negara, "id_negara")

donor = petl.convert(donor, {"id_negara": lambda id_negara : lkp_negara[id_negara]["negara_key"]})
donor = petl.rename(donor, {"id_negara": "negara_key"})

cursor = data_mart.cursor()
cursor.execute("TRUNCATE dim_donor RESTART IDENTITY CASCADE")
petl.todb(donor, cursor, "dim_donor")