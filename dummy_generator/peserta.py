import petl
import psycopg2
import os
import random
from utils import random_dates
import datetime

from dotenv import load_dotenv
load_dotenv()

dbname = os.getenv("DB_NAME")
dbuser = os.getenv("DB_USER")
dbpass = os.getenv("DB_PASS")

def peserta(n, seed=42):
    connection = psycopg2.connect(
    f'dbname={dbname} user={dbuser} password={dbpass}')

    dummy_peserta = [
        {"nama_peserta": f"nama{i}", 
        "jenis_kelamin" : random.choice(["L", "P", None]), 
        "tanggal_lahir": random_dates(datetime.date(1970, 1, 1), datetime.date(2000, 12, 31), min_day=0, step=1)}
        for i in range(n)]

    dummy_peserta = petl.fromdicts(dummy_peserta)

    cursor = connection.cursor()
    cursor.execute("TRUNCATE peserta RESTART IDENTITY CASCADE")
    petl.todb(dummy_peserta, cursor, "peserta")