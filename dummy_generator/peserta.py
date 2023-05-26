import petl
import random
from utils import random_dates
import datetime
from faker import Faker

def peserta(connection, n):
    print("===DUMMY PESERTA===")
    fake = Faker("id")

    dummy_peserta = [
        {"jenis_kelamin": random.choice(["L", "P"]),
         "tanggal_lahir": random_dates(datetime.date(1970, 1, 1), datetime.date(2000, 12, 31), min_day=0, step=1)}
        for i in range(n)]

    dummy_peserta = petl.fromdicts(dummy_peserta)
    dummy_peserta = petl.addfield(dummy_peserta, "nama_peserta",
                                  lambda row: fake.name_male() if row["jenis_kelamin"] == "L" else fake.name_female())

    cursor = connection.cursor()
    cursor.execute("TRUNCATE peserta RESTART IDENTITY CASCADE")
    petl.todb(dummy_peserta, cursor, "peserta")