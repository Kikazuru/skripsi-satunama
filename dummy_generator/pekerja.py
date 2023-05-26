import petl
import random
from functools import partial
from faker import Faker


def pekerja(connection, n, seed=42):
    print("===DUMMY PEKERJA===")
    fake = Faker("id")

    jabatan_proyek = petl.fromdb(connection, "SELECT * FROM jabatan_proyek")
    if petl.nrows(jabatan_proyek) == 0:
        pass
    else:
        id_karyawan = [None, ] + [i for i in range(n)]

        fields = [
            ("id_karyawan", partial(random.choice, id_karyawan))
        ]

        dummy_pekerja = petl.dummytable(n, fields=fields, seed=seed)
        dummy_pekerja = petl.addfield(
            dummy_pekerja, "nama_pekerja", value=lambda _: fake.name())

        cursor = connection.cursor()
        cursor.execute("TRUNCATE pekerja RESTART IDENTITY CASCADE")
        petl.todb(dummy_pekerja, cursor, "pekerja")
