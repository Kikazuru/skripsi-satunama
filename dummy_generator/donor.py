import petl
import psycopg2
import os
import random
from functools import partial

def donor(connection, n, seed=42):

    negara = petl.fromdb(connection, "SELECT * FROM negara")

    if petl.nrows(negara) == 0:
        print("Masukan data negara terlebih dahulu")
    else:
        id_negara = list(negara["id_negara"])

        fields = [
            ("id_negara", partial(random.choice, id_negara))
        ]

        dummy_donor = petl.dummytable(n, fields=fields, seed=seed)
        dummy_donor = petl.addcolumn(dummy_donor,
                                     field="nama_donor",
                                     col=[f"donor{i}" for i in range(n)],
                                     index=0)
        dummy_donor = petl.addcolumn(dummy_donor,
                                     field="singkatan",
                                     col=[f"dnr{i}" for i in range(n)])

        cursor = connection.cursor()
        cursor.execute("TRUNCATE donor RESTART IDENTITY CASCADE")
        petl.todb(dummy_donor, cursor, "donor")
        cursor.close()
