import petl
import psycopg2
import os
import random
from functools import partial

def goal(connection, n, seed=42):
    print("===DUMMY GOAL===")

    proyek = petl.fromdb(connection, "SELECT * FROM proyek")

    if petl.nrows(proyek) == 0:
        print("Masukan data proyek terlebih dahulu")
    else:
        id_proyek = list(proyek["id_proyek"])

        fields = [
            ("id_proyek", partial(random.choice, id_proyek))
        ]

        dummy_goal = petl.dummytable(n, fields=fields, seed=seed)
        dummy_goal = petl.addcolumn(dummy_goal,
                                    field="nama_goal",
                                    col=[f"goal{i}" for i in range(n)])

        cursor = connection.cursor()
        cursor.execute("TRUNCATE goal RESTART IDENTITY CASCADE")
        petl.todb(dummy_goal, cursor, "goal")
