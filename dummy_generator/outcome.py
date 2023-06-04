import petl
import psycopg2
import os
import random
from functools import partial

from dotenv import load_dotenv
load_dotenv()

dbname = os.getenv("DBNAME_OP")
dbuser = os.getenv("DBUSER_OP")
dbpass = os.getenv("DBPASS_OP")


def outcome(n, seed=42):
    print("===DUMMY OUTCOME===")
    connection = psycopg2.connect(
        f'host={os.getenv("DBHOST_OP")} dbname={dbname} user={dbuser} password={dbpass}')

    goal = petl.fromdb(connection, "SELECT * FROM goal")

    if petl.nrows(goal) == 0:
        print("Masukan data goal terlebih dahulu")
    else:
        id_goal = list(goal["id_goal"])

        fields = [
            ("id_goal", partial(random.choice, id_goal))
        ]

        dummy_outcome = petl.dummytable(n, fields=fields, seed=seed)
        dummy_outcome = petl.addcolumn(dummy_outcome,
                                       field="nama_outcome",
                                       col=[f"outcome{i}" for i in range(n)])

        cursor = connection.cursor()
        cursor.execute("TRUNCATE outcome RESTART IDENTITY CASCADE")
        petl.todb(dummy_outcome, cursor, "outcome")
