import requests as req
from bs4 import BeautifulSoup as bs
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

page = 1
members_name = []

while True:
    url = f"https://filantropi.or.id/en/membership/members-of-filantropi-indonesia/?pg1={page}"
    print(url)

    res = req.get(url)
    soup = bs(res.text, "html.parser")

    members = soup.find_all("div", {"class": "member-item"})
    if len(members) == 0:
        break

    for member in members:
        member_name = member.find("div", {"class": "member-name"}).a.string
        members_name.append(member_name)

    page += 1

connection = psycopg2.connect(
    f'host={os.getenv("DBHOST_OP")} dbname={dbname} user={dbuser} password={dbpass}')

negara = petl.fromdb(connection, "SELECT * FROM negara")

if petl.nrows(negara) == 0:
    print("Masukan data negara terlebih dahulu")
else:
    # members_name = list(set(members_name))
    id_negara = list(negara["id_negara"])

    n = len(members_name)
    print(f"JUMLAH DONOR {n}")

    fields = [
        ("id_negara", partial(random.choice, id_negara))
    ]

    dummy_donor = petl.dummytable(n, fields=fields, seed=42)
    dummy_donor = petl.addcolumn(dummy_donor,
                                 field="nama_donor",
                                 col=members_name,
                                 index=0)
    dummy_donor = petl.addfield(dummy_donor,
                                field="singkatan",
                                value=lambda row: "".join([name[0].upper() for name in row["nama_donor"].split()]))

    print(dummy_donor)
    cursor = connection.cursor()
    cursor.execute("TRUNCATE donor RESTART IDENTITY CASCADE")
    petl.todb(dummy_donor, cursor, "donor")
    cursor.close()
