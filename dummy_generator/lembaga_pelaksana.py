import requests as req
from bs4 import BeautifulSoup as bs
import psycopg2
import petl
import os

from dotenv import load_dotenv
load_dotenv()

dbname = os.getenv("DB_NAME")
dbuser = os.getenv("DB_USER")
dbpass = os.getenv("DB_PASS")

connection = psycopg2.connect(
    f'dbname={dbname} user={dbuser} password={dbpass}')

data_table = []

for i in range(17):
    url = f"https://smeru.or.id/en/content/ngo-database?page={i}"
    print(url)

    res = req.get(url)
    soup = bs(res.text, "html.parser")

    table = soup.find("table")
    list_tr = table.tbody.children

    for tr in list_tr:
        list_td = list(tr.children)

        ngo_id = int(list_td[0].text)

        ngo_name = list_td[1]
        ngo_address_contact = list_td[2]

        ngo_full_name = ngo_name.a.text
        ngo_abbr = ngo_name.contents[-1].text
        ngo_address = ngo_address_contact.contents[0].text

        data_table.append({
            "nama_lembaga": ngo_full_name,
            "singkatan": ngo_abbr
        })

data_table = petl.fromdicts(
    data_table)

petl.todb(data_table, connection, 'lembaga_pelaksana')
