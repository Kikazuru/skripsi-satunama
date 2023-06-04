from dotenv import load_dotenv
from neo4j import GraphDatabase
import petl
import psycopg2
import os

load_dotenv()

operasional = psycopg2.connect(
    f'host={os.getenv("DBHOST_OP")} dbname={os.getenv("DBNAME_OP")} user={os.getenv("DBUSER_OP")} password={os.getenv("DBPASS_OP")}')

table_peserta = petl.fromdb(operasional, "SELECT * FROM peserta")
table_peserta = petl.rowslice(table_peserta, 50_000)
table_peserta = petl.dicts(table_peserta)


def create_peserta(tx, id_peserta, nama_peserta):
    tx.run(
        "MERGE (a:Peserta {id_peserta: $id_peserta, nama_peserta: $nama_peserta})", id_peserta=id_peserta, nama_peserta=nama_peserta)


uri = "neo4j://localhost:7687/"
with GraphDatabase.driver(uri, auth=("neo4j", "@Harris99")) as driver:
    with driver.session(database="datamart") as session:
        for peserta in table_peserta:
            id_peserta = peserta["id_peserta"]
            nama_peserta = peserta["nama_peserta"]

            session.execute_write(create_peserta, id_peserta, nama_peserta)
